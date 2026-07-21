package etcdserver

import (
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"math/rand"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
	ulid "yunli.com/jobpool/pkg/v2/ulid"
)

const (
	DefaultMaxFetchNodeTimes = 10
	DefaultMaxSkipJobTimes   = 5
	DefaultMaxPendingSeconds = 60 * 60
)

const (
	ReasonFailedNoSlot = "无槽位，任务失败"
	ReasonSkipped      = "任务运行中，本任务跳过"
)

// DispatchPlan 这个的实现现在很简单，后续需要结合策略和资源
func (s *EtcdServer) DispatchPlan(plan *domain.Plan) (*schedulepb.Evaluation, error) {
	s.Logger().Debug("将Plan发送给client或队列用于下一步执行", zap.String("plan-name", plan.Name))
	// 看是否有足够的slot
	if s.isLeader() && !s.jobBirdEye.HasEmptySlot(plan.Namespace) {
		s.Logger().Warn("the slot of jobRoadMap is full, reject the new job")
		err := s.failPlanForNoSlot(plan)
		return nil, err
	}
	jobInitStatus := constant.JobStatusPending
	if plan.Synchronous {
		jobInitStatus = constant.JobStatusComplete
	}
	job, err := s.createJobByPeriodic(plan, jobInitStatus, "")
	if err != nil {
		return nil, err
	}
	s.Logger().Debug("the job id when create job finished", zap.String("job-id", job.Id))
	eval := &pb.ScheduleEvalAddRequest{
		Id:          ulid.Generate(),
		Namespace:   plan.Namespace,
		Priority:    plan.Priority,
		Type:        plan.Type,
		TriggeredBy: constant.EvalTriggerPeriodicJob,
		JobId:       job.Id,
		PlanId:      plan.ID,
		Status:      constant.EvalStatusPending,
	}
	evalResponse, err := s.EvalAdd(s.ctx, eval)
	if err != nil {
		// 写入job成功但是写入eval或者队列不成功，这个需要补偿机制
		// 2024年8月12日
		s.Logger().Error("add eval error in dispatch logic", zap.String("job-id", job.Id), zap.Error(err))
		s.Logger().Info("start update job from pending to failed", zap.String("job-id", job.Id))
		_, upErr := s.JobUpdate(s.ctx, &pb.ScheduleJobStatusUpdateRequest{
			Id:          job.Id,
			Status:      constant.JobStatusFailed,
			Description: "the job can't create eval or can't enqueue",
		})
		if upErr != nil {
			s.Logger().Error("update job status error, the job will pending all the time", zap.String("job-id", job.Id))
		}
		return nil, err
	}
	return evalResponse.Data, nil
}

func (s *EtcdServer) RunningChildren(plan *domain.Plan) (bool, error) {
	jobMatchRequest := &pb.ScheduleJobExistRequest{
		PlanId:    plan.ID,
		Namespace: plan.Namespace,
		Status:    fmt.Sprintf("%s,%s", constant.JobStatusRunning, constant.JobStatusPending),
	}
	jobExistResp, err := s.JobExist(s.ctx, jobMatchRequest)
	if err != nil {
		return false, err
	}
	if jobExistResp != nil && jobExistResp.Count > 0 {
		return true, nil
	}
	return false, nil
}

// schedulePeriodic is used to do periodic plan dispatch while we are leader
func (s *EtcdServer) schedulePeriodic(stopCh chan struct{}) {
	// TODO from config
	jobGC := time.NewTicker(1 * time.Hour)
	defer jobGC.Stop()
	evalGC := time.NewTicker(1 * time.Hour)
	defer evalGC.Stop()
	nodeGC := time.NewTicker(5 * time.Minute)
	defer nodeGC.Stop()
	allocGC := time.NewTicker(5 * time.Minute)
	defer allocGC.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-evalGC.C:
			s.evalBroker.Enqueue(s.coreJobEval(constant.CoreJobEvalGC, s.getAppliedIndex()))
		case <-nodeGC.C:
			s.evalBroker.Enqueue(s.coreJobEval(constant.CoreJobNodeGC, s.getAppliedIndex()))
		case <-jobGC.C:
			s.evalBroker.Enqueue(s.coreJobEval(constant.CoreJobJobGC, s.getAppliedIndex()))
		case <-allocGC.C:
			s.evalBroker.Enqueue(s.coreJobEval(constant.CoreAllocationGC, s.getAppliedIndex()))
		}
	}

}

// coreJobEval 创建系统用的eval信息，用于垃圾回收
func (s *EtcdServer) coreJobEval(flagId string, modifyIndex uint64) *domain.Evaluation {
	return &domain.Evaluation{
		ID:          ulid.Generate(),
		Namespace:   "-",
		Priority:    200,
		Type:        constant.PlanTypeCore,
		TriggeredBy: constant.EvalTriggerScheduled,
		JobID:       flagId,
		PlanID:      flagId,
		Status:      constant.EvalStatusPending,
		CreateTime:  xtime.NewFormatTime(time.Now()),
		UpdateTime:  xtime.NewFormatTime(time.Now()),
	}
}

// SkipPlan 为该plan创建跳过任务记录
// 仅当周期配置为不允许并行时记录
// 调用插件请求添加跳过计划任务
func (s *EtcdServer) SkipPlan(plan *domain.Plan) error {
	jobResponse, err := s.createJobByPeriodic(plan, constant.JobStatusSkipped, "skip this plan while has running job")
	if err != nil {
		s.Logger().Warn("create job for skiped failed", zap.Error(err))
		return err
	}
	// create the skip allocation to run in the dispatcher
	if !plan.Synchronous {
		go s.createSkipAllocation(plan, jobResponse.Id)
		// 2024年8月23日 离线任务，跳过次数有限制，连续[5次]跳过或pending超过[1小时]则直接失败该任务
		go s.checkAndRepairHistoryJob(plan)
	}
	s.Logger().Debug("skip the job with plugin", zap.String("job-id", jobResponse.Id))
	return nil
}

// FailPlanForNoSlot 没有槽位只能将其失败
func (s *EtcdServer) failPlanForNoSlot(plan *domain.Plan) error {
	jobResponse, err := s.createJobByPeriodic(plan, constant.JobStatusFailed, "create failed job while no slot left")
	if err != nil {
		s.Logger().Warn("create job for failed failed", zap.Error(err))
		return err
	}
	// create the failed allocation to run in the dispatcher
	if !plan.Synchronous {
		// 耗时操作
		go s.createFailedAllocation(plan, jobResponse.Id)
	}
	s.Logger().Debug("failed the job with plugin", zap.String("job-id", jobResponse.Id))
	return nil
}

// createSkipAllocation 创建跳过的任务分配
func (s *EtcdServer) createSkipAllocation(plan *domain.Plan, jobId string) error {
	var evals []*schedulepb.Evaluation
	var allocs []*schedulepb.Allocation
	nodeId, errNode := s.fetchAvaluableNode()
	clientStatus := constant.AllocClientStatusPending
	if errNode != nil {
		clientStatus = constant.AllocClientStatusFailed
	}
	now := xtime.NewFormatTime(time.Now())
	skippedAllocation := &domain.Allocation{
		ID:                 ulid.Generate(),
		Name:               plan.Name,
		Namespace:          plan.Namespace,
		PlanID:             plan.ID,
		Plan:               plan,
		JobId:              jobId,
		NodeID:             nodeId,
		DesiredStatus:      constant.AllocDesiredStatusSkip,
		DesiredDescription: ReasonSkipped,
		ClientStatus:       clientStatus,
		ClientDescription:  string(plan.Parameters),
		CreateTime:         now,
		ModifyTime:         now,
	}
	allocs = append(allocs, domain.ConvertAllocationToPb(skippedAllocation))
	skipAllocation := &pb.ScheduleAllocationAddRequest{
		EvaluationPreemption: evals,
		AllocationUpdated:    allocs,
	}
	_, err := s.AllocationAdd(s.ctx, skipAllocation)
	if err != nil {
		s.Logger().Warn("create allocation error", zap.Error(err))
		return err
	}
	return nil
}

// 检查是否有异常的任务，尝试修复跳过
func (s *EtcdServer) checkAndRepairHistoryJob(plan *domain.Plan) error {
	unNormalJob := &pb.ScheduleJobListRequest{
		PlanId:     plan.ID,
		Namespace:  plan.Namespace,
		PageNumber: 1,
		PageSize:   50,
	}
	jobs, err := s.JobList(s.ctx, unNormalJob)
	if err != nil {
		return err
	}
	skipTimes := 0
	latestPendingJobTime := xtime.NewFormatTime(time.Now())
	pendingHourAgo := time.Now().Add(time.Second * -1 * DefaultMaxPendingSeconds)
	var pendingJob *domain.Job
	for _, job := range jobs.Data {
		if constant.JobStatusSkipped == job.Status {
			skipTimes++
		} else {
			if constant.JobStatusPending == job.Status {
				pendingJob = domain.ConvertJob(job)
				latestPendingJobTime = pendingJob.CreateTime
			}
			break
		}
	}
	// 达成下面任意条件，以后就不能跳过了
	shouldRepair := false
	repairReason := ""
	if skipTimes >= DefaultMaxSkipJobTimes {
		shouldRepair = true
		repairReason = fmt.Sprintf("该任务导致%d个任务跳过", DefaultMaxSkipJobTimes)
	}
	if latestPendingJobTime.TimeValue().Before(pendingHourAgo) {
		shouldRepair = true
		repairReason = fmt.Sprintf("该任务已经等资源%d秒，影响后续任务运行", DefaultMaxPendingSeconds)
	}
	if !shouldRepair {
		return nil
	}
	if pendingJob == nil {
		// shikp过来的，需要找到那个pending
		for _, job := range jobs.Data {
			if constant.JobStatusPending == job.Status {
				pendingJob = domain.ConvertJob(job)
			}
		}
		if pendingJob == nil {
			s.logger.Warn("试图将pending的任务设置失败，但是没找到pending的任务", zap.String("plan-id", plan.ID), zap.String("plan-name", plan.Name))
			return nil
		}
	}
	jobUpdateReq := &pb.ScheduleJobStatusUpdateRequest{
		Id:          pendingJob.ID,
		Status:      constant.JobStatusFailed,
		Description: repairReason,
	}
	_, err = s.JobUpdate(s.ctx, jobUpdateReq)
	return err
}

// createFailedAllocation 创建一个失败的alloc因为没有槽位导致
func (s *EtcdServer) createFailedAllocation(plan *domain.Plan, jobId string) error {
	var evals []*schedulepb.Evaluation
	var allocs []*schedulepb.Allocation
	nodeId, errNode := s.fetchAvaluableNode()
	clientStatus := constant.AllocClientStatusPending
	if errNode != nil {
		clientStatus = constant.AllocClientStatusFailed
	}
	now := xtime.NewFormatTime(time.Now())
	skipedAllocation := &domain.Allocation{
		ID:                 ulid.Generate(),
		Name:               plan.Name,
		Namespace:          plan.Namespace,
		PlanID:             plan.ID,
		Plan:               plan,
		JobId:              jobId,
		NodeID:             nodeId,
		DesiredStatus:      constant.AllocDesiredStatusFail,
		DesiredDescription: ReasonFailedNoSlot,
		ClientStatus:       clientStatus,
		ClientDescription:  string(plan.Parameters),
		CreateTime:         now,
		ModifyTime:         now,
	}
	allocs = append(allocs, domain.ConvertAllocationToPb(skipedAllocation))
	failedAllocation := &pb.ScheduleAllocationAddRequest{
		EvaluationPreemption: evals,
		AllocationUpdated:    allocs,
	}
	_, err := s.AllocationAdd(s.ctx, failedAllocation)
	if err != nil {
		s.Logger().Warn("create allocation error", zap.Error(err))
		return err
	}
	return nil
}

// fetchAvaluableNode 选取节点
// TODO 这个选取过程应该统一到一个类中
func (s *EtcdServer) fetchAvaluableNode() (string, error) {
	var nodeId string
	var fetchErr error
	var retryTimes int = 1
OUTER:
	for {
		if retryTimes > DefaultMaxFetchNodeTimes {
			s.Logger().Warn("no ready nodes for create unnormal allocation", zap.Int("retry", retryTimes))
			fetchErr = fmt.Errorf("no ready nodes")
			break
		}
		nodeResponse, err := s.NodeList(s.ctx, &pb.NodeListRequest{
			Status: constant.NodeStatusReady,
		})
		if err != nil {
			retryTimes++
			s.Logger().Warn("get nodes error", zap.Error(err))
			time.Sleep(5 * time.Second)
			goto OUTER
		}
		nodes := nodeResponse.Data
		if len(nodes) > 0 {
			if len(nodes) > 1 {
				// 随机
				num := rand.Intn(len(nodes) - 1)
				if num < len(nodes) && nodes[num] != nil {
					nodeId = nodes[num].ID
				}
			}
			// 取第一个
			if nodeId == "" {
				nodeId = nodes[0].ID
			}
		} else {
			s.Logger().Debug("no ready nodes for create skip allocation")
			retryTimes++
			time.Sleep(5 * time.Second)
			goto OUTER
		}
		break
	}
	return nodeId, fetchErr
}

// 调度将在有效日期内自动生效，反之，在有效期外的任务将不会自动调度
func (s *EtcdServer) ExpirePlan(planRequest *domain.Plan) error {
	detail, err := s.PlanDetail(s.ctx, &pb.SchedulePlanDetailRequest{
		Id:        planRequest.ID,
		Namespace: planRequest.Namespace,
	})
	if err != nil {
		return err
	}
	plan := detail.Data
	if constant.PlanStatusDead == plan.Status || constant.PlanStatusDead == planRequest.Status {
		return domain.NewErr1107FailExecute("计划", planRequest.ID, "该计划已下线")
	}
	_, err = s.PlanUpdate(s.ctx, &pb.SchedulePlanUpdateRequest{
		Namespace:   plan.Namespace,
		Id:          plan.Id,
		Status:      constant.PlanStatusDead,
		Description: fmt.Sprintf("计划：[%s]调度有效时间[%s]已到，该计划自动过期", plan.Name, plan.Periodic.EndTime),
	})
	if err != nil {
		s.Logger().Error("offline plan failed", zap.Error(err))
		return err
	}
	return nil
}

/*
*
当主发生转换时需要从快照中将plan恢复出来
*/
func (s *EtcdServer) restorePeriodicDispatcher() error {
	s.Logger().Debug("读取缓存内容重新加载plan")
	resp, err := s.scheduleStore.PlanList(&pb.SchedulePlanListRequest{})
	if err != nil {
		s.Logger().Warn("list plan failed", zap.Error(err))
		return err
	}
	if resp == nil {
		return nil
	}
	for _, r := range resp.Data {
		plan := domain.ConvertPlan(r)
		err := s.periodicDispatcher.Add(plan)
		if err != nil {
			return err
		}
		runningJobResp, err := s.scheduleStore.JobList(&pb.ScheduleJobListRequest{
			PlanId: plan.ID,
			Status: constant.JobStatusRunning,
		})
		if err != nil {
			return err
		}
		if runningJobResp != nil && runningJobResp.Data != nil && len(runningJobResp.Data) > 0 {
			for _, jobPb := range runningJobResp.Data {
				job := domain.ConvertJob(jobPb)
				s.jobBirdEye.JobRunningRestore(job)
			}
		}
	}
	return nil
}

// createJobByPeriodic 创建y个
func (s *EtcdServer) createJobByPeriodic(plan *domain.Plan, status string, reason string) (*schedulepb.Job, error) {
	// storage job
	now := timestamppb.TimestampNow()
	newJob := &pb.ScheduleJobAddRequest{
		Id:            ulid.Generate(),
		PlanId:        plan.ParentID,
		DerivedPlanId: plan.ID,
		Name:          plan.Name,
		Type:          constant.JobTypeAuto,
		Namespace:     plan.Namespace,
		OperatorId:    plan.CreatorId,
		Timeout:       3600000,
		Status:        status,
		Info:          reason,
		Parameters:    string(plan.Parameters),
		CreateTime:    now,
		UpdateTime:    now,
	}
	if constant.JobStatusSkipped == status && newJob.PlanId == "" {
		newJob.PlanId = plan.ID
	}
	s.Logger().Debug("create a new job", zap.Reflect("job", newJob))

	jobResp, err := s.JobAdd(s.ctx, newJob)
	if jobResp == nil {
		return nil, domain.NewErr1101None("任务", newJob.Id)
	}
	return jobResp.Data, err
}
