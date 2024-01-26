// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"fmt"
	"go.uber.org/zap"
	"strings"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	ulid "yunli.com/jobpool/pkg/v2/ulid"
)

// GenericScheduler 通用调度器
type GenericScheduler struct {
	logger           *zap.Logger
	repository       ScheduleRepository
	planWorker       PlanWorkerService
	eval             *domain.Evaluation
	job              *domain.Job
	plan             *domain.Plan
	planAlloc        *domain.PlanAlloc
	planResult       *domain.PlanResult
	followUpEvalList []*domain.Evaluation // followUpEvalList 失败后重新追加的评估
	blocked          *domain.Evaluation
	queuedAllocs     map[string]int
}

// NewGenericScheduler 创建一个通用调度器，用于日常调度业务
func NewGenericScheduler(name string, logger *zap.Logger, repository ScheduleRepository, planWorker PlanWorkerService) *GenericScheduler {
	s := &GenericScheduler{
		logger:     logger.Named(name),
		repository: repository,
		planWorker: planWorker,
	}
	return s
}

func (s *GenericScheduler) Process(eval *domain.Evaluation) error {
	s.eval = eval
	s.logger = s.logger.With(zap.String("eval_id", eval.ID), zap.String("plan_id", eval.PlanID), zap.String("namespace", eval.Namespace))
	progress := func() bool { return progressResultReset(s.planResult) }
	retryLimit := 5
	if err := retryMax(retryLimit, s.processImpl, progress); err != nil {
		if statusErr, ok := err.(*EvalStatusWithError); ok {
			// 暂时阻塞住该任务，后续重试
			var multierr []error
			if err := s.createBlockedEval(true); err != nil {
				multierr = append(multierr, err)
			}
			if err := setStatus(s.logger, s.planWorker, s.eval, nil, s.blocked,
				statusErr.EvalStatus, err.Error(), s.queuedAllocs); err != nil {
				multierr = append(multierr, err)
			}
			if len(multierr) > 0 {
				return multierr[0]
			} else {
				return nil
			}
		}
		return err
	}
	if constant.EvalStatusCancelled == s.eval.StatusDescription {
		return setStatus(s.logger, s.planWorker, s.eval, nil, s.blocked,
			constant.EvalStatusCancelled, "", s.queuedAllocs)
	}
	if s.eval.Status == constant.EvalStatusBlocked {
		newEval := s.eval.Copy()
		// newEval.QuotaLimitReached = e.QuotaLimitReached()
		return s.planWorker.ReblockEval(newEval)
	}

	// Update the status to complete
	return setStatus(s.logger, s.planWorker, s.eval, nil, s.blocked,
		constant.EvalStatusComplete, "", s.queuedAllocs)
}

// retryMax 重试执行方法cb
func retryMax(max int, cb func() (bool, error), reset func() bool) error {
	attempts := 0
	for attempts < max {
		done, err := cb()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if reset != nil && reset() {
			attempts = 0
		} else {
			attempts++
		}
	}
	return &EvalStatusWithError{
		Err:        fmt.Errorf("maximum attempts reached (%d)", max),
		EvalStatus: constant.EvalStatusFailed,
	}
}

type EvalStatusWithError struct {
	Err        error
	EvalStatus string
}

func (s *EvalStatusWithError) Error() string {
	return s.Err.Error()
}

func progressResultReset(result *domain.PlanResult) bool {
	return result != nil && (len(result.NodeUpdate) != 0 ||
		len(result.NodeAllocation) != 0)
}

func (s *GenericScheduler) createBlockedEval(planFailure bool) error {
	ev := s.eval
	s.blocked = &domain.Evaluation{
		ID:           ulid.Generate(),
		Namespace:    ev.Namespace,
		Priority:     ev.Priority,
		Type:         ev.Type,
		TriggeredBy:  constant.EvalTriggerQueuedAllocs,
		JobID:        ev.JobID,
		PlanID:       ev.PlanID,
		Status:       constant.EvalStatusBlocked,
		PreviousEval: ev.ID,
		CreateTime:   xtime.NewFormatTime(time.Now()),
	}
	if planFailure {
		s.blocked.TriggeredBy = constant.EvalTriggerMaxPlans
		s.blocked.StatusDescription = "created reached max retry times"
	} else {
		s.blocked.StatusDescription = "created to place remaining allocations"
	}
	return s.planWorker.CreateEval(s.blocked)
}

// setStatus is used to update the status of the evaluation
func setStatus(logger *zap.Logger, planWorker PlanWorkerService,
	eval, nextEval, spawnedBlocked *domain.Evaluation,
	status, description string, queuedAllocs map[string]int) error {

	logger.Debug("setting eval status", zap.String("status", status))
	newEval := eval.Copy()
	newEval.Status = status
	newEval.StatusDescription = description
	if nextEval != nil {
		newEval.NextEval = nextEval.ID
	}
	if spawnedBlocked != nil {
		newEval.BlockedEval = spawnedBlocked.ID
	}
	if queuedAllocs != nil {
		newEval.QueuedAllocations = queuedAllocs
	}
	return planWorker.UpdateEval(newEval)
}

func (s *GenericScheduler) getJob() (bool, error) {
	var err error
	s.job, err = s.repository.JobByID(s.eval.JobID)
	if err != nil {
		s.logger.Debug("get job failed and will retry", zap.String("job-id", s.eval.JobID), zap.Error(err))
		time.Sleep(50 * time.Millisecond)
		// return false, fmt.Errorf("failed to get job %q: %v", s.eval.JobID, err)
		return false, nil
	}
	return true, nil
}

// 正常流程执行eval和alloc
func (s *GenericScheduler) processImpl() (bool, error) {
	// Lookup the PlanAllocation by ID
	var err error
	threeHourAgo := time.Now().Add(time.Hour * -3)
	if constant.EvalStatusBlocked == s.eval.Status && s.eval.CreateTime.TimeValue().Before(threeHourAgo) {
		// s.eval.Status = constant.EvalStatusCancelled
		s.eval.StatusDescription = constant.EvalStatusCancelled
		s.logger.Warn("the eval blocked more than 3 hours skipped", zap.String("eval-id", s.eval.ID))
		return true, nil
	}

	// retry five times
	err = retryMax(3, s.getJob, nil)
	if err != nil {
		return false, fmt.Errorf("failed to get job %q: %v", s.eval.JobID, err)
	}
	if s.job == nil {
		return false, fmt.Errorf("get job by id empty, namespace:%q, jobId: %q", s.eval.Namespace, s.eval.JobID)
	}

	s.plan, err = s.repository.PlanByID(s.eval.Namespace, s.job.PlanID)
	if err != nil {
		return false, fmt.Errorf("failed to get plan %q: %v", s.eval.PlanID, err)
	}
	if s.plan == nil {
		return false, fmt.Errorf("get plan by id empty, namespace:%q, planId:  %q", s.eval.Namespace, s.job.PlanID)
	}
	numTaskGroups := 0
	stopped := s.plan.Stopped()
	if !stopped {
		numTaskGroups = 1
	}

	s.queuedAllocs = make(map[string]int, numTaskGroups)
	s.followUpEvalList = nil

	// Create a planAlloc
	s.planAlloc = makePlanAllocByEvalAndPlan(s.eval, s.plan, s.job)

	// 开始计算评估信息	// Compute the target job allocations
	if err := s.computeJobAllocs(); err != nil {
		s.logger.Warn("failed to compute job allocations", zap.String("error", err.Error()))
		return false, err
	}
	delayInstead := len(s.followUpEvalList) > 0 && s.eval.WaitUntil.IsZero()

	// TODO 如果有失败的alloc才需要这里的逻辑
	/*
		if s.eval.Status != constant.EvalStatusBlocked && s.blocked == nil &&
			!delayInstead {
			if err := s.createBlockedEval(false); err != nil {
				s.logger.Error("failed to make blocked eval", zap.Error(err))
				return false, err
			}
			s.logger.Debug("failed to place all allocations, blocked eval created", zap.String("blocked_eval_id", s.blocked.ID))
		}
	*/

	if s.planAlloc.IsNoOp() && !s.eval.AnnotatePlan {
		return true, nil
	}
	if delayInstead {
		for _, eval := range s.followUpEvalList {
			eval.PreviousEval = s.eval.ID
			// TODO(preetha) this should be batching evals before inserting them
			if err := s.planWorker.CreateEval(eval); err != nil {
				s.logger.Error("failed to make next eval for rescheduling", zap.Error(err))
				return false, err
			}
			s.logger.Debug("found reschedulable allocs, followup eval created", zap.String("followup_eval_id", eval.ID))
		}
	}
	// Submit the plan and store the results.
	result, err := s.planWorker.SubmitPlan(s.planAlloc)
	s.planResult = result
	s.logger.Debug("the plan alloc", zap.Reflect("alloc", s.planAlloc))
	if err != nil {
		return false, err
	}

	// Try again if the plan was not fully committed, potential conflict
	fullCommit, expected, actual := result.FullCommit(s.planAlloc)
	s.logger.Debug("the full commit info", zap.Bool("fullcommit", fullCommit), zap.Int("excpet", expected),
		zap.Int("act", actual))

	if !fullCommit {
		s.logger.Warn("plan didn't fully commit", zap.Int("attempted", expected), zap.Int("placed", actual))
		return false, nil
	}
	// Success!
	return true, nil
}

func makePlanAllocByEvalAndPlan(eval *domain.Evaluation, plan *domain.Plan, job *domain.Job) *domain.PlanAlloc {
	p := &domain.PlanAlloc{
		EvalID:          eval.ID,
		Priority:        eval.Priority,
		Plan:            plan,
		JobId:           job.ID,
		NodeUpdate:      make(map[string][]*domain.Allocation),
		NodeAllocation:  make(map[string][]*domain.Allocation),
		NodePreemptions: make(map[string][]*domain.Allocation),
	}
	if plan != nil {
		p.AllAtOnce = plan.AllAtOnce
	}
	return p
}

// 核心
func (s *GenericScheduler) computeJobAllocs() error {
	// 下面的这个逻辑没必要，且超级卡顿
	// 逻辑内容是从plan对应的alloc中找到pending或running的allocation，且标识为stop或evit的，将它们纳入到alloc中以便后续更新状态
	// 这些逻辑完全可以在gc中做
	allocs, err := s.repository.UnhealthAllocsByPlan(s.eval.Namespace, s.eval.PlanID, true)
	if err != nil {
		return fmt.Errorf("failed to get allocs for plan '%s': %v",
			s.eval.PlanID, err)
	}
	updateNonTerminalAllocsToLost(s.planAlloc, allocs, s.logger)

	coordinator := NewNodeCoordinator(s.logger, s.planWorker.RunningSlotLeft, s.planAlloc)
	// runNode, err := s.getRandomNodeForSelect(s.repository)
	// 算法，获取对应的node
	runNode, err := coordinator.Compute(s.repository)
	if err != nil {
		return fmt.Errorf("failed to fetch node for scheduler '%s': %v",
			s.eval.PlanID, err)
	}
	if runNode != nil {
		s.logger.Debug("select running plan node ", zap.String("plan", s.plan.Name), zap.String("planAlloc", s.planAlloc.JobId), zap.String("node", runNode.ID))
		updateAlloc := &domain.Allocation{
			ID:           ulid.Generate(),
			Name:         s.plan.Name,
			Namespace:    s.plan.Namespace,
			EvalID:       s.eval.ID,
			PlanID:       s.plan.ID,
			Plan:         s.plan,
			JobId:        s.job.ID,
			NodeID:       runNode.ID,
			ClientStatus: constant.AllocClientStatusPending,
			CreateTime:   xtime.NewFormatTime(time.Now()),
			ModifyTime:   xtime.NewFormatTime(time.Now()),
		}
		s.planAlloc.AppendAlloc(updateAlloc, s.planAlloc.Plan)
	} else {
		s.logger.Debug("select run node failed, there is no client to use, job will store in blocked queue")
	}
	return nil
}

func taintedNodes(repository ScheduleRepository, allocs []*domain.Allocation) (map[string]*domain.Node, error) {
	out := make(map[string]*domain.Node)
	for _, alloc := range allocs {
		if _, ok := out[alloc.NodeID]; ok {
			continue
		}
		node, err := repository.NodeByID(alloc.NodeID)
		if err != nil {
			if strings.Contains(err.Error(), "]不存在") {
				out[alloc.NodeID] = nil
				continue
			}
			return nil, err
		}

		// If the node does not exist, we should migrate
		if node == nil {
			out[alloc.NodeID] = nil
			continue
		}
		if domain.ShouldDrainNode(node.Status) {
			out[alloc.NodeID] = node
		}
		if node.Status == constant.NodeStatusDisconnected {
			out[alloc.NodeID] = node
		}
	}
	return out, nil
}

// updateNonTerminalAllocsToLost updates the allocations which are in pending/running repository
// on tainted node to lost, but only for allocs already DesiredStatus stop or evict
func updateNonTerminalAllocsToLost(plan *domain.PlanAlloc, allocs []*domain.Allocation, logger *zap.Logger) {
	for _, alloc := range allocs {
		allocLost := "alloc is lost since its node is down"
		// If the alloc is already correctly marked lost, we're done
		if (alloc.DesiredStatus == constant.AllocDesiredStatusStop ||
			alloc.DesiredStatus == constant.AllocDesiredStatusEvict) &&
			(alloc.ClientStatus == constant.AllocClientStatusRunning ||
				alloc.ClientStatus == constant.AllocClientStatusPending) {
			logger.Warn("find down node and mark the running alloc unknown",
				zap.String("alloc", alloc.ID), zap.String("alloc.DesiredStatus", alloc.DesiredStatus),
				zap.String("alloc.ClientStatus", alloc.ClientStatus))
			plan.AppendStoppedAlloc(alloc, allocLost, constant.AllocClientStatusUnknown, "")
		}
	}
}
