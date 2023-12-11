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

package etcdserver

import (
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/server/v2/scheduler"
)

const (
	DefaultEvalGcHours               time.Duration = -168 * time.Hour
	DefaultJobBlockGcHours           time.Duration = -48 * time.Hour
	TTLStrategySecondAfterCompletion time.Duration = -600 * time.Second // 10分钟后自动清理
	TTLStrategySecondAfterFailure    time.Duration = -24 * time.Hour    // 24小时后自动清理
)

type CoreScheduler struct {
	srv    *EtcdServer
	logger *zap.Logger
	store  *scheduler.ScheduleStore
}

// NewCoreScheduler 核心调度器主要用于垃圾回收
func NewCoreScheduler(server *EtcdServer, logger *zap.Logger) scheduler.Scheduler {
	s := &CoreScheduler{
		srv:    server,
		logger: logger,
		store:  &server.scheduleStore,
	}
	return s
}

func (c *CoreScheduler) Process(evaluation *domain.Evaluation) error {
	c.logger.Debug("----start process the core scheduler", zap.String("eval", evaluation.ID))
	switch evaluation.JobID {
	case constant.CoreJobEvalGC:
		return c.evalGC()
	case constant.CoreJobJobGC:
		return c.jobGC()
	case constant.CoreJobNodeGC:
		return c.nodeGC()
	case constant.CoreAllocationGC:
		return c.allocGC()
	default:
		return fmt.Errorf("core scheduler cannot handle job '%s'", evaluation.JobID)
	}
}

// 将下线很久的client节点从列表中剔除，并删除心跳的监控
func (c *CoreScheduler) nodeGC() error {
	resp, err := c.srv.NodeList(c.srv.ctx, &etcdserverpb.NodeListRequest{})
	if err != nil {
		return err
	}
	var gcNode []string
	nodes := resp.Data
OUTER:
	for _, pbNode := range nodes {
		if pbNode == nil {
			break
		}
		node := domain.ConvertNodeFromPb(pbNode)
		tenMinuteAgo := time.Now().Add(-10 * time.Minute)
		c.logger.Debug("the node status in gc", zap.Reflect("node", node), zap.Int64("ten-ago", tenMinuteAgo.UnixMicro()))
		if !node.TerminalStatus() || node.StatusUpdatedAt > tenMinuteAgo.UnixMicro() {
			continue
		}
		allocListResp, err := c.srv.AllocationList(c.srv.ctx, &etcdserverpb.ScheduleAllocationListRequest{
			NodeId: node.ID,
			Status: constant.AllocClientStatusPending,
		})
		if err != nil {
			c.logger.Warn("find allocation by nodeId failed", zap.String("err", err.Error()))
			continue
		}

		if allocListResp != nil && allocListResp.Data != nil {
			allocs := allocListResp.Data
			for _, alloc := range allocs {
				allocation := domain.ConvertAllocation(alloc)
				if !allocation.TerminalStatus() {
					// 有任务还是非完成态，先不移除
					continue OUTER
				}
			}
		}
		gcNode = append(gcNode, node.ID)
	}
	c.logger.Debug("the gc Node size", zap.Int("size", len(gcNode)))

	if len(gcNode) == 0 {
		return nil
	}
	return c.nodeReap(gcNode)
}

func (c *CoreScheduler) nodeReap(nodeIDs []string) error {
	for _, id := range nodeIDs {
		_, err := c.srv.NodeDelete(c.srv.ctx, &etcdserverpb.NodeDeleteRequest{
			Id: id,
		})
		if err != nil {
			c.logger.Warn("can't deregister the node ", zap.String("node-id", id), zap.Error(err))
			return err
		}
	}
	return nil
}

// evaluation会随时间增长而一直增加，其实历史任务已经运行完成没必要一直保存
func (c *CoreScheduler) evalGC() error {
	var someDaysAgo time.Time
	if c.srv.Cfg.EvalGCThreshold > 0 {
		someDaysAgo = time.Now().Add(c.srv.Cfg.EvalGCThreshold * -1)
	} else {
		someDaysAgo = time.Now().Add(DefaultEvalGcHours)
	}
	endTime, _ := timestamppb.TimestampProto(someDaysAgo)
	// 获取这个时间之前的eval
	resp, err := c.srv.EvalList(c.srv.ctx, &etcdserverpb.ScheduleEvalListRequest{
		EndTime: endTime,
	})
	if err != nil {
		return err
	}
	if resp.Data == nil || len(resp.Data) == 0 {
		return nil
	}
	var gcAlloc, gcEval []string
	for _, evaluation := range resp.Data {
		evalDomain := domain.ConvertEvaluationFromPb(evaluation)
		c.logger.Debug("--the eval to be delete--",
			zap.String("eval-id", evaluation.Id),
			zap.String("status", evaluation.Status),
			zap.String("ctm", evaluation.CreateTime.String()))
		if evalDomain.TerminalStatus() {
			// delete it!
			allowEvalGc, allocs, err := c.evalGCInternal(evalDomain, endTime)
			if err != nil {
				return err
			}
			if allowEvalGc {
				gcEval = append(gcEval, evalDomain.ID)
			}
			gcAlloc = append(gcAlloc, allocs...)
		}
	}
	if len(gcEval) == 0 && len(gcAlloc) == 0 {
		return nil
	}
	c.logger.Debug("eval GC found eligibile objects",
		zap.Int("evals", len(gcEval)),
		zap.Int("allocs", len(gcAlloc)),
		zap.Strings("eval-ids", gcEval),
		zap.Strings("alloc-ids", gcAlloc),
	)
	return c.evalReap(gcEval, gcAlloc)
}

func (c *CoreScheduler) evalGCInternal(eval *domain.Evaluation, endTime *timestamppb.Timestamp) (bool, []string, error) {
	allocResp, err := c.srv.AllocationList(c.srv.ctx, &etcdserverpb.ScheduleAllocationListRequest{EvalId: eval.ID})
	if err != nil {
		return false, nil, err
	}
	allowEvalGc := true
	var gcAllocIDs []string
	if allocResp != nil && allocResp.Data != nil && len(allocResp.Data) > 0 {
		for _, alloc := range allocResp.Data {
			allocDomain := domain.ConvertAllocation(alloc)
			if alloc.CreateTime != nil && alloc.CreateTime.Compare(endTime) > 0 {
				allowEvalGc = false
				continue
			}
			if !allocDomain.TerminalStatus() {
				allowEvalGc = false
			} else {
				gcAllocIDs = append(gcAllocIDs, alloc.Id)
			}
		}
	}
	return allowEvalGc, gcAllocIDs, nil
}

func (c *CoreScheduler) jobGC() error {
	// 创建时间是N小时之前并且还是pending或running状态，那么可以认为这个任务卡住了，直接给GC掉
	errJobBlocked := c.jobBlockGcInternal()
	if errJobBlocked != nil {
		c.logger.Error("job long pending GC failed to update jobs",
			zap.Error(errJobBlocked))
	}
	// 超过预设时间还没回收掉的走这里
	// TODO 发现存在job中为pending但是eval中没记录的情况
	var someDaysAgo time.Time
	if c.srv.Cfg.JobGCThreshold > 0 {
		someDaysAgo = time.Now().Add(c.srv.Cfg.JobGCThreshold * -1)
	} else {
		someDaysAgo = time.Now().Add(168 * time.Hour * -1)
	}
	endTime, _ := timestamppb.TimestampProto(someDaysAgo)
	// 获取这个时间之前的eval
	resp, err := c.srv.JobList(c.srv.ctx, &etcdserverpb.ScheduleJobListRequest{
		EndTime: endTime,
	})
	if err != nil {
		return err
	}
	if resp.Data == nil || len(resp.Data) == 0 {
		return nil
	}
	var gcAlloc, gcJob, gcEval []string
OUTER:
	for _, job := range resp.Data {
		jobDomain := domain.ConvertJob(job)
		if jobDomain.TerminalStatus() {
			// delete it!
			evalResp, err := c.srv.EvalList(c.srv.ctx, &etcdserverpb.ScheduleEvalListRequest{JobId: jobDomain.ID})
			if err != nil {
				c.logger.Error("job GC failed to get evals for job", zap.String("job", jobDomain.ID), zap.Error(err))
				continue
			}
			allEvalsGC := true
			if evalResp != nil && evalResp.Data != nil && len(evalResp.Data) > 0 {
				for _, eval := range evalResp.Data {
					evalDomain := domain.ConvertEvaluationFromPb(eval)
					allowEvalGc, allocs, err := c.evalGCInternal(evalDomain, endTime)
					if err != nil {
						continue OUTER
					} else if allowEvalGc {
						gcEval = append(gcEval, eval.Id)
						gcAlloc = append(gcAlloc, allocs...)
					} else {
						allEvalsGC = false
						break
					}
				}
			}
			if allEvalsGC {
				gcJob = append(gcJob, jobDomain.ID)
			}
		}
	}
	if len(gcJob) == 0 && len(gcAlloc) == 0 && len(gcEval) == 0 {
		return nil
	}
	c.logger.Debug("eval GC found eligibile objects",
		zap.Int("evals", len(gcJob)),
		zap.Int("jobs", len(gcJob)),
		zap.Int("allocs", len(gcAlloc)))
	if len(gcEval) > 0 || len(gcAlloc) > 0 {
		if err := c.evalReap(gcEval, gcAlloc); err != nil {
			return err
		}
	}
	return c.jobRepa(gcJob)
}

func (c *CoreScheduler) allocGC() error {
	errPending := c.allocLongPendingGCInternal()
	if errPending != nil {
		c.logger.Error("allocation long pending GC failed to update allocations",
			zap.Error(errPending))
	}
	errTtl := c.allocGCInternal()
	if errTtl != nil {
		c.logger.Error("allocation TTL GC failed to delete allocations",
			zap.Error(errTtl))
	}
	if errPending != nil {
		return errPending
	}
	if errTtl != nil {
		return errTtl
	}
	return nil
}

func (c *CoreScheduler) allocGCInternal() error {
	// 鉴于boltdb的性能，完成态的数据需要被清理掉以防引发性能问题
	someMinuteAgo := time.Now().Add(-10 * time.Minute)
	endTime, _ := timestamppb.TimestampProto(someMinuteAgo)
	allocResp, err := c.srv.AllocationList(c.srv.ctx, &etcdserverpb.ScheduleAllocationListRequest{
		Status: fmt.Sprintf("%s,%s,%s,%s,%s",
			constant.AllocClientStatusComplete,
			constant.AllocClientStatusFailed,
			constant.AllocClientStatusExpired,
			constant.AllocClientStatusSkipped,
			constant.AllocClientStatusCancelled),
		EndTime: endTime,
	})
	if err != nil {
		return err
	}
	var gcAllocIDs []string
	var gcEvalIds []string
	if allocResp != nil && allocResp.Data != nil && len(allocResp.Data) > 0 {
		beforeComplete, _ := timestamppb.TimestampProto(time.Now().Add(TTLStrategySecondAfterCompletion))
		beforeFailed, _ := timestamppb.TimestampProto(time.Now().Add(TTLStrategySecondAfterFailure))
		for _, alloc := range allocResp.Data {
			if constant.AllocClientStatusComplete == alloc.ClientStatus &&
				alloc.UpdateTime.Compare(beforeComplete) < 0 {
				gcAllocIDs = append(gcAllocIDs, alloc.Id)
			} else if alloc.UpdateTime.Compare(beforeFailed) < 0 {
				gcAllocIDs = append(gcAllocIDs, alloc.Id)
			}
		}
	}
	if len(gcAllocIDs) > 0 {
		return c.evalReap(gcEvalIds, gcAllocIDs)
	}
	return nil
}

func (c *CoreScheduler) evalReap(evals, allocs []string) error {
	if len(evals) > 0 {
		c.logger.Debug("start gc evals", zap.Strings("eval-ids", evals))
	}
	if len(allocs) > 0 {
		c.logger.Debug("start gc allocs", zap.Strings("alloc-ids", allocs))
	}
	for _, allocId := range allocs {
		_, err := c.srv.AllocationDelete(c.srv.ctx, &etcdserverpb.ScheduleAllocationDeleteRequest{Id: allocId})
		if err != nil {
			c.logger.Debug("failed to delete allocation", zap.Error(err))
			continue
		}
	}
	for _, evalId := range evals {
		_, err := c.srv.EvalDelete(c.srv.ctx, &etcdserverpb.ScheduleEvalDeleteRequest{Id: evalId})
		if err != nil {
			c.logger.Debug("failed to delete eval", zap.Error(err))
			continue
		}
	}
	return nil
}

func (c *CoreScheduler) jobRepa(jobs []string) error {
	if len(jobs) > 0 {
		c.logger.Debug("start gc jobs", zap.Strings("job-ids", jobs))
	}
	for _, jobId := range jobs {
		_, err := c.srv.JobDelete(c.srv.ctx, &etcdserverpb.ScheduleJobDeleteRequest{Id: jobId})
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *CoreScheduler) allocLongPendingGCInternal() error {
	// 有一种情况是一次性任务，期望skip但是状态一直是pending，影响后续任务的执行
	someMinuteAgo := time.Now().Add(2 * time.Minute * -1)
	endTime, _ := timestamppb.TimestampProto(someMinuteAgo)
	// 获取这个时间之前的allocation
	resp, err := c.srv.AllocationList(c.srv.ctx, &etcdserverpb.ScheduleAllocationListRequest{
		EndTime: endTime,
		Status:  constant.AllocClientStatusPending,
	})
	if err != nil {
		return err
	}
	if resp.Data == nil || len(resp.Data) == 0 {
		return nil
	}
	var updateAllocIds []string
	nodeResp, err := c.srv.NodeList(c.srv.ctx, &etcdserverpb.NodeListRequest{})
	if err != nil {
		return err
	}

	var downNodeList []string
	if nodeResp != nil && nodeResp.Data != nil {
		var nodes []*etcdserverpb.Node
		nodes = nodeResp.Data
		for _, node := range nodes {
			if constant.NodeStatusDown == node.Status {
				downNodeList = append(downNodeList, node.ID)
			}
		}
	}
	for _, alloc := range resp.Data {
		if constant.AllocDesiredStatusSkip == alloc.DesiredStatus {
			updateAllocIds = append(updateAllocIds, alloc.Id)
		} else {
			// 节点状态为下线的，直接失败
			for _, nodeId := range downNodeList {
				if alloc.NodeId == nodeId {
					updateAllocIds = append(updateAllocIds, alloc.Id)
				}
			}
		}
	}
	if len(updateAllocIds) > 0 {
		_, err := c.srv.AllocationUpdate(c.srv.ctx, &etcdserverpb.ScheduleAllocationStatusUpdateRequest{
			Ids:         updateAllocIds,
			Status:      constant.AllocClientStatusFailed,
			Description: "update by gc for 2 minute later",
		})
		if err != nil {
			c.logger.Error("allocation for skip GC failed to update allocation status",
				zap.Strings("allocation", updateAllocIds),
				zap.Error(err))
			return err
		}
	}
	return nil
}

// jobBlockGcInternal 将待运行或运行时间过长的任务终结掉
func (c *CoreScheduler) jobBlockGcInternal() error {
	blockJobHoursAgo := time.Now().Add(DefaultJobBlockGcHours)
	endTime, _ := timestamppb.TimestampProto(blockJobHoursAgo)
	// 获取这个时间之前的job
	resp, err := c.srv.JobList(c.srv.ctx, &etcdserverpb.ScheduleJobListRequest{
		EndTime: endTime,
		Status:  fmt.Sprintf("%s,%s", constant.JobStatusPending, constant.JobStatusRunning),
	})
	if err != nil {
		return err
	}
	if resp.Data == nil || len(resp.Data) == 0 {
		return nil
	}
	for _, job := range resp.Data {
		_, err := c.srv.JobUpdate(c.srv.ctx, &etcdserverpb.ScheduleJobStatusUpdateRequest{
			Id:          job.Id,
			Status:      constant.JobStatusFailed,
			Description: fmt.Sprintf("任务%s运行时间超过%s自动停止", job.Id, DefaultJobBlockGcHours.String()),
		})
		if err != nil {
			c.logger.Error("job GC failed to get update status of job", zap.String("job", job.Id), zap.Error(err))
			continue
		}
	}
	return nil
}
