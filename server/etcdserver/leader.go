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
	"go.uber.org/zap"
	"math/rand"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/pkg/v2/timetable"
)

const (
	// failedEvalUnblockInterval 重新进入调度器的时间间隔
	failedEvalUnblockInterval = 1 * time.Minute
	monitorQueueInterval      = 5 * time.Second
)

// leaderLoop 仅在主节点上运行
func (s *EtcdServer) leaderLoop(stopCh chan struct{}) {
RECONCILE:
	if s.isLeader() {
		if err := s.establishLeadership(stopCh); err != nil {
			s.Logger().Error("estab leader failed", zap.Error(err))
			if err := s.revokeLeadership(); err != nil {
				s.Logger().Error("failed to revoke leadership", zap.Error(err))
			}
			// interval := time.After(5 * time.Second)
			select {
			case <-stopCh:
				return
			default:
			}
			time.Sleep(5 * time.Second)
			goto RECONCILE
		}
	}
}

// establishLeadership 建立主节点主要逻辑
func (s *EtcdServer) establishLeadership(stopCh chan struct{}) error {
	// node heartbeat
	s.dispatcherHeartbeater.SetEnabled(true)
	if err := s.dispatcherHeartbeater.initializeHeartbeatTimers(); err != nil {
		s.logger.Error("heartbeat timer setup failed", zap.Error(err))
		return err
	}

	s.allocationQueue.SetEnabled(true)
	go s.allocator.planApply(stopCh)

	s.evalBroker.SetEnabled(true)
	s.blockedEvals.SetEnabled(true)
	s.jobBirdEye.SetEnabled(true)
	s.blockedEvals.SetTimetable(timetable.NewTimeTable(5*time.Minute, 72*time.Hour))
	s.periodicDispatcher.SetEnabled(true)

	// restore evals
	if err := s.restoreEvals(); err != nil {
		return err
	}

	// periodic start
	if err := s.restorePeriodicDispatcher(); err != nil {
		return err
	}

	go s.schedulePeriodic(stopCh)
	// restore resources

	// s.GoAttach(s.reapFailedEvaluations)
	go s.reapFailedEvaluations(stopCh)

	// Reap any duplicate blocked evaluations
	go s.reapDupBlockedEvaluations(stopCh)

	go s.periodicUnblockFailedEvals(stopCh)

	return nil
}

func (s *EtcdServer) revokeLeadership() error {
	// 设置队列不可用
	s.periodicDispatcher.SetEnabled(false)
	s.evalBroker.SetEnabled(false)
	s.blockedEvals.SetEnabled(false)
	s.allocationQueue.SetEnabled(false)
	s.jobBirdEye.SetEnabled(false)
	if err := s.dispatcherHeartbeater.clearAllHeartbeatTimers(); err != nil {
		return err
	}
	s.dispatcherHeartbeater.SetEnabled(false)
	return nil
}

// 上次未运行完成的分配重新入队列
func (s *EtcdServer) restoreEvals() error {
	rsp, err := s.EvalList(s.ctx, &etcdserverpb.ScheduleEvalListRequest{})
	if err != nil {
		return fmt.Errorf("failed to get evaluations: %v", err)
	}
	if rsp == nil || rsp.Data == nil {
		return nil
	}
	for _, evalRsp := range rsp.Data {
		eval := convertEvalFromEvalucation(evalRsp)
		if eval.ShouldEnqueue() {
			s.evalBroker.Enqueue(eval)
		} else if eval.ShouldBlock() {
			s.blockedEvals.Block(eval)
		}
	}
	return nil
}

// periodicUnblockFailedEvals periodically unblocks failed, blocked evaluations.
func (s *EtcdServer) periodicUnblockFailedEvals(stopCh chan struct{}) {
	ticker := time.NewTicker(failedEvalUnblockInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Unblock the failed allocations
			s.blockedEvals.UnblockFailed()
		}
	}
}

/**
 * 失败任务处理
 */
func (s *EtcdServer) reapFailedEvaluations(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			// Scan for a failed evaluation
			eval, token, err := s.evalBroker.Dequeue([]string{constant.QueueNameFailed}, time.Second)
			if err != nil {
				return
			}
			if eval == nil {
				continue
			}

			// Update the status to failed
			updateEval := eval.Copy()
			updateEval.Status = constant.EvalStatusFailed
			updateEval.StatusDescription = fmt.Sprintf("evaluation reached delivery limit (%d)", 3)
			s.logger.Warn("eval reached delivery limit, marking as failed",
				zap.Reflect("eval", updateEval))

			if eval.Type != constant.PlanTypeCore {
				updateEval.UpdateTime = xtime.NewFormatTime(time.Now())
				shouldCreate := s.shouldCreateFollowupEval(eval)

				if shouldCreate {
					followupEvalWait := 1*time.Minute +
						time.Duration(rand.Int63n(int64(5*time.Minute)))
					followupEval := eval.CreateFailedFollowUpEval(followupEvalWait)
					updateEval.NextEval = followupEval.ID
					// Update via Raft
					_, followError := s.EvalAdd(s.ctx, &etcdserverpb.ScheduleEvalAddRequest{
						Id:                followupEval.ID,
						Namespace:         followupEval.Namespace,
						Priority:          uint64(followupEval.Priority),
						Type:              followupEval.Type,
						TriggeredBy:       followupEval.TriggeredBy,
						PlanId:            followupEval.PlanID,
						JobId:             followupEval.JobID,
						NodeId:            followupEval.NodeID,
						Status:            followupEval.Status,
						StatusDescription: followupEval.StatusDescription,
						PreviousEval:      followupEval.PreviousEval,
					})
					if followError != nil {
						s.logger.Warn("eval create follow up failed",
							zap.Reflect("eval", followupEval), zap.Error(followError))
					}
				} else {
					// 此处将job状态更新为失败，并给出原因
					_, updateJobErr := s.JobUpdate(s.ctx, &etcdserverpb.ScheduleJobStatusUpdateRequest{
						Id:          updateEval.JobID,
						Status:      updateEval.Status,
						Description: updateEval.StatusDescription,
					})
					if updateJobErr != nil {
						s.logger.Warn("job update failed",
							zap.Reflect("job-id", updateEval.JobID), zap.Error(updateJobErr))
					}
				}
				_, updateError := s.EvalUpdate(s.ctx, &etcdserverpb.ScheduleEvalStatusUpdateRequest{
					Id:                updateEval.ID,
					Status:            updateEval.Status,
					StatusDescription: updateEval.StatusDescription,
					NextEval:          updateEval.NextEval,
				})
				if updateError != nil {
					s.logger.Warn("eval update failed",
						zap.Reflect("eval", updateEval), zap.Error(updateError))
				}
			}
			// Ack completion
			s.evalBroker.Ack(eval.ID, token)
		}
	}
}

/**
 * 阻塞任务处理
 */
func (s *EtcdServer) reapDupBlockedEvaluations(stopCh chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
			// Scan for duplicate blocked evals.
			dups := s.blockedEvals.GetDuplicates(time.Second)
			if dups == nil {
				continue
			}

			for _, dup := range dups {
				// Update the status to cancelled
				newEval := dup.Copy()
				newEval.Status = constant.EvalStatusCancelled
				newEval.StatusDescription = fmt.Sprintf("existing blocked evaluation exists for job %q", newEval.JobID)
				newEval.UpdateTime = xtime.NewFormatTime(time.Now())
				_, updateError := s.EvalUpdate(s.ctx, &etcdserverpb.ScheduleEvalStatusUpdateRequest{
					Id:                newEval.ID,
					Status:            newEval.Status,
					StatusDescription: newEval.StatusDescription,
				})
				if updateError != nil {
					s.logger.Warn("cancelled eval update failed",
						zap.Reflect("eval", newEval), zap.Error(updateError))
				}
			}
		}
	}
}

func (s *EtcdServer) shouldCreateFollowupEval(eval *domain.Evaluation) bool {
	// 尝试N次之后如果还是不行，放弃
	resp, err := s.scheduleStore.EvalList(&etcdserverpb.ScheduleEvalListRequest{
		Namespace: eval.Namespace,
		PlanId:    eval.PlanID,
		JobId:     eval.JobID,
		Status:    constant.EvalStatusFailed,
	})
	if err != nil {
		s.logger.Error("failed to find exist eval and then create a follow-up",
			zap.Reflect("eval", eval), zap.Error(err))
	}
	evals := resp.Data
	if evals != nil {
		if len(evals) > 10 {
			s.logger.Warn("该job已分配失败重试10次，达到最大重试次数，放弃该job的运行",
				zap.String("jobId", eval.JobID), zap.String("planId", eval.PlanID))
			// Ack completion
			return false
		} else {
			s.logger.Debug("job分配失败，创建重试任务", zap.String("jobId", eval.JobID), zap.Int("times", len(evals)))
		}
	}
	return true
}
