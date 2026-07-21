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

package queueservice

import (
	"fmt"
	"go.uber.org/zap"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/schedulepb"
	"yunli.com/jobpool/server/v2/scheduler"
)

// QueueService 队列操作统一在这里，因为server中leader需要进行该处理，
// 非leader需要转发，其中http也要实现这部分逻辑，防止重复在此一并处理
type QueueService interface {
	// EvalBrokerQueue 评估入队列
	EvalBrokerQueue(eval *domain.Evaluation) (err error)
	// EvalBrokerDequeue 评估出队列
	EvalBrokerDequeue(r *pb.ScheduleEvalDequeueRequest) (resp *pb.ScheduleEvalDequeueResponse, err error)
	// EvalBrokerAck 评估确认
	EvalBrokerAck(id string, token string) error
	// EvalBrokerNack 评估未得到确认
	EvalBrokerNack(id string, token string) error
	// AllocationEnqueue 分配入队列
	AllocationEnqueue(r *pb.PlanAllocationEnqueueRequest) (allocations []*schedulepb.Allocation, err error)
	// QueueStats 队列状态查询
	QueueStats(tp string) (detail *pb.QueueDetail, err error)
	// QueueJobViewStats 任务地图
	QueueJobViewStats(namespace string) (detail *pb.QueueJobStats, err error)
}

const (
	STATUS_ON  = "on"
	STATUS_OFF = "off"
)

type QueueOperator struct {
	evalBroker   *scheduler.EvalBroker
	planQueue    *scheduler.AllocationQueue
	jobBirdEye   *scheduler.JobBirdEyeView
	blockedEvals *scheduler.BlockedEvals
	logger       *zap.Logger
}

func NewQueueOperator(l *scheduler.EvalBroker, q *scheduler.AllocationQueue,
	j *scheduler.JobBirdEyeView, b *scheduler.BlockedEvals,
	log *zap.Logger) *QueueOperator {
	return &QueueOperator{
		evalBroker:   l,
		planQueue:    q,
		jobBirdEye:   j,
		blockedEvals: b,
		logger:       log,
	}
}

func (q *QueueOperator) EvalBrokerQueue(eval *domain.Evaluation) (*pb.ScheduleEvalAddResponse, error) {
	// eval入队列
	if eval.ShouldEnqueue() {
		q.evalBroker.Enqueue(eval)
		q.jobBirdEye.Enqueue(eval)
	} else if eval.ShouldBlock() {
		q.blockedEvals.Block(eval)
		q.jobBirdEye.Enqueue(eval)
	} else if eval.Status == constant.EvalStatusComplete {
		q.blockedEvals.Untrack(eval.PlanID, eval.Namespace)
	}
	return &pb.ScheduleEvalAddResponse{
		Header: &pb.ResponseHeader{},
	}, nil
}

func (q *QueueOperator) EvalBrokerDequeue(r *pb.ScheduleEvalDequeueRequest) (response *pb.ScheduleEvalDequeueResponse, err error) {
	dur, err := time.ParseDuration(fmt.Sprintf("%dms", r.Timeout))
	if err != nil {
		q.logger.Warn("dequeue from eval broker error", zap.Error(err))
		return nil, err
	}
	eval, token, err := q.evalBroker.Dequeue(r.Schedulers, dur)
	if err != nil {
		q.logger.Warn("dequeue from eval broker error", zap.Error(err))
		return nil, err
	}
	var data *schedulepb.Evaluation
	if eval != nil {
		data = domain.ConvertEvaluation(eval)
	}
	resp := &pb.ScheduleEvalDequeueResponse{
		Data:   data,
		Token:  token,
		Header: &pb.ResponseHeader{},
	}
	return resp, nil
}

func (q *QueueOperator) EvalBrokerAck(id string, token string) error {
	err := q.evalBroker.Ack(id, token)
	if err != nil {
		q.logger.Warn("ack from eval broker error", zap.Error(err))
		return err
	}
	return nil
}

func (q *QueueOperator) EvalBrokerNack(id string, token string) error {
	err := q.evalBroker.Nack(id, token)
	if err != nil {
		q.logger.Warn("nack from eval broker error", zap.Error(err))
		return err
	}
	return nil
}

func (q *QueueOperator) AllocationEnqueue(r *pb.PlanAllocationEnqueueRequest) (allocations []*schedulepb.Allocation, err error) {
	if r.Allocations == nil {
		return nil, fmt.Errorf("cannot submit nil plan allocation")
	}
	evalId := r.EvalId
	token := r.EvalToken
	if err := q.evalBroker.PauseNackTimeout(evalId, token); err != nil {
		return nil, err
	}
	defer q.evalBroker.ResumeNackTimeout(evalId, token)
	// Submit the plan to the queue
	plan := domain.ConvertPlanAllocation(r)
	future, err := q.planQueue.Enqueue(plan)
	if err != nil {
		return nil, err
	}
	// Wait for the results
	result, err := future.Wait()
	if err != nil {
		return nil, err
	}
	var allocs []*schedulepb.Allocation
	if result != nil {
		if result.NodeAllocation != nil && len(result.NodeAllocation) > 0 {
			// 后续拼装
			allocs = domain.ConvertPlanAllocationToPb(result.NodeAllocation)
		}
		if result.NodeUpdate != nil && len(result.NodeUpdate) > 0 {
			allocs = domain.ConvertPlanAllocationToPb(result.NodeUpdate)
		}
	}
	return allocs, nil
}

func (q *QueueOperator) QueueStats(tp string) (detail *pb.QueueDetail, err error) {
	data := pb.QueueDetail{}
	if constant.QueueTypeAllocation == tp {
		stats := q.planQueue.Stats()
		data.Total = int32(stats.Depth)
	} else if constant.QueueTypeEvaluation == tp {
		stats := q.evalBroker.Stats()
		data.TotalBlocked = int32(stats.TotalBlocked)
		data.TotalDelayed = int32(len(stats.DelayedEvals))
		data.TotalReady = int32(stats.TotalReady)
		data.TotalUnacked = int32(stats.TotalUnacked)
		data.TotalFailed = int32(stats.TotalFailedQueue)
	} else {
		return nil, fmt.Errorf("not support type %s", tp)
	}
	return &data, nil
}

func (q *QueueOperator) QueueJobViewStats(namespace string) (detail *pb.QueueJobStats, err error) {
	stats := q.jobBirdEye.Stats(namespace)
	return &pb.QueueJobStats{
		Total:       int32(stats.TotalRunning + stats.TotalRetry + stats.TotalPending + stats.TotalUnUsed),
		Pending:     int32(stats.TotalPending),
		Running:     int32(stats.TotalRunning),
		Retry:       int32(stats.TotalRetry),
		UnUsed:      int32(stats.TotalUnUsed),
		RunningJobs: stats.RunningJobs,
		PendingJobs: stats.PendingJobs,
	}, nil
}

func (q *QueueOperator) QueueForbidden() {
	// 设置队列不可用
	q.evalBroker.SetEnabled(false)
	q.blockedEvals.SetEnabled(false)
	q.planQueue.SetEnabled(false)
	q.jobBirdEye.SetEnabled(false)
}

func (q *QueueOperator) QueueStatusDetail() (detail *pb.QueueEnableStats, err error) {
	queueStatus := &pb.QueueEnableStats{}
	if q.planQueue.Enabled() {
		queueStatus.AllocationQueue = STATUS_ON
	} else {
		queueStatus.AllocationQueue = STATUS_OFF
	}
	if q.evalBroker.Enabled() {
		queueStatus.EvalBroker = STATUS_ON
	} else {
		queueStatus.EvalBroker = STATUS_OFF
	}
	if q.blockedEvals.Enabled() {
		queueStatus.BlockedEvals = STATUS_ON
	} else {
		queueStatus.BlockedEvals = STATUS_OFF
	}
	if q.jobBirdEye.Enabled() {
		queueStatus.JobBirdEye = STATUS_ON
	} else {
		queueStatus.JobBirdEye = STATUS_OFF
	}
	return queueStatus, nil
}
