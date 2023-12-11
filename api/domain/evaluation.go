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

package domain

import (
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/helper/ulid"
	"yunli.com/jobpool/api/v2/schedulepb"
)

// Evaluation 任务评估基础信息
type Evaluation struct {
	// ID 唯一编码，使用UUID
	ID                string
	Namespace         string
	Priority          int
	Type              string
	TriggeredBy       string // 由什么类型的调度器生成的
	PlanID            string
	JobID             string // plan -> job -> eval
	NodeID            string
	Status            string
	StatusDescription string
	// Deprecated
	Wait time.Duration
	// WaitUntil is the time when this eval should be run. This is used to
	// supported delayed rescheduling of failed allocations
	WaitUntil    time.Time
	NextEval     string
	PreviousEval string
	BlockedEval  string

	// RelatedEvals is a list of all the evaluations that are related (next,
	// previous, or blocked) to this one. It may be nil if not requested.
	RelatedEvals []*EvaluationStub

	// ClassEligibility tracks computed node classes that have been explicitly
	// marked as eligible or ineligible.
	ClassEligibility map[string]bool

	// EscapedComputedClass marks whether the plan has constraints that are not
	// captured by computed node classes.
	EscapedComputedClass bool

	// AnnotatePlan triggers the scheduler to provide additional annotations
	// during the evaluation. This should not be set during normal operations.
	AnnotatePlan bool

	// QueuedAllocations is the number of unplaced allocations at the time the
	// evaluation was processed. The map is keyed by Task Group names.
	QueuedAllocations map[string]int
	CreateTime        xtime.FormatTime `json:"createTime"`
	UpdateTime        xtime.FormatTime `json:"updateTime"`
}

type EvaluationStub struct {
	ID                string
	Namespace         string
	Priority          int
	Type              string
	TriggeredBy       string
	JobID             string
	PlanID            string
	NodeID            string
	DeploymentID      string
	Status            string
	StatusDescription string
	WaitUntil         time.Time
	NextEval          string
	PreviousEval      string
	BlockedEval       string
	CreateTime        xtime.FormatTime
	UpdateTime        xtime.FormatTime
}

// GetID implements the IDGetter interface, required for pagination.
func (e *Evaluation) GetID() string {
	if e == nil {
		return ""
	}
	return e.ID
}

func (e *Evaluation) Stub() *EvaluationStub {
	if e == nil {
		return nil
	}

	return &EvaluationStub{
		ID:                e.ID,
		Namespace:         e.Namespace,
		Priority:          e.Priority,
		Type:              e.Type,
		TriggeredBy:       e.TriggeredBy,
		PlanID:            e.PlanID,
		JobID:             e.JobID,
		NodeID:            e.NodeID,
		Status:            e.Status,
		StatusDescription: e.StatusDescription,
		WaitUntil:         e.WaitUntil,
		NextEval:          e.NextEval,
		PreviousEval:      e.PreviousEval,
		BlockedEval:       e.BlockedEval,
		CreateTime:        e.CreateTime,
		UpdateTime:        e.UpdateTime,
	}
}

func (e *Evaluation) Copy() *Evaluation {
	if e == nil {
		return nil
	}
	ne := new(Evaluation)
	*ne = *e

	// Copy ClassEligibility
	if e.ClassEligibility != nil {
		classes := make(map[string]bool, len(e.ClassEligibility))
		for class, elig := range e.ClassEligibility {
			classes[class] = elig
		}
		ne.ClassEligibility = classes
	}

	// Copy queued allocations
	if e.QueuedAllocations != nil {
		queuedAllocations := make(map[string]int, len(e.QueuedAllocations))
		for tg, num := range e.QueuedAllocations {
			queuedAllocations[tg] = num
		}
		ne.QueuedAllocations = queuedAllocations
	}

	return ne
}

func (e *Evaluation) ShouldEnqueue() bool {
	switch e.Status {
	case constant.EvalStatusPending:
		return true
	case constant.EvalStatusComplete, constant.EvalStatusFailed, constant.EvalStatusBlocked, constant.EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// ShouldBlock checks if a given evaluation should be entered into the blocked
// eval tracker.
func (e *Evaluation) ShouldBlock() bool {
	switch e.Status {
	case constant.EvalStatusBlocked:
		return true
	case constant.EvalStatusComplete, constant.EvalStatusFailed, constant.EvalStatusPending, constant.EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// TerminalStatus returns if the current status is terminal and
// will no longer transition.
func (e *Evaluation) TerminalStatus() bool {
	switch e.Status {
	case constant.EvalStatusComplete, constant.EvalStatusFailed, constant.EvalStatusCancelled:
		return true
	default:
		return false
	}
}

func (e *Evaluation) CreateFailedFollowUpEval(wait time.Duration) *Evaluation {
	now := xtime.NewFormatTime(time.Now())
	return &Evaluation{
		ID:           ulid.Generate(),
		Namespace:    e.Namespace,
		Priority:     e.Priority,
		Type:         e.Type,
		TriggeredBy:  constant.EvalTriggerFailedFollowUp,
		PlanID:       e.PlanID,
		JobID:        e.JobID,
		Status:       constant.EvalStatusPending,
		Wait:         wait,
		PreviousEval: e.ID,
		CreateTime:   now,
		UpdateTime:   now,
	}
}

func ConvertEvaluation(eval *Evaluation) *schedulepb.Evaluation {
	data := &schedulepb.Evaluation{
		Id:                eval.ID,
		Namespace:         eval.Namespace,
		Priority:          uint64(eval.Priority),
		Type:              eval.Type,
		TriggeredBy:       eval.TriggeredBy,
		PlanId:            eval.PlanID,
		JobId:             eval.JobID,
		NodeId:            eval.NodeID,
		Status:            eval.Status,
		StatusDescription: eval.StatusDescription,
		NextEval:          eval.NextEval,
		PreviousEval:      eval.PreviousEval,
		BlockedEval:       eval.BlockedEval,
	}
	var create *timestamppb.Timestamp
	var update *timestamppb.Timestamp
	if eval.CreateTime != "" {
		create, _ = timestamppb.TimestampProto(eval.CreateTime.TimeValue())
	}
	if eval.UpdateTime != "" {
		update, _ = timestamppb.TimestampProto(eval.UpdateTime.TimeValue().UTC())
	}
	data.CreateTime = create
	data.UpdateTime = update
	return data
}

func ConvertEvaluationFromPb(eval *schedulepb.Evaluation) *Evaluation {
	data := &Evaluation{
		ID:                eval.Id,
		Namespace:         eval.Namespace,
		Priority:          int(eval.Priority),
		Type:              eval.Type,
		TriggeredBy:       eval.TriggeredBy,
		PlanID:            eval.PlanId,
		JobID:             eval.JobId,
		NodeID:            eval.NodeId,
		Status:            eval.Status,
		StatusDescription: eval.StatusDescription,
		NextEval:          eval.NextEval,
		PreviousEval:      eval.PreviousEval,
		BlockedEval:       eval.BlockedEval,
	}
	if eval.CreateTime != nil {
		data.CreateTime = xtime.NewFormatTime(time.Unix(eval.CreateTime.Seconds, int64(eval.CreateTime.Nanos)))
	}
	if eval.UpdateTime != nil {
		data.UpdateTime = xtime.NewFormatTime(time.Unix(eval.UpdateTime.Seconds, int64(eval.UpdateTime.Nanos)))
	}
	return data
}
