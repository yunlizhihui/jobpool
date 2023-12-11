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
	"time"
	"yunli.com/jobpool/api/v2/constant"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
)

// Job 任务，每次调度都会生成一个任务
type Job struct {
	ID            string           `json:"id"`
	PlanID        string           `json:"planId"`
	DerivedPlanId string           `json:"derivedPlanId"`
	Name          string           `json:"name"`
	Type          string           `json:"type"`
	Namespace     string           `json:"namespace"`
	OperatorId    string           `json:"operatorId"`
	Timeout       uint64           `json:"timeout"`
	Status        string           `json:"status"`
	Parameters    string           `json:"parameters"`
	Info          string           `json:"info"`
	CreateTime    xtime.FormatTime `json:"createTime"`
	UpdateTime    xtime.FormatTime `json:"updateTime"`
}

func (j *Job) Copy() *Job {
	if j == nil {
		return nil
	}
	nj := new(Job)
	*nj = *j
	return nj
}

func (e *Job) TerminalStatus() bool {
	switch e.Status {
	case constant.JobStatusComplete, constant.JobStatusFailed, constant.JobStatusCancelled, constant.JobStatusSkipped, constant.JobStatusExpired:
		return true
	default:
		return false
	}
}

func (j *Job) GetNamespace() string {
	if j == nil {
		return ""
	}
	return j.Namespace
}

func ConvertJob(job *schedulepb.Job) *Job {
	j := &Job{
		ID:            job.Id,
		PlanID:        job.PlanId,
		DerivedPlanId: job.DerivedPlanId,
		Name:          job.Name,
		Type:          job.Type,
		Namespace:     job.Namespace,
		OperatorId:    job.OperatorId,
		Timeout:       job.Timeout,
		Status:        job.Status,
		Info:          job.Info,
	}
	if job.Parameters != "" {
		j.Parameters = job.Parameters
	}
	// job.CreateTime.Nanos
	if job.CreateTime != nil {
		j.CreateTime = xtime.NewFormatTime(time.Unix(job.CreateTime.Seconds, int64(job.CreateTime.Nanos)))
	}
	if job.UpdateTime != nil {
		j.UpdateTime = xtime.NewFormatTime(time.Unix(job.UpdateTime.Seconds, int64(job.UpdateTime.Nanos)))
	}
	return j
}

func (job *Job) ConvertJobSlot() *JobSlot {
	return &JobSlot{
		ID:         job.ID,
		Name:       job.Name,
		CreateTime: job.CreateTime.TimeValue(),
	}
}
