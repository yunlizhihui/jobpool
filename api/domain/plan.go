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
	xtime "time"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/helper/funcs"
	"yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
)

// Plan 计划信息，是业务系统的入口，所有的作业都以plan的方式加入到系统中
type Plan struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	ParentID    string          `json:"parentId"` // 父编码（暂时未使用）
	Type        string          `json:"type"`     // 类别
	Namespace   string          `json:"namespace"`
	CreatorId   string          `json:"creatorId"`
	Priority    uint64          `json:"priority,optional"` // 优先级
	Stop        bool            `json:"-"`                 // 是否已停止
	Periodic    *PeriodicConfig `json:"periodic"`          // 周期配置
	AllAtOnce   bool            `json:"allAtOnce"`         // 一次性任务
	Status      string          `json:"status"`
	Description string          `json:"description"` // config and detail
	Synchronous bool            `json:"synchronous"` // 是否为同步任务
	Parameters  []byte          `json:"parameters"`
	SubmitTime  time.FormatTime `json:"submitTime"` // other attr
	CreateTime  time.FormatTime `json:"createTime"`
	UpdateTime  time.FormatTime `json:"updateTime"`
	Version     uint64          `json:"version"`
}

// NamespacedID returns the namespaced id useful for logging
func (p *Plan) NamespacedID() NamespacedID {
	return NamespacedID{
		ID:        p.ID,
		Namespace: p.Namespace,
	}
}

// Copy 复制一个对象
func (p *Plan) Copy() *Plan {
	if p == nil {
		return nil
	}
	nj := new(Plan)
	*nj = *p
	nj.Parameters = funcs.CopySliceByte(p.Parameters)
	return nj
}

// IsPeriodicActive 是否周期作业
func (p *Plan) IsPeriodicActive() bool {
	return p.IsPeriodic() && p.Periodic.Enabled && !p.Stopped()
}

// IsPeriodic 是否周期
func (p *Plan) IsPeriodic() bool {
	return p.Periodic != nil
}

// Stopped returns if a plan is stopped.
func (p *Plan) Stopped() bool {
	return p == nil || p.Stop
}

// Canonicalize 判断参数是否合法
func (p *Plan) Canonicalize() {
	if p == nil {
		return
	}

	// Ensure the job is in a namespace.
	if p.Namespace == "" {
		p.Namespace = "default"
	}

	if p.Periodic != nil {
		p.Periodic.Canonicalize()
	}
}

func (p *Plan) Merge(b *Plan) *Plan {
	var result *Plan
	if p == nil {
		result = &Plan{}
	} else {
		result = p.Copy()
	}
	if b == nil {
		return result
	}

	if b.ID != "" {
		result.ID = b.ID
	}
	if b.Name != "" {
		result.Name = b.Name
	}
	if b.ParentID != "" {
		result.ParentID = b.ParentID
	}
	if b.Type != "" {
		result.Type = b.Type
	}
	if b.Namespace != "" {
		result.Namespace = b.Namespace
	}
	if b.CreatorId != "" {
		result.CreatorId = b.CreatorId
	}
	if &b.Priority != nil {
		result.Priority = b.Priority
	}
	if &b.Stop != nil {
		result.Stop = b.Stop
	}
	if b.Periodic != nil {
		if result.Periodic != nil {
			result.Periodic = result.Periodic.Merge(b.Periodic)
		} else {
			result.Periodic = b.Periodic
		}
	}
	if &b.AllAtOnce != nil {
		result.AllAtOnce = b.AllAtOnce
	}
	if b.Status != "" {
		result.Status = b.Status
	}
	if b.Description != "" {
		result.Description = b.Description
	}
	if &b.Synchronous != nil {
		result.Synchronous = b.Synchronous
	}
	if b.Parameters != nil && len(b.Parameters) > 0 {
		result.Parameters = funcs.CopySliceByte(b.Parameters)
	}
	if b.SubmitTime != "" {
		result.SubmitTime = b.SubmitTime
	}
	if b.CreateTime != "" {
		result.CreateTime = b.CreateTime
	}
	if b.UpdateTime != "" {
		result.UpdateTime = b.UpdateTime
	}
	if &b.Version != nil {
		result.Version = b.Version
	}
	return result
}

func (p *Plan) GetNamespace() string {
	if p == nil {
		return ""
	}
	return p.Namespace
}

func ConvertPlan(p *schedulepb.Plan) *Plan {
	plan := &Plan{
		ID:          p.Id,
		Name:        p.Name,
		Type:        p.Type,
		Namespace:   p.Namespace,
		Priority:    p.Priority,
		Stop:        p.Stop,
		Status:      p.Status,
		Description: p.Description,
		Synchronous: p.Synchronous,                   // config and detail
		SubmitTime:  time.NewFormatTime(xtime.Now()), // other attr
	}
	if p.CreateTime != nil {
		plan.CreateTime = time.NewFormatTime(xtime.Unix(p.CreateTime.Seconds, int64(p.CreateTime.Nanos)))
	}
	if p.UpdateTime != nil {
		plan.UpdateTime = time.NewFormatTime(xtime.Unix(p.UpdateTime.Seconds, int64(p.UpdateTime.Nanos)))
	}
	if p.Parameters != "" {
		plan.Parameters = []byte(p.Parameters)
	}
	if p.Periodic != nil {
		periodic := &PeriodicConfig{
			Enabled:         p.Periodic.Enabled,
			Spec:            p.Periodic.Spec,
			TimeZone:        p.Periodic.TimeZone,
			ProhibitOverlap: p.Periodic.ProhibitOverlap,
		}
		if p.Periodic.StartTime != nil {
			periodic.StartTime = time.NewFormatTime(xtime.Unix(p.Periodic.StartTime.Seconds, int64(p.Periodic.StartTime.Nanos)))
		}
		if p.Periodic.EndTime != nil {
			periodic.EndTime = time.NewFormatTime(xtime.Unix(p.Periodic.EndTime.Seconds, int64(p.Periodic.EndTime.Nanos)))
		}
		plan.Periodic = periodic
	}
	return plan
}

func ConvertPlanFromRequest(p *pb.SchedulePlanAddRequest) *Plan {
	plan := &Plan{
		ID:          p.Id,
		Name:        p.Name,
		Type:        p.Type,
		Namespace:   p.Namespace,
		Priority:    p.Priority,
		Stop:        p.Stop,
		Status:      p.Status,
		Description: p.Description,
		Synchronous: p.Synchronous,                   // config and detail
		SubmitTime:  time.NewFormatTime(xtime.Now()), // other attr
		CreateTime:  time.NewFormatTime(xtime.Now()),
		UpdateTime:  time.NewFormatTime(xtime.Now()),
	}
	if p.Parameters != "" {
		plan.Parameters = []byte(p.Parameters)
	}
	if p.Periodic != nil {
		periodic := &PeriodicConfig{
			Enabled:         p.Periodic.Enabled,
			Spec:            p.Periodic.Spec,
			TimeZone:        p.Periodic.TimeZone,
			ProhibitOverlap: p.Periodic.ProhibitOverlap,
		}
		if p.Periodic.StartTime != nil {
			periodic.StartTime = time.NewFormatTime(xtime.Unix(p.Periodic.StartTime.Seconds, int64(p.Periodic.StartTime.Nanos)))
		}
		if p.Periodic.EndTime != nil {
			periodic.EndTime = time.NewFormatTime(xtime.Unix(p.Periodic.EndTime.Seconds, int64(p.Periodic.EndTime.Nanos)))
		}
		plan.Periodic = periodic
	}
	return plan
}
