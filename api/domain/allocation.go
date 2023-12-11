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
	timestamppb "github.com/gogo/protobuf/types"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
)

// Allocation 任务的分配信息
type Allocation struct {
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	Namespace             string `json:"namespace"`
	EvalID                string `json:"evalId"`
	NodeID                string `json:"nodeId"`
	ClientDescription     string `json:"clientDescription"`
	PlanID                string `json:"planId"`
	Plan                  *Plan  `json:"-"`
	JobId                 string `json:"jobId"`
	ClientStatus          string `json:"clientStatus"`
	NextAllocation        string
	DesiredStatus         string // Desired Status of the allocation on the client
	DesiredDescription    string
	DesiredTransition     DesiredTransition
	FollowupEvalID        string
	PreviousAllocation    string // PreviousAllocation is the allocation that this allocation is replacing
	AllocModifyIndex      uint64
	PreemptedByAllocation string // PreemptedByAllocation tracks the alloc ID of the allocation that caused this allocation
	// CreateTime is the time the allocation has finished scheduling and been
	// verified by the plan applier.
	CreateTime xtime.FormatTime `json:"createTime"`

	// UpdateTime is the time the allocation was last updated.
	ModifyTime xtime.FormatTime `json:"updateTime"`
}

// Terminated returns if the allocation is in a terminal state on a client.
func (a *Allocation) Terminated() bool {
	if a.ClientStatus == constant.AllocClientStatusFailed ||
		a.ClientStatus == constant.AllocClientStatusComplete ||
		a.ClientStatus == constant.AllocClientStatusSkipped ||
		a.ClientStatus == constant.AllocClientStatusExpired ||
		a.ClientStatus == constant.AllocClientStatusCancelled {
		return true
	}
	return false
}

// TerminalStatus returns if the desired or actual status is terminal and
// will no longer transition.
func (a *Allocation) TerminalStatus() bool {
	// First check the desired state and if that isn't terminal, check client
	// state.
	return a.ServerTerminalStatus() || a.ClientTerminalStatus()
}

func (a *Allocation) MigrateStatus() bool {
	// 检查是否需要迁移这个alloc
	switch a.ClientStatus {
	case constant.AllocClientStatusPending:
		return true
	case constant.AllocClientStatusRunning:
		if a.Plan != nil && a.Plan.Synchronous {
			return true
		} else {
			return false
		}
	default:
		return false
	}
}

// ServerTerminalStatus returns true if the desired state of the allocation is terminal
func (a *Allocation) ServerTerminalStatus() bool {
	switch a.DesiredStatus {
	case constant.AllocDesiredStatusStop, constant.AllocDesiredStatusEvict:
		return true
	default:
		return false
	}
}

// ClientTerminalStatus returns if the client status is terminal and will no longer transition
func (a *Allocation) ClientTerminalStatus() bool {
	switch a.ClientStatus {
	case constant.AllocClientStatusComplete, constant.AllocClientStatusFailed, constant.AllocClientStatusCancelled, constant.AllocClientStatusExpired, constant.AllocClientStatusSkipped:
		return true
	default:
		return false
	}
}

// Copy provides a copy of the allocation and deep copies the plan
func (a *Allocation) Copy() *Allocation {
	return a.copyImpl(true)
}

func (a *Allocation) copyImpl(plan bool) *Allocation {
	if a == nil {
		return nil
	}
	na := new(Allocation)
	*na = *a

	if plan {
		na.Plan = na.Plan.Copy()
	}
	return na
}

// AllocationDiff 状态不一致的分配信息
type AllocationDiff Allocation

func (a *Allocation) AllocationDiff() *AllocationDiff {
	return (*AllocationDiff)(a)
}

// Canonicalize Allocation 校验
func (a *Allocation) Canonicalize() {
	a.Plan.Canonicalize()
}

func AllocSubset(allocs []*Allocation, subset []*Allocation) bool {
	if len(subset) == 0 {
		return true
	}
	// Convert allocs into a map
	allocMap := make(map[string]struct{})
	for _, alloc := range allocs {
		allocMap[alloc.ID] = struct{}{}
	}

	for _, alloc := range subset {
		if _, ok := allocMap[alloc.ID]; !ok {
			return false
		}
	}
	return true
}

func RemoveAllocs(allocs []*Allocation, remove []*Allocation) []*Allocation {
	if len(remove) == 0 {
		return allocs
	}
	removeSet := make(map[string]struct{})
	for _, remove := range remove {
		removeSet[remove.ID] = struct{}{}
	}

	r := make([]*Allocation, 0, len(allocs))
	for _, alloc := range allocs {
		if _, ok := removeSet[alloc.ID]; !ok {
			r = append(r, alloc)
		}
	}
	return r
}

func (a *Allocation) GetNamespace() string {
	if a == nil {
		return ""
	}
	return a.Namespace
}

func (a *Allocation) PlanNamespacedID() NamespacedID {
	return NewNamespacedID(a.PlanID, a.Namespace)
}

// 迁移标记
type DesiredTransition struct {
	// Migrate is used to indicate that this allocation should be stopped and
	// migrated to another node.
	Migrate *bool

	// Reschedule 重新调度
	Reschedule *bool

	ForceReschedule *bool
}

// ShouldMigrate returns whether the transition object dictates a migration.
func (d DesiredTransition) ShouldMigrate() bool {
	return d.Migrate != nil && *d.Migrate
}

func (d *DesiredTransition) Merge(o *DesiredTransition) {
	if o.Migrate != nil {
		d.Migrate = o.Migrate
	}

	if o.Reschedule != nil {
		d.Reschedule = o.Reschedule
	}

	if o.ForceReschedule != nil {
		d.ForceReschedule = o.ForceReschedule
	}
}

func ConvertAllocationToPb(alloc *Allocation) *schedulepb.Allocation {
	j := &schedulepb.Allocation{
		Id:                 alloc.ID,
		Name:               alloc.Name,
		Namespace:          alloc.Namespace,
		EvalId:             alloc.EvalID,
		NodeId:             alloc.NodeID,
		ClientDescription:  alloc.ClientDescription,
		PlanId:             alloc.PlanID,
		JobId:              alloc.JobId,
		ClientStatus:       alloc.ClientStatus,
		NextAllocation:     alloc.NextAllocation,
		DesiredStatus:      alloc.DesiredStatus,
		DesiredDescription: alloc.DesiredDescription,
	}
	if alloc.CreateTime != "" {
		create, _ := timestamppb.TimestampProto(alloc.CreateTime.TimeValue().UTC())
		j.CreateTime = create
	}
	if alloc.ModifyTime != "" {
		update, _ := timestamppb.TimestampProto(alloc.ModifyTime.TimeValue().UTC())
		j.UpdateTime = update
	}
	return j
}
func ConvertAllocation(alloc *schedulepb.Allocation) *Allocation {
	j := &Allocation{
		ID:                 alloc.Id,
		Name:               alloc.Name,
		Namespace:          alloc.Namespace,
		EvalID:             alloc.EvalId,
		NodeID:             alloc.NodeId,
		ClientDescription:  alloc.ClientDescription,
		PlanID:             alloc.PlanId,
		JobId:              alloc.JobId,
		ClientStatus:       alloc.ClientStatus,
		NextAllocation:     alloc.NextAllocation,
		DesiredStatus:      alloc.DesiredStatus,
		DesiredDescription: alloc.DesiredDescription,
	}
	if alloc.CreateTime != nil {
		j.CreateTime = xtime.NewFormatTime(time.Unix(alloc.CreateTime.Seconds, int64(alloc.CreateTime.Nanos)))
	}
	if alloc.UpdateTime != nil {
		j.ModifyTime = xtime.NewFormatTime(time.Unix(alloc.UpdateTime.Seconds, int64(alloc.UpdateTime.Nanos)))
	}
	return j
}

func AllocsFit(node *Node, allocs []*Allocation, checkDevices bool) (bool, string, error) {

	// Allocations fit!
	return true, "", nil
}
