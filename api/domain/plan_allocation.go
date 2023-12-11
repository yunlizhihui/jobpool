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
	"yunli.com/jobpool/api/v2/constant"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/schedulepb"
)

type PlanAlloc struct {
	EvalID         string // EvalID is the evaluation ID this plan is associated with
	EvalToken      string // 重新选举可能引发不同leader，增加校验
	Priority       int
	AllAtOnce      bool // 是否为一次性任务
	Plan           *Plan
	JobId          string
	NodeUpdate     map[string][]*Allocation
	NodeAllocation map[string][]*Allocation
	// Annotations contains annotations by the scheduler to be used by operators
	// to understand the decisions made by the scheduler.
	Annotations     *PlanAnnotations         // 说明
	NodePreemptions map[string][]*Allocation // 节点被取代
}

// PlanAnnotations holds annotations made by the scheduler to give further debug
// information to operators.
type PlanAnnotations struct {
	PreemptedAllocs []*AllocListStub // PreemptedAllocs is the set of allocations to be preempted to make the placement successful.
}

// AllocListStub is used to return a subset of alloc information
type AllocListStub struct {
	ID                    string
	EvalID                string
	Name                  string
	Namespace             string
	NodeID                string
	NodeName              string
	JobID                 string
	JobType               string
	JobVersion            uint64
	TaskGroup             string
	DesiredStatus         string
	DesiredDescription    string
	ClientStatus          string
	ClientDescription     string
	FollowupEvalID        string
	PreemptedAllocations  []string
	PreemptedByAllocation string
	CreateTime            int64
	ModifyTime            int64
}

// PlanResult 任务分配结果
type PlanResult struct {
	// NodeUpdate contains all the updates that were committed.
	NodeUpdate map[string][]*Allocation

	// NodeAllocation contains all the allocations that were committed.
	NodeAllocation map[string][]*Allocation

	// NodePreemptions is a map from node id to a set of allocations from other
	// lower priority jobs that are preempted. Preempted allocations are marked
	// as stopped.
	NodePreemptions map[string][]*Allocation
}

// IsNoOp checks if this plan result would do nothing
func (p *PlanResult) IsNoOp() bool {
	return len(p.NodeUpdate) == 0 && len(p.NodeAllocation) == 0
}

func (p *PlanResult) FullCommit(planAlloc *PlanAlloc) (bool, int, int) {
	expected := 0
	actual := 0
	for name, allocList := range planAlloc.NodeAllocation {
		didAlloc := p.NodeAllocation[name]
		expected += len(allocList)
		actual += len(didAlloc)
	}
	return actual == expected, expected, actual
}

// NormalizeAllocations normalizes allocations to remove fields that can
// be fetched from the MemDB instead of sending over the wire
func (p *PlanAlloc) NormalizeAllocations() {
	for _, allocs := range p.NodeUpdate {
		for i, alloc := range allocs {
			allocs[i] = &Allocation{
				ID:                 alloc.ID,
				DesiredDescription: alloc.DesiredDescription,
				ClientStatus:       alloc.ClientStatus,
				FollowupEvalID:     alloc.FollowupEvalID,
			}
		}
	}

	for _, allocs := range p.NodePreemptions {
		for i, alloc := range allocs {
			allocs[i] = &Allocation{
				ID:                    alloc.ID,
				PreemptedByAllocation: alloc.PreemptedByAllocation,
			}
		}
	}
}

// AppendStoppedAlloc marks an allocation to be stopped. The clientStatus of the
// allocation may be optionally set by passing in a non-empty value.
func (p *PlanAlloc) AppendStoppedAlloc(alloc *Allocation, desiredDesc, clientStatus, followupEvalID string) {
	newAlloc := new(Allocation)
	*newAlloc = *alloc
	if p.Plan == nil && newAlloc.Plan != nil {
		p.Plan = newAlloc.Plan
	}
	newAlloc.Plan = nil
	newAlloc.DesiredStatus = constant.AllocDesiredStatusStop
	newAlloc.DesiredDescription = desiredDesc
	if clientStatus != "" {
		newAlloc.ClientStatus = clientStatus
	}
	if followupEvalID != "" {
		newAlloc.FollowupEvalID = followupEvalID
	}
	node := alloc.NodeID
	existing := p.NodeUpdate[node]
	p.NodeUpdate[node] = append(existing, newAlloc)
}

func (p *PlanAlloc) AppendAlloc(alloc *Allocation, plan *Plan) {
	node := alloc.NodeID
	existing := p.NodeAllocation[node]
	alloc.Plan = plan
	p.NodeAllocation[node] = append(existing, alloc)
}

// IsNoOp checks if this plan would do nothing
func (p *PlanAlloc) IsNoOp() bool {
	return len(p.NodeUpdate) == 0 &&
		len(p.NodeAllocation) == 0
}

func ConvertPlanAllocation(r *pb.PlanAllocationEnqueueRequest) *PlanAlloc {
	p := &PlanAlloc{
		EvalID:    r.EvalId,
		EvalToken: r.EvalToken,
		Priority:  int(r.Priority),
		AllAtOnce: r.AllAtOnce,
		JobId:     r.JobId,
	}
	if r.Allocations != nil && len(r.Allocations) > 0 {
		allocs := make(map[string][]*Allocation)
		for _, item := range r.Allocations {
			nodeId := item.NodeId
			existing := allocs[nodeId]
			allocs[nodeId] = append(existing, ConvertAllocation(item))
		}
		p.NodeAllocation = allocs
	}
	return p
}

func ConvertPlanAllocationToPb(m map[string][]*Allocation) []*schedulepb.Allocation {
	var alloc []*schedulepb.Allocation
	for key := range m {
		for _, item := range m[key] {
			alloc = append(alloc, ConvertAllocationToPb(item))
		}
	}
	return alloc
}
