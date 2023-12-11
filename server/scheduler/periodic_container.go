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
	"container/heap"
	"fmt"
	"time"
	"yunli.com/jobpool/api/v2/domain"
)

type periodicPlan struct {
	plan  *domain.Plan
	next  time.Time
	index int
}

type periodicHeapImp []*periodicPlan

// periodicHeap 提供出/入操作的队列
type periodicHeap struct {
	index map[domain.NamespacedID]*periodicPlan
	heap  periodicHeapImp
}

func NewPeriodicHeap() *periodicHeap {
	return &periodicHeap{
		index: make(map[domain.NamespacedID]*periodicPlan),
		heap:  make(periodicHeapImp, 0),
	}
}

func (h periodicHeapImp) Len() int { return len(h) }

func (h periodicHeapImp) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	// Sort such that zero times are at the end of the list.
	iZero, jZero := h[i].next.IsZero(), h[j].next.IsZero()
	if iZero && jZero {
		return false
	} else if iZero {
		return false
	} else if jZero {
		return true
	}

	return h[i].next.Before(h[j].next)
}

func (h periodicHeapImp) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *periodicHeapImp) Push(x interface{}) {
	n := len(*h)
	plan := x.(*periodicPlan)
	plan.index = n
	*h = append(*h, plan)
}

func (h *periodicHeapImp) Pop() interface{} {
	old := *h
	n := len(old)
	plan := old[n-1]
	plan.index = -1 // for safety
	*h = old[0 : n-1]
	return plan
}

func (p *periodicHeap) Length() int {
	return len(p.heap)
}

func (p *periodicHeap) Push(plan *domain.Plan, next time.Time) error {
	tuple := domain.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	if _, ok := p.index[tuple]; ok {
		return fmt.Errorf("Plan %q (%s) already exists", plan.ID, plan.Namespace)
	}

	pPlan := &periodicPlan{plan, next, 0}
	p.index[tuple] = pPlan
	heap.Push(&p.heap, pPlan)
	return nil
}

func (p *periodicHeap) Pop() *periodicPlan {
	if len(p.heap) == 0 {
		return nil
	}

	pPlan := heap.Pop(&p.heap).(*periodicPlan)
	tuple := domain.NamespacedID{
		ID:        pPlan.plan.ID,
		Namespace: pPlan.plan.Namespace,
	}
	delete(p.index, tuple)
	return pPlan
}

func (p *periodicHeap) Peek() *periodicPlan {
	if len(p.heap) == 0 {
		return nil
	}

	return p.heap[0]
}

func (p *periodicHeap) Contains(plan *domain.Plan) bool {
	tuple := domain.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	_, ok := p.index[tuple]
	return ok
}

func (p *periodicHeap) Update(plan *domain.Plan, next time.Time) error {
	tuple := domain.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	if pPlan, ok := p.index[tuple]; ok {
		// Need to update the Plan as well because its spec can change.
		pPlan.plan = plan
		pPlan.next = next
		heap.Fix(&p.heap, pPlan.index)
		return nil
	}
	return fmt.Errorf("heap doesn't contain Plan %q (%s)", plan.ID, plan.Namespace)
}

func (p *periodicHeap) Remove(plan *domain.Plan) error {
	tuple := domain.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	if pPlan, ok := p.index[tuple]; ok {
		heap.Remove(&p.heap, pPlan.index)
		delete(p.index, tuple)
		return nil
	}

	return fmt.Errorf("heap doesn't contain Plan %q (%s)", plan.ID, plan.Namespace)
}
