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
	"sync"
	"time"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/helper/funcs"
)

var (
	allocationQueueFlushed = fmt.Errorf("PlanAllocation queue flushed")
)

type AllocationFuture interface {
	Wait() (*domain.PlanResult, error)
}

// AllocationQueue is used to submit commit plans for task allocations
// to the current leader. The leader verifies that resources are not
// over-committed and commits to Raft. This allows sub-schedulers to
// be optimistically concurrent. In the case of an overcommit, the PlanAllocation
// may be partially applied if allowed, or completely rejected (gang commit).
type AllocationQueue struct {
	enabled bool
	stats   *QueueStats
	ready   PendingAllocations
	waitCh  chan struct{}
	l       sync.RWMutex
}

// NewAllocationQueue is used to construct and return a new PlanAllocation queue
func NewAllocationQueue() (*AllocationQueue, error) {
	q := &AllocationQueue{
		enabled: false,
		stats:   new(QueueStats),
		ready:   make([]*PendingAllocation, 0, 16),
		waitCh:  make(chan struct{}, 1),
	}
	return q, nil
}

// PendingAllocation 用于队列中存储的内容
type PendingAllocation struct {
	PlanAllocation *domain.PlanAlloc
	enqueueTime    time.Time
	result         *domain.PlanResult
	errCh          chan error
}

// Wait 用于等待返回结果
func (p *PendingAllocation) Wait() (*domain.PlanResult, error) {
	err := <-p.errCh
	return p.result, err
}

// Respond 用于返回结果信息
func (p *PendingAllocation) Respond(result *domain.PlanResult, err error) {
	p.result = result
	p.errCh <- err
}

// PendingAllocations 优先级队列
type PendingAllocations []*PendingAllocation

// Enabled is used to check if the queue is enabled.
func (q *AllocationQueue) Enabled() bool {
	q.l.RLock()
	defer q.l.RUnlock()
	return q.enabled
}

// SetEnabled 设置是否可用，本方法用于leader节点
func (q *AllocationQueue) SetEnabled(enabled bool) {
	q.l.Lock()
	q.enabled = enabled
	q.l.Unlock()
	if !enabled {
		q.Flush()
	}
}

// Enqueue 入队列
func (q *AllocationQueue) Enqueue(plan *domain.PlanAlloc) (AllocationFuture, error) {
	q.l.Lock()
	defer q.l.Unlock()

	// Do nothing if not enabled
	if !q.enabled {
		return nil, fmt.Errorf("PlanAllocation queue is disabled")
	}

	// Wrap the pending PlanAllocation
	pending := &PendingAllocation{
		PlanAllocation: plan,
		enqueueTime:    time.Now(),
		errCh:          make(chan error, 1),
	}

	// Push onto the heap
	heap.Push(&q.ready, pending)

	// Update the stats
	q.stats.Depth += 1

	// Unblock any blocked reader
	select {
	case q.waitCh <- struct{}{}:
	default:
	}
	return pending, nil
}

// Dequeue 出队列
func (q *AllocationQueue) Dequeue(timeout time.Duration) (*PendingAllocation, error) {
SCAN:
	q.l.Lock()

	// Do nothing if not enabled
	if !q.enabled {
		q.l.Unlock()
		return nil, fmt.Errorf("PlanAllocation queue is disabled")
	}

	// Look for available work
	if len(q.ready) > 0 {
		raw := heap.Pop(&q.ready)
		pending := raw.(*PendingAllocation)
		q.stats.Depth -= 1
		q.l.Unlock()
		return pending, nil
	}
	q.l.Unlock()

	// Setup the timeout timer
	var timerCh <-chan time.Time
	if timerCh == nil && timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timerCh = timer.C
	}

	// Wait for timeout or new work
	select {
	case <-q.waitCh:
		goto SCAN
	case <-timerCh:
		return nil, nil
	}
}

// Flush is used to reset the state of the PlanAllocation queue
func (q *AllocationQueue) Flush() {
	q.l.Lock()
	defer q.l.Unlock()

	// Error out all the futures
	for _, pending := range q.ready {
		pending.Respond(nil, allocationQueueFlushed)
	}

	// Reset the broker
	q.stats.Depth = 0
	q.ready = make([]*PendingAllocation, 0, 16)

	// Unblock any waiters
	select {
	case q.waitCh <- struct{}{}:
	default:
	}
}

// Stats is used to query the state of the queue
func (q *AllocationQueue) Stats() *QueueStats {
	// Allocate a new stats struct
	stats := new(QueueStats)

	q.l.RLock()
	defer q.l.RUnlock()

	// Copy all the stats
	*stats = *q.stats
	return stats
}

// EmitStats 用于周期上报队列状态（暂时废弃）
func (q *AllocationQueue) EmitStats(period time.Duration, stopCh <-chan struct{}) {
	timer, stop := funcs.NewSafeTimer(period)
	defer stop()

	for {
		select {
		case <-timer.C:
			// stats := q.Stats()
			// metrics.SetGauge([]string{constant.JobPoolName, "PlanAllocation", "queue_depth"}, float32(stats.Depth))
		case <-stopCh:
			return
		}
	}
}

// QueueStats 统计深度信息
type QueueStats struct {
	Depth int
}

// Len 用于获取长度，后续均为队列操作
func (p PendingAllocations) Len() int {
	return len(p)
}

func (p PendingAllocations) Less(i, j int) bool {
	if p[i].PlanAllocation.Priority != p[j].PlanAllocation.Priority {
		return !(p[i].PlanAllocation.Priority < p[j].PlanAllocation.Priority)
	}
	return p[i].enqueueTime.Before(p[j].enqueueTime)
}

func (p PendingAllocations) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *PendingAllocations) Push(e interface{}) {
	*p = append(*p, e.(*PendingAllocation))
}

func (p *PendingAllocations) Pop() interface{} {
	n := len(*p)
	e := (*p)[n-1]
	(*p)[n-1] = nil
	*p = (*p)[:n-1]
	return e
}

func (p PendingAllocations) Peek() *PendingAllocation {
	n := len(p)
	if n == 0 {
		return nil
	}
	return p[n-1]
}
