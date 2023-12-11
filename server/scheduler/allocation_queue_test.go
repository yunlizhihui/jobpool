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
	"testing"
	"time"
	"yunli.com/jobpool/api/v2/domain"
)

func initAllocationQueue(t *testing.T) *AllocationQueue {
	pq, err := NewAllocationQueue()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	return pq
}

func PlanAlloc() *domain.PlanAlloc {
	return &domain.PlanAlloc{
		Priority: 50,
	}
}

func PlanResult() *domain.PlanResult {
	return &domain.PlanResult{}
}

func TestAllocationQueue_Enqueue_Dequeue(t *testing.T) {
	Parallel(t)
	pq := initAllocationQueue(t)
	if pq.Enabled() {
		t.Fatalf("should not be enabled")
	}
	pq.SetEnabled(true)
	if !pq.Enabled() {
		t.Fatalf("should be enabled")
	}

	plan := PlanAlloc()
	future, err := pq.Enqueue(plan)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stats := pq.Stats()
	if stats.Depth != 1 {
		t.Fatalf("bad: %#v", stats)
	}

	resCh := make(chan *domain.PlanResult, 1)
	errCh := make(chan error)
	go func() {
		defer close(errCh)
		defer close(resCh)

		res, err := future.Wait()
		if err != nil {
			errCh <- err
			return
		}
		resCh <- res
	}()

	pending, err := pq.Dequeue(time.Second)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	stats = pq.Stats()
	if stats.Depth != 0 {
		t.Fatalf("bad: %#v", stats)
	}

	if pending == nil || pending.PlanAllocation != plan {
		t.Fatalf("bad: %#v", pending)
	}

	result := PlanResult()
	pending.Respond(result, nil)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("error in anonymous goroutine: %s", err)
		}
	case r := <-resCh:
		if r != result {
			t.Fatalf("Bad: %#v", r)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}
}
