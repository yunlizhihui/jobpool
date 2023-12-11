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

import "yunli.com/jobpool/api/v2/domain"

type EvaluateDispatcher interface {
	EvaluateNodePlan(planAllocation *domain.PlanAlloc, nodeID string) (bool, string, error)
}

// EvaluatePool 评估池供评估者工作
type EvaluatePool struct {
	workers    int
	workerStop []chan struct{}
	req        chan EvaluateRequest
	res        chan evaluateResult
	dispatcher EvaluateDispatcher
}

type EvaluateRequest struct {
	Plan   *domain.PlanAlloc
	NodeID string
}

type evaluateResult struct {
	NodeID string
	Fit    bool
	Reason string
	Err    error
}

// NewEvaluatePool returns a pool of the given size.
func NewEvaluatePool(workers, bufSize int, dispatcher EvaluateDispatcher) *EvaluatePool {
	p := &EvaluatePool{
		workers:    workers,
		workerStop: make([]chan struct{}, workers),
		req:        make(chan EvaluateRequest, bufSize),
		res:        make(chan evaluateResult, bufSize),
		dispatcher: dispatcher,
	}
	for i := 0; i < workers; i++ {
		stopCh := make(chan struct{})
		p.workerStop[i] = stopCh
		go p.run(stopCh)
	}
	return p
}

// Size returns the current size
func (p *EvaluatePool) Size() int {
	return p.workers
}

// SetSize is used to resize the worker pool
func (p *EvaluatePool) SetSize(size int) {
	// Protect against a negative size
	if size < 0 {
		size = 0
	}

	// Handle an upwards resize
	if size >= p.workers {
		for i := p.workers; i < size; i++ {
			stopCh := make(chan struct{})
			p.workerStop = append(p.workerStop, stopCh)
			go p.run(stopCh)
		}
		p.workers = size
		return
	}

	// Handle a downwards resize
	for i := p.workers; i > size; i-- {
		close(p.workerStop[i-1])
		p.workerStop[i-1] = nil
	}
	p.workerStop = p.workerStop[:size]
	p.workers = size
}

// RequestCh is used to push requests
func (p *EvaluatePool) RequestCh() chan<- EvaluateRequest {
	return p.req
}

// ResultCh is used to read the results as they are ready
func (p *EvaluatePool) ResultCh() <-chan evaluateResult {
	return p.res
}

// Shutdown is used to shutdown the pool
func (p *EvaluatePool) Shutdown() {
	p.SetSize(0)
}

// run is a long running go routine per worker
func (p *EvaluatePool) run(stopCh chan struct{}) {
	for {
		select {
		case req := <-p.req:
			fit, reason, err := p.dispatcher.EvaluateNodePlan(req.Plan, req.NodeID)
			p.res <- evaluateResult{req.NodeID, fit, reason, err}
		case <-stopCh:
			return
		}
	}
}
