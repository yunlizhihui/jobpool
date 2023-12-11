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

package dispatcher

import (
	"go.uber.org/zap"
	"sync"
	"time"
)

type heartbeatStop struct {
	lastOk        time.Time
	startupGrace  time.Time
	allocInterval map[string]time.Duration
	logger        *zap.Logger
	shutdownCh    chan struct{}
	lock          *sync.RWMutex
}

func newHeartbeatStop(
	timeout time.Duration,
	logger *zap.Logger,
	shutdownCh chan struct{}) *heartbeatStop {

	h := &heartbeatStop{
		startupGrace:  time.Now().Add(timeout),
		allocInterval: make(map[string]time.Duration),
		logger:        logger,
		shutdownCh:    shutdownCh,
		lock:          &sync.RWMutex{},
	}

	return h
}

func (h *heartbeatStop) watch() {
	h.lastOk = time.Now()
	stop := make(chan string, 1)
	var now time.Time
	var interval time.Duration
	checkAllocs := false

	for {
		// minimize the interval
		interval = 5 * time.Second
		for _, t := range h.allocInterval {
			if t < interval {
				interval = t
			}
		}

		checkAllocs = false
		timeout := time.After(interval)

		select {
		case allocID := <-stop:
			// TODO
			delete(h.allocInterval, allocID)

		case <-timeout:
			checkAllocs = true

		case <-h.shutdownCh:
			return
		}

		if !checkAllocs {
			continue
		}

		now = time.Now()
		for allocID, d := range h.allocInterval {
			if h.shouldStopAfter(now, d) {
				stop <- allocID
			}
		}
	}
}

func (h *heartbeatStop) shouldStopAfter(now time.Time, interval time.Duration) bool {
	lastOk := h.getLastOk()
	if lastOk.IsZero() {
		return now.After(h.startupGrace)
	}
	return now.After(lastOk.Add(interval))
}

func (h *heartbeatStop) setLastOk(t time.Time) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.lastOk = t
}
func (h *heartbeatStop) getLastOk() time.Time {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.lastOk
}
