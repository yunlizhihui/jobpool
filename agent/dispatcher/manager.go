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
	"time"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
)

const (
	allocSyncIntv      = 200 * time.Millisecond
	allocSyncRetryIntv = 5 * time.Second
)

/**
 * 同步更新后的allocation
 */
func (d *Dispatcher) allocSync() {
	syncTicker := time.NewTicker(allocSyncIntv)
	updates := make(map[string]*domain.Allocation)
	for {
		select {
		case <-d.shutdownCh:
			syncTicker.Stop()
			return
		case alloc := <-d.allocUpdates:
			// Batch the allocation updates until the timer triggers.
			updates[alloc.ID] = alloc
		case <-syncTicker.C:
			// Fast path if there are no updates
			if len(updates) == 0 {
				continue
			}

			sync := make([]*domain.Allocation, 0, len(updates))
			for _, alloc := range updates {
				sync = append(sync, alloc)
			}

			// Send to server.
			d.logger.Debug("-----start run update alloc stauts ----", zap.Int("alloc", len(sync)))
			for _, item := range sync {
				d.logger.Info("the state is :", zap.String("allocId", item.ID), zap.String("state", item.ClientStatus))
				// TODO 优化成一次请求
				args := etcdserverpb.ScheduleAllocationStatusUpdateRequest{
					Id:          item.ID,
					Status:      item.ClientStatus,
					Description: item.ClientDescription,
				}
				resp, err := d.client.AllocationUpdate(d.ctx, &args)
				if err != nil {
					d.logger.Error("error updating allocations", zap.Error(err))
					syncTicker.Stop()
					syncTicker = time.NewTicker(d.retryIntv(allocSyncRetryIntv))
					continue
				}
				d.logger.Debug("the result", zap.Reflect("resp", resp))
			}

			// 更新后重置map和ticker
			updates = make(map[string]*domain.Allocation, len(updates))
			syncTicker.Stop()
			syncTicker = time.NewTicker(allocSyncIntv)
		}
	}
}
