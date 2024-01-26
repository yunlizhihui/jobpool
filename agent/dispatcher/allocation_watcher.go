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
	"errors"
	"fmt"
	"go.uber.org/zap"
	"time"
	"yunli.com/jobpool/agent/v2/helper/util"
	"yunli.com/jobpool/agent/v2/runner"
	"yunli.com/jobpool/agent/v2/service"
	"yunli.com/jobpool/agent/v2/structs"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/schedulepb"
)

const (
	getAllocRetryIntv  = 30 * time.Second
	listAllocSleepIntv = 200 * time.Millisecond
)

var (
	// 无服务器节点
	noServersErr = errors.New("no servers")
)

// watchAllocations 监听是否有本节点的allocation尚未进行处理
func (d *Dispatcher) watchAllocations(updates chan *structs.AllocUpdates) {
	// 在node注册成功之前，不要处理任务
	flagWatch := false
	for {
		select {
		case <-d.shutdownCh:
			return
		case <-d.registeredCh:
			// begin watch
			flagWatch = true
		default:
			time.Sleep(500 * time.Millisecond)
		}
		if flagWatch {
			break
		}
	}
	// node注册成功后，开始进入监听状态，并处理对应的任务分配
	req := &etcdserverpb.ScheduleAllocationListRequest{
		NodeId: d.NodeID(),
		Status: constant.AllocClientStatusPending,
	}

OUTER:
	for {
		// var resp etcdserverpb.ScheduleAllocationListResponse
		// 请求频繁接口轻量化
		resp, err := d.client.SimpleAllocationList(d.ctx, req)
		if err != nil {
			select {
			case <-d.shutdownCh:
				return
			default:
			}
			if err != noServersErr {
				d.logger.Error("error querying node simple allocations", zap.Error(err))
			}
			retry := d.retryIntv(getAllocRetryIntv)
			select {
			case <-d.rpcRetryWatcher():
				continue
			case <-time.After(retry):
				continue
			case <-d.shutdownCh:
				return
			}
		}
		// Check for shutdown
		select {
		case <-d.shutdownCh:
			return
		default:
		}
		filtered := make(map[string]struct{})
		var pull []string
		var pullId map[string]struct{}

		var pulledAllocs map[string]*domain.Allocation
		if resp.Data != nil && len(resp.Data) > 0 {
			// 优先看filtered
			for _, allocation := range resp.Data {
				allocID := allocation.Id
				d.allocLock.RLock()
				// 已经加入到allocs中的，后续即使查到也不会再加入
				_, ok := d.allocs[allocID]
				d.allocLock.RUnlock()
				d.invalidAllocsLock.Lock()
				_, isInvalid := d.invalidAllocs[allocID]
				d.invalidAllocsLock.Unlock()
				if !ok && !isInvalid {
					// Only pull allocs that are required. Filtered
					// allocs might be at a higher index, so ignore
					// it.
					pull = append(pull, allocID)
				} else {
					filtered[allocID] = struct{}{}
				}
			}
			pullId = make(map[string]struct{}, len(pull))
			for _, id := range pull {
				pullId[id] = struct{}{}
			}
			// 只加入在pull中的
			pulledAllocs = make(map[string]*domain.Allocation, len(pull))
			// 通过Ids查询具体的alloc
			req.Ids = []string{}
			for _, id := range pull {
				req.Ids = append(req.Ids, id)
			}
			//此处不需要校验请求错误，因为请求失败allocation找不到会在跳入到OUTER中处理
			tmp, _ := d.client.AllocationList(d.ctx, req)
			req.Ids = []string{}

			var allocations []*schedulepb.Allocation
			if tmp != nil && tmp.Data != nil {
				allocations = tmp.Data
			}

			for _, allocation := range allocations {
				allocId := allocation.Id
				if _, ok := pullId[allocId]; !ok {
					// d.logger.Warn("这任务我跑过了", zap.String("allocation-id", allocId))
					continue
				}
				pulledAllocs[allocation.Id] = domain.ConvertAllocation(allocation)
			}
			for _, desiredID := range pull {
				if _, ok := pulledAllocs[desiredID]; !ok {
					// 拿到ID了，但是没有拿到内容的话，需要重新查
					// We didn't get everything we wanted. Do not update the
					// MinQueryIndex, sleep and then retry.
					wait := d.retryIntv(5 * time.Second)
					select {
					case <-time.After(wait):
						// Wait for the server we contact to receive the
						// allocations
						continue OUTER
					case <-d.shutdownCh:
						return
					}
				}
			}

			// Check for shutdown
			select {
			case <-d.shutdownCh:
				return
			default:
			}

		}
		if len(pulledAllocs) > 0 {
			update := &structs.AllocUpdates{
				Filtered: filtered,
				Pulled:   pulledAllocs,
			}
			select {
			case updates <- update:
			case <-d.shutdownCh:
				return
			}
		}
		time.Sleep(listAllocSleepIntv)
	}
}

func (d *Dispatcher) runAllocations(update *structs.AllocUpdates) {
	// 通过runner运行
	d.allocLock.RLock()
	existing := make(map[string]uint64, len(d.allocs))
	for id, ar := range d.allocs {
		existing[id] = ar.Alloc().AllocModifyIndex
	}
	d.allocLock.RUnlock()
	diff := util.DiffAllocs(existing, update)
	d.logger.Debug("allocation updates",
		zap.Int("added", len(diff.Added)),
		zap.Int("removed", len(diff.Removed)),
		zap.Int("updated", len(diff.Updated)),
		zap.Int("ignored", len(diff.Ignore)))
	for _, update := range diff.Updated {
		d.updateAlloc(update)
	}
	var allocAddList []*domain.Allocation
	for _, pulled := range update.Pulled {
		allocAddList = append(allocAddList, pulled)
	}
	errs := 0
	// Start the new allocations
	flag := false
	for _, add := range allocAddList {
		if err := d.addAlloc(add); err != nil {
			d.logger.Error("error adding alloc", zap.String("error", err.Error()), zap.String("alloc_id", add.ID))
			errs++
			// We mark the alloc as failed and send an update to the server
			// We track the fact that creating an allocrunner failed so that we don't send updates again
			if add.ClientStatus != constant.AllocClientStatusFailed {
				d.handleInvalidAllocs(add, err)
			}
		}
		flag = true
	}
	if flag {
		d.logger.Debug("allocation updates applied", zap.Int("added", len(allocAddList)), zap.Int("errors", errs))
	}
}

/**
 * 将拿到的allocation 发给runner并记录在d.alloc中去
 */
func (d *Dispatcher) addAlloc(alloc *domain.Allocation) error {
	d.allocLock.Lock()
	defer d.allocLock.Unlock()
	// 运行前需要检查是否已经运行过
	if _, ok := d.allocs[alloc.ID]; ok {
		d.logger.Debug("dropping duplicate add allocation request", zap.String("alloc_id", alloc.ID))
		return nil
	}
	d.configLock.RLock()
	// query plan detail for alloc
	plan, err := d.getPlanByPlanId(alloc.PlanID, alloc.Namespace)
	if err != nil {
		return err
	}
	alloc.Plan = plan
	arConf := &runner.Config{
		Alloc:        alloc,
		Logger:       d.logger,
		StateUpdater: d,
		// Region:       d.Region(),
	}
	d.configLock.RUnlock()

	ar, err := runner.NewAllocRunner(arConf)
	if err != nil {
		return err
	}

	// Store the alloc runner.
	d.allocs[alloc.ID] = ar

	go ar.Run()
	return nil
}

func (d *Dispatcher) updateAlloc(update *domain.Allocation) {
	ar, err := d.getAllocRunner(update.ID)
	if err != nil {
		d.logger.Warn("cannot update nonexistent alloc", zap.String("alloc_id", update.ID))
		return
	}

	// Reconnect unknown allocations
	if update.ClientStatus == constant.AllocClientStatusUnknown && update.AllocModifyIndex > ar.Alloc().AllocModifyIndex {
		return
	}

	// Update alloc runner
	ar.Update(update)
}

func (d *Dispatcher) handleInvalidAllocs(alloc *domain.Allocation, err error) {
	d.invalidAllocsLock.Lock()
	d.invalidAllocs[alloc.ID] = struct{}{}
	d.invalidAllocsLock.Unlock()

	// Mark alloc as failed so server can handle this
	failed := makeFailedAlloc(alloc, err)
	select {
	case d.allocUpdates <- failed:
	case <-d.shutdownCh:
	}
}

func makeFailedAlloc(add *domain.Allocation, err error) *domain.Allocation {
	stripped := new(domain.Allocation)
	stripped.ID = add.ID
	stripped.ClientStatus = constant.AllocClientStatusFailed
	stripped.ClientDescription = fmt.Sprintf("Unable to add allocation due to error: %v", err)
	return stripped
}

func (d *Dispatcher) getAllocRunner(allocID string) (service.AllocRunner, error) {
	d.allocLock.RLock()
	defer d.allocLock.RUnlock()

	ar, ok := d.allocs[allocID]
	if !ok {
		return nil, fmt.Errorf("%s %q", "Unknown allocation", allocID)
	}

	return ar, nil
}

func (d *Dispatcher) getPlanByPlanId(planID string, namespace string) (*domain.Plan, error) {
	resp, err := d.client.PlanDetail(d.ctx, &etcdserverpb.SchedulePlanDetailRequest{Id: planID, Namespace: namespace})
	if err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return nil, fmt.Errorf("can't find the plan by id: %s, namespace: %s", planID, namespace)
	}
	return domain.ConvertPlan(resp.Data), nil
}
