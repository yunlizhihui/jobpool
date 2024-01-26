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

package runner

import (
	"fmt"
	"go.uber.org/zap"
	"sync"
	"yunli.com/jobpool/agent/v2/plugins"
	"yunli.com/jobpool/agent/v2/plugins/rest"
	"yunli.com/jobpool/agent/v2/service"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
)

type allocRunner struct {
	id        string
	logger    *zap.Logger
	alloc     *domain.Allocation
	allocLock sync.RWMutex

	// 用户调用client接口
	stateUpdater service.AllocStateHandler
	// 用于状态更新输入
	jobStateUpdatedCh chan struct{}

	waitCh                   chan struct{}
	taskStateUpdateHandlerCh chan struct{}
	allocUpdatedCh           chan *domain.Allocation

	// 运行正常任务的
	runner plugins.PluginRunner
	// 运行不正常任务的
	nervous plugins.PluginJobDispatcher

	// state is the alloc runner's state
	state     *State
	stateLock sync.RWMutex
}

func NewAllocRunner(config *Config) (*allocRunner, error) {
	alloc := config.Alloc
	ar := &allocRunner{
		alloc:                    alloc,
		id:                       alloc.ID,
		logger:                   config.Logger,
		waitCh:                   make(chan struct{}),
		taskStateUpdateHandlerCh: make(chan struct{}),
		state:                    &State{},
		stateUpdater:             config.StateUpdater,
		jobStateUpdatedCh:        make(chan struct{}, 1),
		allocUpdatedCh:           make(chan *domain.Allocation, 1),
	}
	ar.runner = rest.NewPlugin(nil, ar.logger)
	ar.nervous = rest.NewDispatcher(nil, ar.logger)
	return ar, nil
}

func (ar *allocRunner) Run() {
	defer close(ar.waitCh)
	// 增加监听
	go ar.handleJobStateUpdates()

	go ar.handleAllocUpdates()

	// If task update chan has been closed, that means we've been shutdown.
	select {
	case <-ar.taskStateUpdateHandlerCh:
		return
	default:
	}
	if ar.shouldRun() {
		ar.logger.Debug("should run the job")
		// run之前需要确认这个job状态（避免同一个job多次执行的问题）
		jobStatus := ar.stateUpdater.GetAllocJobStatus(ar.alloc)
		if constant.JobStatusPending != jobStatus {
			ar.logger.Debug("the job status is not pending, won't run this job", zap.String("jobId", ar.alloc.JobId), zap.String("namespace", ar.alloc.Namespace), zap.String("status", jobStatus))
			return
		}
		var result *plugins.JobResult
		if constant.AllocDesiredStatusSkip == ar.alloc.DesiredStatus {
			result = &plugins.JobResult{
				Message: "finish run skip allocation in dispatcher",
			}
			result.State = ar.RunSkipAllocation()
		} else if constant.AllocDesiredStatusFail == ar.alloc.DesiredStatus {
			result = &plugins.JobResult{
				Message: "finish run failed allocation in dispatcher",
			}
			result.State = ar.RunFailAllocation()
		} else {
			jobRequest := &plugins.JobRequest{
				JobId:   ar.alloc.JobId,
				Params:  make(map[string]string),
				Setting: ar.alloc.Plan.Parameters,
			}
			if ar.alloc.Plan.Synchronous {
				jobResult, err := ar.runner.StartSynchroJob(jobRequest)
				if err != nil {
					ar.logger.Warn("run the job failed", zap.Error(err))
				}
				result = jobResult
			} else {
				ar.logger.Debug("start run asyn job", zap.String("jobId", jobRequest.JobId), zap.String("jobrequest_setting", string(jobRequest.Setting)))
				body := fmt.Sprintf(`{"planId": "%s", "jobId":"%s"}`, ar.alloc.PlanID, ar.alloc.JobId)
				jobRequest.Params["body"] = body
				jobResult, err := ar.runner.StartAsynchroJob(jobRequest)
				if err != nil {
					ar.logger.Warn("run the job failed", zap.Error(err))
				}
				result = jobResult
			}
		}

		ar.logger.Debug("the result is", zap.Reflect("result", result))
		// storage state
		ar.stateLock.Lock()
		defer ar.stateLock.Unlock()
		ar.state.JobStatus = result.State
		ar.state.ClientStatus = result.State
		ar.state.Info = result.Message
		ar.JobStateUpdated()
	}
}

func (ar *allocRunner) JobStateUpdated() {
	select {
	case ar.jobStateUpdatedCh <- struct{}{}:
	default:
		// already pending updates
	}
}
func (ar *allocRunner) shouldRun() bool {
	// Do not run allocs that are terminal
	if ar.Alloc().TerminalStatus() {
		ar.logger.Debug("alloc terminal; not running",
			zap.String("desired_status", ar.Alloc().DesiredStatus),
			zap.String("client_status", ar.Alloc().ClientStatus),
		)
		return false
	}

	// It's possible that the alloc local state was marked terminal before
	// the server copy of the alloc (checked above) was marked as terminal,
	// so check the local state as well.
	switch clientStatus := ar.AllocState().ClientStatus; clientStatus {
	case constant.AllocClientStatusComplete, constant.AllocClientStatusFailed, constant.AllocClientStatusExpired, constant.AllocClientStatusSkipped, constant.AllocClientStatusUnknown:
		ar.logger.Debug("alloc terminal; updating server and not running", zap.String("status", clientStatus))
		return false
	}
	return true
}

func (ar *allocRunner) Alloc() *domain.Allocation {
	ar.allocLock.RLock()
	defer ar.allocLock.RUnlock()
	return ar.alloc
}

func (ar *allocRunner) AllocState() *State {
	return ar.state
}

func (ar *allocRunner) handleAllocUpdates() {
	for {
		select {
		case update := <-ar.allocUpdatedCh:
			ar.handleAllocUpdate(update)
		case <-ar.waitCh:
			return
		}
	}
}

func (ar *allocRunner) handleAllocUpdate(update *domain.Allocation) {
	// Update ar.alloc
	ar.setAlloc(update)
}

func (ar *allocRunner) setAlloc(updated *domain.Allocation) {
	ar.allocLock.Lock()
	ar.alloc = updated
	ar.allocLock.Unlock()
}

func (ar *allocRunner) handleJobStateUpdates() {
	defer close(ar.taskStateUpdateHandlerCh)
	for done := false; !done; {
		select {
		case <-ar.jobStateUpdatedCh:
			ar.logger.Debug("receive jobStateUpdatedCh")
		case <-ar.waitCh:
			ar.logger.Debug("receive waitCH")
			// Run has exited, sync once more to ensure final
			// states are collected.
			done = true
		}

		// Task state has been updated; gather the state of the other tasks
		// trNum := len(ar.tasks)
		//trNum := 1
		//states := make(map[string]*structs.TaskState, trNum)
		//
		//if trNum > 0 {
		//	// Get the client allocation
		calloc := ar.clientAlloc()
		// Update the server
		ar.stateUpdater.AllocStateUpdated(calloc)
		//
		//	// Broadcast client alloc to listeners
		// ar.allocBroadcaster.Send(calloc)
		//}
	}
}

func (ar *allocRunner) clientAlloc() *domain.Allocation {
	a := &domain.Allocation{
		ID: ar.id,
	}
	if ar.alloc != nil && ar.alloc.JobId != "" {
		a.JobId = ar.alloc.JobId
		a.Namespace = ar.alloc.Namespace
	}
	// Compute the ClientStatus
	if ar.state.ClientStatus != "" {
		// The client status is being forced
		a.ClientStatus, a.ClientDescription = ar.state.ClientStatus, ar.state.Info
	} else {
		a.ClientStatus, a.ClientDescription = getClientStatus(ar.state.JobStatus, ar.state.Info)
	}
	return a
}

func getClientStatus(taskStates string, info string) (status, description string) {
	var pending, running, dead, failed bool
	switch taskStates {
	case constant.JobStatusRunning:
		running = true
	case constant.JobStatusPending:
		pending = true
	case constant.JobStatusSkipped:
		dead = true
	case constant.JobStatusCancelled:
		dead = true
	case constant.JobStatusComplete:
		dead = true
	case constant.JobStatusFailed:
		failed = true
	}
	// Determine the alloc status
	if failed {
		if info == "" {
			return constant.AllocClientStatusFailed, "Failed jobs"
		} else {
			return constant.AllocClientStatusFailed, info
		}
	} else if running {
		return constant.AllocClientStatusRunning, "job are running"
	} else if pending {
		return constant.AllocClientStatusPending, "No job have started"
	} else if dead {
		return constant.AllocClientStatusComplete, "The job have completed"
	}

	// fmt.Println(fmt.Sprintf("status in tasks pending, running, dea, failed: %s, %s, %s, %s", pending, running, dead, failed))
	return "", ""
}

func (ar *allocRunner) Update(update *domain.Allocation) {
	select {
	// Drain queued update from the channel if possible, and check the modify
	// index
	case oldUpdate := <-ar.allocUpdatedCh:
		// If the old update is newer than the replacement, then skip the new one
		// and return. This case shouldn't happen, but may in the case of a bug
		// elsewhere inside the system.
		if oldUpdate.AllocModifyIndex > update.AllocModifyIndex {
			ar.logger.Debug("Discarding allocation update due to newer alloc revision in queue",
				zap.Uint64("old_modify_index", oldUpdate.AllocModifyIndex),
				zap.Uint64("new_modify_index", update.AllocModifyIndex))
			ar.allocUpdatedCh <- oldUpdate
			return
		} else {
			ar.logger.Debug("Discarding allocation update",
				zap.Uint64("skipped_modify_index", oldUpdate.AllocModifyIndex),
				zap.Uint64("new_modify_index", update.AllocModifyIndex))
		}
	case <-ar.waitCh:
		ar.logger.Debug("AllocRunner has terminated, skipping alloc update",
			zap.Uint64("modify_index", update.AllocModifyIndex))
		return
	default:
	}

	// Queue the new update
	ar.allocUpdatedCh <- update
}

func (ar *allocRunner) RunSkipAllocation() string {
	err := ar.nervous.DispatchSkipJob(ar.alloc.PlanID, ar.alloc.JobId, []byte(ar.alloc.ClientDescription))
	if err != nil {
		ar.logger.Warn("dispatch skip job failed", zap.Error(err))
		return constant.JobStatusFailed
	}
	return constant.JobStatusSkipped
}

func (ar *allocRunner) RunFailAllocation() string {
	err := ar.nervous.DispatchNoSlotJob(ar.alloc.PlanID, ar.alloc.JobId, []byte(ar.alloc.ClientDescription))
	if err != nil {
		ar.logger.Warn("dispatch failed job failed", zap.Error(err))
		return constant.JobStatusFailed
	}
	return constant.JobStatusSkipped
}
