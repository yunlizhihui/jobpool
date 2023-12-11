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

package constant

const (
	EvalStatusBlocked   = "blocked"
	EvalStatusPending   = "pending"
	EvalStatusComplete  = "complete"
	EvalStatusFailed    = "failed"
	EvalStatusCancelled = "canceled"
)

const (
	QueueNameFailed = "_failed"
)


const (
	EvalTriggerJobRegister          = "job-register"
	EvalTriggerJobDeregister        = "job-deregister"
	EvalTriggerPeriodicJob          = "periodic-job"
	EvalTriggerNodeDrain            = "node-drain"
	EvalTriggerNodeUpdate           = "node-update"
	EvalTriggerAllocStop            = "alloc-stop"
	EvalTriggerScheduled            = "scheduled"
	EvalTriggerRollingUpdate        = "rolling-update"
	EvalTriggerDeploymentWatcher    = "deployment-watcher"
	EvalTriggerFailedFollowUp       = "failed-follow-up"
	EvalTriggerMaxPlans             = "max-plan-attempts"
	EvalTriggerRetryFailedAlloc     = "alloc-failure"
	EvalTriggerQueuedAllocs         = "queued-allocs"
	EvalTriggerPreemption           = "preemption"
	EvalTriggerScaling              = "job-scaling"
	EvalTriggerMaxDisconnectTimeout = "max-disconnect-timeout"
	EvalTriggerReconnect            = "reconnect"
)
