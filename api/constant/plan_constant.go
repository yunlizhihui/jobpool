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
	// PlanTypeCore 用于区分系统任务还是业务任务
	PlanTypeCore    = "_core"
	PlanTypeService = "service"
)

const (
	PlanStatusPending = "pending" // Pending means the plan is waiting on scheduling
	PlanStatusRunning = "running" // Running means the plan has non-terminal allocations
	PlanStatusDead    = "dead"    // Dead means all evaluation's and allocations are terminal
)
