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
	"yunli.com/jobpool/api/v2/domain"
)

type Scheduler interface {
	// Process 是调度核心逻辑，目的是进行任务评估的业务逻辑
	Process(*domain.Evaluation) error
}

type ScheduleRepository interface {
	PlanByID(namespace string, id string) (*domain.Plan, error)

	JobByID(id string) (*domain.Job, error)

	UnhealthAllocsByPlan(namespace string, id string, b bool) ([]*domain.Allocation, error)

	NodeByID(id string) (*domain.Node, error)

	Nodes() ([]*domain.Node, error)

	RunningAllocsByNamespace(namespace string) ([]*domain.Allocation, error) // AllocsRunningByNamespace 找到已经分配了的allocation
}

// PlanWorkerService 计划的具体执行者需要做的事情
type PlanWorkerService interface {
	UpdateEval(eval *domain.Evaluation) error

	CreateEval(eval *domain.Evaluation) error

	SubmitPlan(alloc *domain.PlanAlloc) (*domain.PlanResult, error)

	ReblockEval(*domain.Evaluation) error

	RunningSlotLeft(namespace string, hasAlloc int) bool
}
