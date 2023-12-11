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
	"fmt"
	"go.uber.org/zap"
	"yunli.com/jobpool/api/v2/constant"
)

// NewScheduler 创建一个调度器
func NewScheduler(name string, logger *zap.Logger, state ScheduleRepository, planWorker PlanWorkerService) (Scheduler, error) {
	factory, ok := SchedulerFactory[name]
	if !ok {
		return nil, fmt.Errorf("unknown scheduler '%s'", name)
	}
	sched := factory(logger, state, planWorker)
	return sched, nil
}

type Factory func(*zap.Logger, ScheduleRepository, PlanWorkerService) Scheduler

// SchedulerFactory 调度器工厂，目前只支持一种
var SchedulerFactory = map[string]Factory{
	constant.PlanTypeService: NewServiceScheduler,
}

// NewServiceScheduler 初始化一个服务执行的调度器
func NewServiceScheduler(logger *zap.Logger, state ScheduleRepository, planWorker PlanWorkerService) Scheduler {
	return NewGenericScheduler("service_scheduler", logger, state, planWorker)
}
