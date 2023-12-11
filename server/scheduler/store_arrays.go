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

import "yunli.com/jobpool/api/v2/schedulepb"

// PlanByCreateDesc plan desc by createTime
type PlanByCreateDesc []*schedulepb.Plan

func (ms PlanByCreateDesc) Len() int { return len(ms) }
func (ms PlanByCreateDesc) Less(i, j int) bool {
	return ms[j].CreateTime.GetSeconds() < ms[i].CreateTime.GetSeconds()
}
func (ms PlanByCreateDesc) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }

// JobByCreateDesc Job desc by createTime
type JobByCreateDesc []*schedulepb.Job

func (ms JobByCreateDesc) Len() int { return len(ms) }
func (ms JobByCreateDesc) Less(i, j int) bool {
	return ms[j].CreateTime.GetSeconds() < ms[i].CreateTime.GetSeconds()
}
func (ms JobByCreateDesc) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }

// EvalByCreateDesc Evaluation desc by createTime
type EvalByCreateDesc []*schedulepb.Evaluation

func (ms EvalByCreateDesc) Len() int { return len(ms) }
func (ms EvalByCreateDesc) Less(i, j int) bool {
	return ms[j].CreateTime.GetSeconds() < ms[i].CreateTime.GetSeconds()
}
func (ms EvalByCreateDesc) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }

// AllocationByCreateDesc Allocation desc by createTime
type AllocationByCreateDesc []*schedulepb.Allocation

func (ms AllocationByCreateDesc) Len() int { return len(ms) }
func (ms AllocationByCreateDesc) Less(i, j int) bool {
	return ms[j].CreateTime.GetSeconds() < ms[i].CreateTime.GetSeconds()
}
func (ms AllocationByCreateDesc) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
