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

package domain

import "time"

// PeriodicLaunch 计划最后一次执行的时间记录
type PeriodicLaunch struct {
	ID        string    // ID 计划编码
	Namespace string    // Namespace 空间
	Launch    time.Time // 最后执行时间
}
