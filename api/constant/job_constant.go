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
	JobStatusPending   = "pending"
	JobStatusRunning   = "running"
	JobStatusComplete  = "complete"
	JobStatusSkipped   = "skipped"
	JobStatusFailed    = "failed"
	JobStatusCancelled = "cancelled"
	JobStatusExpired   = "expired"
	JobStatusUnknown   = "unknown"
)

const (
	JobTypeAuto = "auto"
)

var JobStatusSet = map[string]bool{
	JobStatusPending:   true,
	JobStatusRunning:   true,
	JobStatusComplete:  true,
	JobStatusSkipped:   true,
	JobStatusFailed:    true,
	JobStatusCancelled: true,
	JobStatusExpired:   true,
	JobStatusUnknown:   true,
}
