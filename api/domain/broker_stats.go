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

// BrokerStats returns all the stats about the broker
type BrokerStats struct {
	TotalReady       int
	TotalUnacked     int
	TotalBlocked     int
	TotalWaiting     int
	TotalFailedQueue int
	DelayedEvals     map[string]*Evaluation
	ByScheduler      map[string]*SchedulerStats
}

// SchedulerStats returns the stats per scheduler
type SchedulerStats struct {
	Ready   int
	Unacked int
}

// 计数器
type BlockedStats struct {
	// TotalEscaped block 的 eval总和以便计算class.
	TotalEscaped int

	// TotalBlocked 总评估.
	TotalBlocked int

	TotalCaptured int
}

func (b *BlockedStats) Block(eval *Evaluation) {
	b.TotalBlocked++
}
func (b *BlockedStats) Unblock(eval *Evaluation) {
	b.TotalBlocked--
}

type JobViewStats struct {
	TotalPending map[string]int
	TotalRunning map[string]int
	TotalRetry   map[string]int
}

type JobMapStats struct {
	TotalPending int
	TotalRunning int
	TotalRetry   int
	TotalUnUsed  int
	RunningJobs  []string `json:"runningJobs"`
	PendingJobs  []string `json:"pendingJobs"`
}

type JobSlot struct {
	ID         string
	Name       string
	Type       string
	CreateTime time.Time
}
