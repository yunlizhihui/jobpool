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
	"container/heap"
	"go.uber.org/zap"
	"sync"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
)

// JobBirdEyeView 任务地图
type JobBirdEyeView struct {
	enabled     bool
	logger      *zap.Logger
	size        int
	slotPending map[string]SlotJobs
	slotRunning map[string]SlotJobs
	slotRetry   map[string]SlotJobs
	stats       *domain.JobViewStats
	l           sync.RWMutex
}

// NewJobBirdEyeView 新建一个任务地图
func NewJobBirdEyeView(size int, log *zap.Logger) (*JobBirdEyeView, error) {
	initSize := 100
	if size > 0 {
		initSize = size
	}
	q := &JobBirdEyeView{
		enabled:     false,
		logger:      log,
		size:        initSize,
		slotPending: make(map[string]SlotJobs),
		slotRunning: make(map[string]SlotJobs),
		slotRetry:   make(map[string]SlotJobs),
		stats:       new(domain.JobViewStats),
	}
	q.stats.TotalPending = make(map[string]int)
	q.stats.TotalRetry = make(map[string]int)
	q.stats.TotalRunning = make(map[string]int)
	return q, nil
}

func (q *JobBirdEyeView) Enabled() bool {
	q.l.RLock()
	defer q.l.RUnlock()
	return q.enabled
}

func (q *JobBirdEyeView) SetEnabled(enabled bool) {
	q.l.Lock()
	defer q.l.Unlock()

	prevEnabled := q.enabled
	q.enabled = enabled
	if !prevEnabled && enabled {
		// start the go routine for instant jobs
	}
	if !enabled {
		q.flush()
	}
}

func (q *JobBirdEyeView) Enqueue(eval *domain.Evaluation) {
	q.l.Lock()
	defer q.l.Unlock()
	if !q.enabled {
		return
	}
	if eval == nil || eval.JobID == "" {
		return
	}

	namespacedID := eval.Namespace
	waiting := q.slotPending[namespacedID]
	waitSlot := &domain.JobSlot{
		ID:         eval.JobID,
		CreateTime: eval.CreateTime.TimeValue(),
	}
	if waiting.containsId(eval.JobID) {
		return
	}
	heap.Push(&waiting, waitSlot)
	q.slotPending[namespacedID] = waiting
	q.stats.TotalPending[namespacedID] += 1
}

func (q *JobBirdEyeView) EnqueueAll(jobs []*domain.Job) {
	q.l.Lock()
	defer q.l.Unlock()
	for _, job := range jobs {
		q.processEnqueue(job)
	}
}

func (q *JobBirdEyeView) processEnqueue(job *domain.Job) {
	if !q.enabled {
		return
	}
	if job == nil || job.ID == "" {
		return
	}
	if constant.JobStatusSkipped == job.Status {
		return
	}
	// 添加必然是pennding
	if constant.JobStatusPending != job.Status {
		q.logger.Warn("wrong job status in add process", zap.String("status", job.Status), zap.Reflect("job", job))
		return
	}
	namespacedID := job.Namespace
	pending := q.slotPending[namespacedID]
	if pending.containsId(job.ID) {
		return
	}
	heap.Push(&pending, job.ConvertJobSlot())
	q.slotPending[namespacedID] = pending
	q.stats.TotalPending[namespacedID] += 1
}

// 任务启动后将其移动到running中
func (q *JobBirdEyeView) JobStartRunning(namespace string, jobId string) error {
	q.l.Lock()
	defer q.l.Unlock()
	if !q.enabled {
		return nil
	}
	index, slot := q.slotPending[namespace].Pick(jobId)
	if slot == nil {
		q.logger.Debug("dequeue eval failed in job roadmap", zap.String("JobID", jobId))
		return nil
	}
	slotJob := q.slotPending[namespace]
	heap.Remove(&slotJob, index)
	q.slotPending[namespace] = slotJob
	q.stats.TotalPending[namespace] -= 1
	// 将其放入running
	runningJobs := q.slotRunning[namespace]
	heap.Push(&runningJobs, slot)
	q.slotRunning[namespace] = runningJobs
	q.stats.TotalRunning[namespace] += 1
	return nil
}

func (q *JobBirdEyeView) HasEmptySlot(namespace string) bool {
	q.l.Lock()
	defer q.l.Unlock()
	if !q.enabled {
		return true
	}
	slotLeft := q.getSlotLeft(namespace)
	if slotLeft > 0 {
		return true
	}
	return false
}

func (q *JobBirdEyeView) DequeueJob(namespace string, jobId string) error {
	q.l.Lock()
	defer q.l.Unlock()
	if !q.enabled {
		return nil
	}
	indexPending, evalPending := q.slotPending[namespace].Pick(jobId)
	indexRunning, evalRunning := q.slotRunning[namespace].Pick(jobId)
	if evalPending == nil && evalRunning == nil {
		q.logger.Debug("dequeue evalPending failed in job roadmap", zap.String("JobID", jobId))
		return nil
	}
	if evalPending != nil {
		slotJob := q.slotPending[namespace]
		heap.Remove(&slotJob, indexPending)
		q.slotPending[namespace] = slotJob
		q.stats.TotalPending[namespace] -= 1
	}
	if evalRunning != nil {
		slotJob := q.slotRunning[namespace]
		heap.Remove(&slotJob, indexRunning)
		q.slotRunning[namespace] = slotJob
		q.stats.TotalRunning[namespace] -= 1
	}
	return nil
}

func (q *JobBirdEyeView) flush() {
	// 清空
	q.stats.TotalPending = make(map[string]int)
	q.stats.TotalRetry = make(map[string]int)
	q.stats.TotalRunning = make(map[string]int)
	q.slotPending = make(map[string]SlotJobs)
	q.slotRetry = make(map[string]SlotJobs)
	q.slotRunning = make(map[string]SlotJobs)
}

func (q *JobBirdEyeView) Stats(namespace string) *domain.JobMapStats {
	stats := new(domain.JobMapStats)
	q.l.RLock()
	defer q.l.RUnlock()
	stats.TotalRunning = q.stats.TotalRunning[namespace]
	stats.TotalRetry = q.stats.TotalRetry[namespace]
	stats.TotalPending = q.stats.TotalPending[namespace]
	stats.TotalUnUsed = q.getSlotLeft(namespace)

	// 将队列中的内容打印出来
	listRunning := q.slotRunning[namespace]
	runningJobs := make([]string, 0)
	if listRunning != nil {
		for _, item := range listRunning {
			q.logger.Debug("running", zap.String("id", item.ID))
			runningJobs = append(runningJobs, item.ID)
		}
	}
	stats.RunningJobs = runningJobs
	listPending := q.slotPending[namespace]
	pendingJobs := make([]string, 0)
	if listPending != nil {
		for _, item := range listPending {
			q.logger.Debug("pending", zap.String("id", item.ID))
			pendingJobs = append(pendingJobs, item.ID)
		}
	}
	stats.PendingJobs = pendingJobs
	listRetry := q.slotRetry[namespace]
	if listRetry != nil {
		for _, item := range listRetry {
			q.logger.Debug("retry", zap.String("id", item.ID))
		}
	}

	return stats
}

func (q *JobBirdEyeView) getSlotLeft(namespace string) int {
	slotLeft := q.size - q.stats.TotalRunning[namespace] - q.stats.TotalRetry[namespace] - q.stats.TotalPending[namespace]
	return slotLeft
}

// 仅用于server重启后的恢复
func (q *JobBirdEyeView) JobRunningRestore(job *domain.Job) {
	q.l.Lock()
	defer q.l.Unlock()
	if !q.enabled {
		return
	}
	if job == nil {
		return
	}
	if constant.JobStatusRunning != job.Status {
		q.logger.Warn("wrong job status in restore process", zap.String("status", job.Status), zap.Reflect("job", job))
		return
	}
	namespacedID := job.Namespace
	running := q.slotRunning[namespacedID]
	if running.containsId(job.ID) {
		return
	}
	heap.Push(&running, job.ConvertJobSlot())
	q.slotRunning[namespacedID] = running
	q.stats.TotalRunning[namespacedID] += 1
}

type SlotJobs []*domain.JobSlot

func (s SlotJobs) Len() int {
	return len(s)
}

func (s SlotJobs) Less(i, j int) bool {
	return s[i].CreateTime.Unix() < s[j].CreateTime.Unix()
}

func (s SlotJobs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *SlotJobs) Push(e interface{}) {
	*s = append(*s, e.(*domain.JobSlot))
}

func (s *SlotJobs) Pop() interface{} {
	n := len(*s)
	e := (*s)[n-1]
	(*s)[n-1] = nil
	*s = (*s)[:n-1]
	return e
}

func (s SlotJobs) Pick(ID string) (int, *domain.JobSlot) {
	flagIndex := -1
	var selected *domain.JobSlot
	for index, item := range s {
		if item.ID == ID {
			flagIndex = index
			selected = item
		}
	}
	if flagIndex >= 0 {
		return flagIndex, selected
		// s = append(s[:flagIndex], s[(flagIndex+1):]...)
	} else {
		return flagIndex, nil
	}
}

func (s SlotJobs) containsId(ID string) bool {
	for _, item := range s {
		if item.ID == ID {
			return true
		}
	}
	return false
}
