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
	"context"
	"fmt"
	"go.uber.org/zap"
	"sync"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/schedulepb"
)

// PlanEvalDispatcher is an interface to submit plans and have evaluations created
// for them.
type PlanEvalDispatcher interface {
	// DispatchPlan takes a Plan a new, untracked Plan and creates an evaluation
	// for it and returns the eval.
	DispatchPlan(plan *domain.Plan) (*schedulepb.Evaluation, error)

	// RunningChildren returns whether the passed Plan has any running children.
	RunningChildren(plan *domain.Plan) (bool, error)

	// SkipPlan create skipped job and task to save this periodic
	SkipPlan(plan *domain.Plan) error

	// ExpirePlan 调度将在有效日期内自动生效，反之，在有效期外的任务将不会自动调度
	ExpirePlan(plan *domain.Plan) error

	ReadyNotify() <-chan struct{}
}

type PeriodicDispatch struct {
	enabled    bool
	dispatcher PlanEvalDispatcher
	tracked    map[domain.NamespacedID]*domain.Plan
	heap       *periodicHeap
	updateCh   chan struct{}
	stopFn     context.CancelFunc
	logger     *zap.Logger
	l          sync.RWMutex
}

func NewPeriodicDispatch(logger *zap.Logger, dispatcher PlanEvalDispatcher) *PeriodicDispatch {
	return &PeriodicDispatch{
		dispatcher: dispatcher,
		tracked:    make(map[domain.NamespacedID]*domain.Plan),
		heap:       NewPeriodicHeap(),
		updateCh:   make(chan struct{}, 1),
		logger:     logger,
	}
}

func (p *PeriodicDispatch) SetEnabled(enabled bool) {
	p.l.Lock()
	defer p.l.Unlock()
	wasRunning := p.enabled
	p.enabled = enabled

	// If we are transitioning from enabled to disabled, stop the daemon and
	// flush.
	if !enabled && wasRunning {
		p.stopFn()
		p.flush()
	} else if enabled && !wasRunning {
		// If we are transitioning from disabled to enabled, run the daemon.
		ctx, cancel := context.WithCancel(context.Background())
		p.stopFn = cancel
		go p.run(ctx, p.updateCh)
	}
}

func (p *PeriodicDispatch) run(ctx context.Context, updateCh <-chan struct{}) {
	var launchCh <-chan time.Time
	select {
	case <-p.dispatcher.ReadyNotify():
	}
	for p.isEnabled() {
		plan, launch := p.nextLaunch()
		// get start time and end time and deal with the plan expired
		skipThisPlan := false
		if plan != nil {
			// plan的创建时间在启动时间之后，则不参与这次调度
			if plan.CreateTime.TimeValue().After(launch) {
				skipThisPlan = true
			} else if plan.Periodic.StartTime != "" && !plan.Periodic.StartTime.TimeValue().IsZero() {
				if time.Now().Before(plan.Periodic.StartTime.TimeValue()) {
					// 时间没到不需要调度
					skipThisPlan = true
				}
			}
			if launch.IsZero() {
				launchCh = nil
			} else {
				launchDur := launch.Sub(time.Now().In(plan.Periodic.GetLocation()))
				launchCh = time.After(launchDur)
				p.logger.Debug("scheduled periodic Plan launch", zap.String("launch_delay", launchDur.String()), zap.String("Plan", plan.NamespacedID().String()))
			}
		} else {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-updateCh:
			continue
		case <-launchCh:
			p.dispatchLaunch(plan, launch, skipThisPlan)
		}
	}
}

// dispatchLaunch 计划执行逻辑
func (p *PeriodicDispatch) dispatchLaunch(plan *domain.Plan, launchTime time.Time, skipThisPlan bool) {
	p.l.Lock()
	nextLaunch, err := plan.Periodic.Next(launchTime)
	if err != nil {
		p.logger.Error("failed to parse next periodic launch", zap.String("Plan", plan.NamespacedID().String()), zap.Error(err))
	}
	p.l.Unlock()
	// 下次调度需要看是否超出了endtime
	skipNextPlan := false
	if plan.Periodic.EndTime != "" && !plan.Periodic.EndTime.TimeValue().IsZero() {
		// TODO 优化点，应该知道下次过期就直接不要放heap里了，这里防止最后一个任务不运行才判断当前时间
		if nextLaunch.After(plan.Periodic.EndTime.TimeValue()) && time.Now().After(plan.Periodic.EndTime.TimeValue()) {
			skipNextPlan = true
			skipThisPlan = true
			err := p.dispatcher.ExpirePlan(plan)
			if err != nil {
				p.logger.Warn("the plan has expired and can't delete from periodic", zap.Time("endtime", plan.Periodic.EndTime.TimeValue()), zap.String("planId", plan.ID), zap.Error(err))
				// 加固，跳过失败，需要从heap中删掉
				if errRemove := p.Remove(plan.Namespace, plan.ID); errRemove != nil {
					p.logger.Warn("remove plan from dispatcher failed", zap.Error(errRemove))
				}
			}
		}
	}
	if !skipNextPlan {
		p.l.Lock()
		if err := p.heap.Update(plan, nextLaunch); err != nil {
			p.logger.Error("failed to update next launch of periodic Plan", zap.String("Plan", plan.NamespacedID().String()), zap.Error(err))
		}
		p.l.Unlock()
	}
	p.logger.Debug(" launching Plan", zap.String("Plan", plan.NamespacedID().String()), zap.Time("launch_time", launchTime))
	if !skipThisPlan {
		p.dispatch(plan, launchTime)
	}
}

func (p *PeriodicDispatch) dispatch(plan *domain.Plan, launchTime time.Time) {
	p.l.Lock()
	// If the Plan prohibits overlapping and there are running children, we skip
	// the launch.
	p.logger.Debug("in periodic dispatch find the running plan", zap.Reflect("plan", plan))
	if plan.Periodic.ProhibitOverlap {
		running, err := p.dispatcher.RunningChildren(plan)
		if err != nil {
			p.logger.Error("failed to determine if periodic Plan has running children", zap.String("Plan", plan.NamespacedID().String()), zap.Error(err))
			p.l.Unlock()
			return
		}
		p.logger.Debug("in periodic has  running", zap.Bool("runn", running))
		if running && !plan.Synchronous {
			// 创建job和task，状态均为跳过
			p.logger.Debug("skipping launch of periodic Plan because Plan prohibits overlap", zap.String("Plan", plan.NamespacedID().String()))
			p.l.Unlock()
			err = p.dispatcher.SkipPlan(plan)
			if err != nil {
				p.logger.Error("create skipped plan failed", zap.String("Plan", plan.Name), zap.Error(err))
				return
			}
			return
		}
	}
	p.l.Unlock()
	p.createEvaluation(plan, launchTime)
}

// createEvaluation 创建评估对象
func (p *PeriodicDispatch) createEvaluation(periodicPlan *domain.Plan, time time.Time) (*schedulepb.Evaluation, error) {
	p.logger.Debug("create eval for plan", zap.String("plan-name", periodicPlan.Name))
	derived, err := p.generatePlan(periodicPlan, time)
	if err != nil {
		return nil, err
	}

	eval, err := p.dispatcher.DispatchPlan(derived)
	if err != nil {
		p.logger.Error("failed to dispatch Plan", zap.String("Plan", periodicPlan.NamespacedID().String()), zap.Error(err))
		return nil, err
	}
	return eval, nil
}

// generatePlan 生成一个周期计划
func (p *PeriodicDispatch) generatePlan(periodicPlan *domain.Plan, time time.Time) (
	derived *domain.Plan, err error) {

	// Have to recover in case the Plan copy panics.
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("deriving child plan from periodic plan failed; deregister from periodic runner",
				zap.String("Plan", periodicPlan.NamespacedID().String()), zap.Error(err))

			p.Remove(periodicPlan.Namespace, periodicPlan.ID)
			derived = nil
			err = fmt.Errorf("Failed to create a copy of the periodic Plan %q (%s): %v",
				periodicPlan.ID, periodicPlan.Namespace, r)
		}
	}()

	// 创建一个计划副本，ID格式需要带时间戳以便唯一标识，非常重要
	derived = periodicPlan.Copy()
	derived.ParentID = periodicPlan.ID
	derived.ID = fmt.Sprintf("%s%s%d", periodicPlan.ID, constant.PeriodicLaunchSuffix, time.Unix())
	derived.Name = periodicPlan.Name
	derived.Periodic = nil
	derived.Status = ""
	return
}

// isEnabled 是否有效
func (p *PeriodicDispatch) isEnabled() bool {
	p.l.RLock()
	defer p.l.RUnlock()
	return p.enabled
}

func (p *PeriodicDispatch) nextLaunch() (*domain.Plan, time.Time) {
	p.l.RLock()
	defer p.l.RUnlock()
	if p.heap.Length() == 0 {
		return nil, time.Time{}
	}
	nextPlan := p.heap.Peek()
	if nextPlan == nil {
		return nil, time.Time{}
	}
	return nextPlan.plan, nextPlan.next
}

func (p *PeriodicDispatch) Add(plan *domain.Plan) error {
	p.l.Lock()
	defer p.l.Unlock()

	// Do nothing if not enabled
	if !p.enabled {
		return nil
	}
	p.logger.Debug("add plan to storage", zap.Reflect("plan", plan))
	// If we were tracking a Plan and it has been disabled, made non-periodic,
	// stopped or is parameterized, remove it
	disabled := !plan.IsPeriodicActive()

	tuple := domain.NamespacedID{
		ID:        plan.ID,
		Namespace: plan.Namespace,
	}
	_, tracked := p.tracked[tuple]
	if disabled {
		if tracked {
			p.removeLocked(tuple)
		}
		// If the Plan is disabled and we aren't tracking it, do nothing.
		return nil
	}

	// Add or update the Plan.
	p.tracked[tuple] = plan
	next, err := plan.Periodic.Next(time.Now().In(plan.Periodic.GetLocation()))
	if err != nil {
		return fmt.Errorf("failed adding Plan %s: %v", plan.NamespacedID(), err)
	}
	if tracked {
		if err := p.heap.Update(plan, next); err != nil {
			return fmt.Errorf("failed to update Plan %q (%s) launch time: %v", plan.ID, plan.Namespace, err)
		}
		p.logger.Debug("updated periodic Plan", zap.String("Plan", plan.NamespacedID().String()))
	} else {
		if err := p.heap.Push(plan, next); err != nil {
			return fmt.Errorf("failed to add Plan %v: %v", plan.ID, err)
		}
		p.logger.Debug("registered periodic Plan", zap.String("Plan", plan.NamespacedID().String()))
	}

	// Signal an update.
	select {
	case p.updateCh <- struct{}{}:
	default:
	}
	return nil
}

func (p *PeriodicDispatch) Remove(namespace, planID string) error {
	p.l.Lock()
	defer p.l.Unlock()
	return p.removeLocked(domain.NamespacedID{
		ID:        planID,
		Namespace: namespace,
	})
}

func (p *PeriodicDispatch) removeLocked(planID domain.NamespacedID) error {
	// Do nothing if not enabled
	if !p.enabled {
		return nil
	}

	plan, tracked := p.tracked[planID]
	if !tracked {
		return nil
	}

	delete(p.tracked, planID)
	if err := p.heap.Remove(plan); err != nil {
		return fmt.Errorf("failed to remove tracked Plan %q (%s): %v", planID.ID, planID.Namespace, err)
	}

	// Signal an update.
	select {
	case p.updateCh <- struct{}{}:
	default:
	}
	p.logger.Debug("deregistered periodic Plan", zap.String("Plan", plan.NamespacedID().String()))
	return nil
}

func (p *PeriodicDispatch) flush() {
	p.updateCh = make(chan struct{}, 1)
	p.tracked = make(map[domain.NamespacedID]*domain.Plan)
	p.heap = NewPeriodicHeap()
	p.stopFn = nil
}
