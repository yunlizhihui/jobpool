package etcdserver

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"strings"
	"sync"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/schedulepb"
	clientv3 "yunli.com/jobpool/client/v2"
	ulid "yunli.com/jobpool/pkg/v2/ulid"
	"yunli.com/jobpool/server/v2/scheduler"
)

type WorkerStatus int

const (
	WorkerUnknownStatus WorkerStatus = iota // Unknown
	WorkerStarting
	WorkerStarted
	WorkerPausing
	WorkerPaused
	WorkerResuming
	WorkerStopping
	WorkerStopped
)

type SchedulerWorkerStatus int

const (
	WorkloadUnknownStatus SchedulerWorkerStatus = iota
	WorkloadRunning
	WorkloadWaitingToDequeue
	WorkloadWaitingForRaft
	WorkloadScheduling
	WorkloadSubmitting
	WorkloadBackoff
	WorkloadStopped
	WorkloadPaused
)

const (
	// backoffBaselineFast is the baseline time for exponential backoff
	backoffBaselineFast = 20 * time.Millisecond

	// backoffBaselineSlow is the baseline time for exponential backoff
	// but that is much slower than backoffBaselineFast
	backoffBaselineSlow = 500 * time.Millisecond

	// backoffLimitSlow is the limit of the exponential backoff for
	// the slower backoff
	backoffLimitSlow = 10 * time.Second

	// backoffSchedulerVersionMismatch is the backoff between retries when the
	// scheduler version mismatches that of the leader.
	backoffSchedulerVersionMismatch = 30 * time.Second

	// dequeueTimeout is used to timeout an evaluation dequeue so that
	// we can check if there is a shutdown event
	dequeueTimeout = 100 * time.Millisecond

	// raftSyncLimit is the limit of time we will wait for Raft replication
	// to catch up to the evaluation. This is used to fast Nack and
	// allow another scheduler to pick it up.
	raftSyncLimit = 5 * time.Second

	// dequeueErrGrace is the grace period where we don't log about
	// dequeue errors after start. This is to improve the user experience
	// in dev mode where the leader isn't elected for a few seconds.
	dequeueErrGrace = 10 * time.Second
)

type Worker struct {
	srv            *EtcdServer
	client         *clientv3.Client
	logger         *zap.Logger
	start          time.Time
	id             string
	status         WorkerStatus
	statusLock     sync.Mutex
	workloadStatus SchedulerWorkerStatus

	pauseFlag bool
	pauseLock sync.Mutex
	pauseCond *sync.Cond
	ctx       context.Context
	cancelFn  context.CancelFunc

	enabledSchedulers []string

	failures      uint
	evalToken     string
	snapshotIndex uint64
}

func NewWorker(ctx context.Context, srv *EtcdServer) (*Worker, error) {
	w := newWorker(ctx, srv)
	w.Start()
	return w, nil
}

func newWorker(ctx context.Context, srv *EtcdServer) *Worker {
	w := &Worker{
		id:                ulid.Generate(),
		srv:               srv,
		start:             time.Now(),
		status:            WorkerStarting,
		enabledSchedulers: []string{constant.PlanTypeService, constant.PlanTypeCore},
	}

	w.logger = srv.Logger()
	w.pauseCond = sync.NewCond(&w.pauseLock)
	w.ctx, w.cancelFn = context.WithCancel(ctx)
	// init the client for worker
	cfg := clientv3.Config{
		Endpoints:        w.srv.Cfg.ClientURLs.StringSlice(),
		AutoSyncInterval: 0,
		DialTimeout:      5 * time.Second,
	}
	w.logger.Debug("the config", zap.Strings("end", cfg.Endpoints))
	client, err := clientv3.New(cfg)
	if err != nil {
		w.logger.Warn("init client in worker error", zap.Error(err))
	}
	w.client = client
	return w
}

func (w *Worker) Start() {
	w.setStatus(WorkerStarting)
	go w.run()
}

func (w *Worker) run() {
	// worker running
	w.logger.Debug("----- the worker is running", zap.String("id", w.ID()))
	defer func() {
		w.markStopped()
	}()
	w.setStatuses(WorkerStarted, WorkloadRunning)
	retryTimes := 1
	select {
	case <-w.srv.ReadyNotify():
		w.logger.Info("---------the server raft is ready----------")
	}
	for {
		w.logger.Debug("in worker-------")
		if !w.srv.cluster.IsReadyToAddVotingMember() {
			time.Sleep(5 * time.Second)
			retryTimes++
			if retryTimes%25 == 0 {
				w.logger.Info("the cluster not ready", zap.Int("retry times", retryTimes))
			}
			continue
		}
		leader := w.srv.cluster.Member(w.srv.Leader())
		if leader == nil {
			time.Sleep(5 * time.Second)
			retryTimes++
			if retryTimes%25 == 0 {
				w.logger.Info("the cluster leader not ready", zap.Int("retry times", retryTimes))
			}
			continue
		}

		// Check to see if the context has been cancelled. Server shutdown and Shutdown()
		// should do this.
		if w.workerShuttingDown() {
			return
		}
		// Dequeue a pending evaluation
		eval, token, waitIndex, shutdown := w.dequeueEvaluation(dequeueTimeout)
		if shutdown {
			return
		}

		w.logger.Debug("eval", zap.Reflect("eval", eval))
		w.logger.Debug("token", zap.String("token", token), zap.Uint64("waitIndex", waitIndex))
		// 下面的这个目前没用，因为dequeue没有返回raft的index
		//w.setWorkloadStatus(WorkloadWaitingForRaft)
		//stats, err := w.client.Status(w.ctx, w.srv.Cfg.ClientURLs.StringSlice()[0])
		//if err != nil {
		//	w.logger.Error("error waiting for Raft index", zap.Error(err))
		//	w.client.EvalNack(eval, token)
		//	continue
		//}
		w.setWorkloadStatus(WorkloadScheduling)
		if err := w.invokeScheduler(eval, token); err != nil {
			w.logger.Error("error invoking scheduler", zap.Error(err))
			w.sendNack(eval, token)
			continue
		}
		// Complete the evaluation
		w.sendAck(eval, token)
	}
}

func (w *Worker) setStatus(newStatus WorkerStatus) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.setWorkerStatusLocked(newStatus)
}

func (w *Worker) setWorkerStatusLocked(newStatus WorkerStatus) {
	if newStatus == w.status {
		return
	}
	w.status = newStatus
}

func (s *EtcdServer) setupWorkers(ctx context.Context) error {
	s.workerLock.Lock()
	defer s.workerLock.Unlock()
	return s.setupWorkersLocked(ctx)
}
func (s *EtcdServer) setupWorkersLocked(ctx context.Context) error {
	s.Logger().Info("starting scheduling worker(s)", zap.Int("num_workers", s.Cfg.WorkerNumber), zap.String("schedulers", "service"))
	// Start the workers
	for i := 0; i < s.Cfg.WorkerNumber; i++ {
		if w, err := NewWorker(ctx, s); err != nil {
			return err
		} else {
			s.Logger().Debug("started scheduling worker", zap.String("id", w.ID()), zap.Int("index", i+1), zap.String("of", "service"))
			s.workers = append(s.workers, w)
		}
	}
	s.Logger().Info("started scheduling worker(s)", zap.Int("num_workers", s.Cfg.WorkerNumber), zap.Bool("schedulers", true))
	return nil
}

func (w *Worker) ID() string {
	return w.id
}

func (w *Worker) markStopped() {
	w.setStatuses(WorkerStopped, WorkloadStopped)
	w.logger.Debug("stopped")
}
func (w *Worker) setStatuses(newWorkerStatus WorkerStatus, newWorkloadStatus SchedulerWorkerStatus) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.setWorkerStatusLocked(newWorkerStatus)
	w.setWorkloadStatusLocked(newWorkloadStatus)
}

func (w *Worker) setWorkloadStatusLocked(newStatus SchedulerWorkerStatus) {
	if newStatus == w.workloadStatus {
		return
	}
	w.logger.Debug("changed workload status", zap.Reflect("from", w.workloadStatus), zap.Reflect("to", newStatus))
	w.workloadStatus = newStatus
}

func (w *Worker) workerShuttingDown() bool {
	select {
	case <-w.ctx.Done():
		return true
	default:
		return false
	}
}

func (w *Worker) setWorkloadStatus(newStatus SchedulerWorkerStatus) {
	w.statusLock.Lock()
	defer w.statusLock.Unlock()
	w.setWorkloadStatusLocked(newStatus)
}

func (w *Worker) maybeWait() {
	w.pauseLock.Lock()
	defer w.pauseLock.Unlock()

	if !w.pauseFlag {
		return
	}

	w.statusLock.Lock()
	w.status = WorkerPaused
	originalWorkloadStatus := w.workloadStatus
	w.workloadStatus = WorkloadPaused
	w.logger.Debug("changed workload status", zap.String("from", fmt.Sprintf("%v", originalWorkloadStatus)), zap.Reflect("to", w.workloadStatus))

	w.statusLock.Unlock()

	for w.pauseFlag {
		w.pauseCond.Wait()
	}

	w.statusLock.Lock()
	w.logger.Debug("changed workload status", zap.String("from", fmt.Sprintf("%v", w.workloadStatus)), zap.Reflect("to", originalWorkloadStatus))
	w.workloadStatus = originalWorkloadStatus

	// only reset the worker status if the worker is not resuming to stop the paused workload.
	if w.status != WorkerStopping {
		w.logger.Debug("changed workload status", zap.String("from", fmt.Sprintf("%v", w.status)), zap.Reflect("to", WorkerStarted))
		w.status = WorkerStarted
	}
	w.statusLock.Unlock()
}

func (w *Worker) dequeueEvaluation(timeout time.Duration) (
	eval *domain.Evaluation, token string, waitIndex uint64, shutdown bool) {
	// Setup the request
REQ:
	// Wait inside this function if the worker is paused.
	w.maybeWait()
	// Immediately check to see if the worker has been shutdown.
	if w.workerShuttingDown() {
		return nil, "", 0, true
	}

	// start := time.Now()
	w.setWorkloadStatus(WorkloadWaitingToDequeue)

	// 转发到leader节点，并从其队列中获取
	w.logger.Debug("---start dequeue of eval in server---",
		zap.Duration("timeout", timeout),
		zap.Strings("schedulers", w.enabledSchedulers))

	r, err := w.srv.EvalDequeue(w.ctx, &etcdserverpb.ScheduleEvalDequeueRequest{
		Timeout:    uint64(timeout.Milliseconds()),
		Schedulers: w.enabledSchedulers,
	})
	// r, err := w.client.Schedule.EvalDequeue(w.ctx, 1*1000*1000000, w.enabledSchedulers)
	if err != nil {
		w.logger.Warn("eval dequeue error", zap.Error(err))
		if time.Since(w.start) > dequeueErrGrace && !w.workerShuttingDown() {
			if w.shouldResubmit(err) {
				w.logger.Debug("failed to dequeue evaluation", zap.Error(err))
			} else {
				w.logger.Error("failed to dequeue evaluation", zap.Error(err))
			}
		}
		// Adjust the backoff based on the error. If it is a scheduler version
		// mismatch we increase the baseline.
		base, limit := backoffBaselineFast, backoffLimitSlow
		if strings.Contains(err.Error(), "calling scheduler version") {
			base = backoffSchedulerVersionMismatch
			limit = backoffSchedulerVersionMismatch
		}
		if w.backoffErr(base, limit) {
			return nil, "", 0, true
		}
		goto REQ
	}
	if r != nil {
		w.logger.Debug("the token in response", zap.String("token", r.Token))
		if r.Data != nil {
			resp := r.Data
			w.logger.Debug("-x-x-x-x-x-the res", zap.String("resp", resp.Id))
			w.logger.Debug("-x-x-x-xx-x-x-x-the eval in dequeue", zap.Reflect("eval", r.Data))
		} else {
			w.logger.Debug("-x-x-x-x-x-the res is nil :(")
		}
	}
	w.backoffReset()
	if r != nil && r.Data != nil {
		w.logger.Debug("------the plan item is---", zap.String("eval-id", r.Data.Id), zap.String("plan-id", r.Data.PlanId))
		ev := convertEvalFromEvalucation(r.Data)
		return ev, r.Token, 0, false
	}
	// TODO to be delete in future
	time.Sleep(10 * time.Millisecond)
	goto REQ
}

func (w *Worker) shouldResubmit(err error) bool {
	s := err.Error()
	switch {
	case strings.Contains(s, "No cluster leader"):
		return true
	case strings.Contains(s, "PlanAllocation queue is disabled"):
		return true
	default:
		return false
	}
}

func (w *Worker) backoffErr(base, limit time.Duration) bool {
	w.setWorkloadStatus(WorkloadBackoff)
	backoff := (1 << (2 * w.failures)) * base
	if backoff > limit {
		backoff = limit
	} else {
		w.failures++
	}
	select {
	case <-time.After(backoff):
		return false
	case <-w.ctx.Done():
		return true
	}
}
func (w *Worker) backoffReset() {
	w.failures = 0
}

func (w *Worker) sendNack(eval *domain.Evaluation, token string) {
	w.sendAcknowledgement(eval, token, false)
}

// sendAck makes a best effort to ack the evaluation.
// Any errors are logged but swallowed.
func (w *Worker) sendAck(eval *domain.Evaluation, token string) {
	w.sendAcknowledgement(eval, token, true)
}
func (w *Worker) sendAcknowledgement(eval *domain.Evaluation, token string, ack bool) {
	if ack {
		_, err := w.srv.EvalAck(w.ctx, &etcdserverpb.ScheduleEvalAckRequest{
			Id:        eval.ID,
			JobId:     eval.JobID,
			Token:     token,
			Namespace: eval.Namespace,
		})
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to %s evaluation", "ack"), zap.String("eval_id", eval.ID), zap.Error(err))
		} else {
			w.logger.Debug(fmt.Sprintf("%s evaluation", "ack"), zap.String("eval_id", eval.ID), zap.String("type", eval.Type), zap.String("namespace", eval.Namespace), zap.String("plan_id", eval.PlanID), zap.String("node_id", eval.NodeID), zap.String("triggered_by", eval.TriggeredBy))
		}
	} else {
		_, err := w.srv.EvalNack(w.ctx, &etcdserverpb.ScheduleEvalNackRequest{
			Id:        eval.ID,
			JobId:     eval.JobID,
			Token:     token,
			Namespace: eval.Namespace,
		})
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to %s evaluation", "nack"), zap.String("eval_id", eval.ID), zap.Error(err))
		} else {
			w.logger.Debug(fmt.Sprintf("%s evaluation", "nack"), zap.String("eval_id", eval.ID), zap.String("type", eval.Type), zap.String("namespace", eval.Namespace), zap.String("plan_id", eval.PlanID), zap.String("node_id", eval.NodeID), zap.String("triggered_by", eval.TriggeredBy))
		}
	}
}

func (w *Worker) invokeScheduler(eval *domain.Evaluation, token string) error {
	w.evalToken = token
	// Store the snapshot's index
	var err error
	// Create the scheduler, or use the special core scheduler
	var sched scheduler.Scheduler
	if eval.Type == constant.PlanTypeCore {
		// finish GC and something
		sched = NewCoreScheduler(w.srv, w.logger)
	} else {
		// TODO 按照type 创建scheduler
		sched, err = scheduler.NewScheduler(constant.PlanTypeService, w.logger, w.srv.ScheduleStore(), w)
		if err != nil {
			return fmt.Errorf("failed to instantiate scheduler: %v", err)
		}
	}

	if eval.JobID == "" {
		return nil
	}

	// Process the evaluation
	err = sched.Process(eval)
	if err != nil {
		return fmt.Errorf("failed to process evaluation: %v", err)
	}
	return nil
}

// implement the interface for scheduler
func (w *Worker) UpdateEval(eval *domain.Evaluation) error {
	req := &etcdserverpb.ScheduleEvalStatusUpdateRequest{
		Id:                eval.ID,
		Status:            eval.Status,
		StatusDescription: eval.StatusDescription,
		NextEval:          eval.NextEval,
		BlockedEval:       eval.BlockedEval,
		EvalToken:         w.evalToken,
	}
SUBMIT:
	_, err := w.client.EvalUpdate(w.ctx, req)
	if err != nil {
		w.logger.Error("failed to update evaluation", zap.Reflect("eval", eval), zap.Error(err))
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debug("updated evaluation", zap.Reflect("eval", eval))
		w.backoffReset()
	}
	return nil
}

func (w *Worker) CreateEval(eval *domain.Evaluation) error {
	req := &etcdserverpb.ScheduleEvalAddRequest{
		Id:           eval.ID,
		Namespace:    eval.Namespace,
		EvalToken:    w.evalToken,
		Priority:     uint64(eval.Priority),
		Type:         eval.Type,
		TriggeredBy:  eval.TriggeredBy,
		JobId:        eval.JobID,
		PlanId:       eval.PlanID,
		Status:       eval.Status,
		PreviousEval: eval.PreviousEval,
	}
SUBMIT:
	_, err := w.client.EvalCreate(w.ctx, req)
	if err != nil {
		w.logger.Error("failed to add evaluation", zap.Reflect("eval", eval), zap.Error(err))
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return err
	} else {
		w.logger.Debug("create evaluation", zap.Reflect("eval", eval))
		w.backoffReset()
	}
	return nil
}

func (w *Worker) SubmitPlan(alloc *domain.PlanAlloc) (*domain.PlanResult, error) {
	req := &etcdserverpb.PlanAllocationEnqueueRequest{
		EvalId:    alloc.EvalID,
		EvalToken: w.evalToken,
		Priority:  int64(alloc.Priority),
		AllAtOnce: alloc.AllAtOnce,
		PlanId:    alloc.Plan.ID,
		JobId:     alloc.JobId,
	}

	if len(alloc.NodeAllocation) > 0 {
		var allocations []*schedulepb.Allocation
		for _, list := range alloc.NodeAllocation {
			for _, item := range list {
				allocations = append(allocations, domain.ConvertAllocationToPb(item))
			}
		}
		req.Allocations = allocations
	}
SUBMIT:
	response, err := w.srv.PlanAllocationEnqueue(w.ctx, req)
	if err != nil {
		w.logger.Error("failed to add plan allocation", zap.Error(err))
		if w.shouldResubmit(err) && !w.backoffErr(backoffBaselineSlow, backoffLimitSlow) {
			goto SUBMIT
		}
		return nil, err
	} else {
		w.logger.Debug("create plan allocation")
		w.backoffReset()
	}

	allocs := make(map[string][]*domain.Allocation)
	if response.Data != nil && len(response.Data) > 0 {
		for _, item := range response.Data {
			nodeId := item.NodeId
			existing := allocs[nodeId]
			allocs[nodeId] = append(existing, domain.ConvertAllocation(item))
		}
	}
	result := &domain.PlanResult{
		NodeAllocation: allocs,
	}
	return result, nil
}

func (w *Worker) ReblockEval(evaluation *domain.Evaluation) error {
	// TODO
	return nil
}

func (w *Worker) RunningSlotLeft(namespace string, runningAllocations int) bool {
	slotCount := 100
	if w.srv.Cfg.JobParallelismLimit > 0 {
		slotCount = w.srv.Cfg.JobParallelismLimit
	}
	if slotCount-runningAllocations <= 0 {
		return false
	}
	return true
}
