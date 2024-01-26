package etcdserver

import (
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"runtime"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
	ulid "yunli.com/jobpool/pkg/v2/ulid"
	"yunli.com/jobpool/server/v2/scheduler"
)

type allocator struct {
	*EtcdServer
	logger          *zap.Logger
	allocationQueue *scheduler.AllocationQueue
}

// newAllocator 创建新的分配器
func newAllocator(s *EtcdServer) (*allocator, error) {
	allocationQueue, err := scheduler.NewAllocationQueue()
	if err != nil {
		return nil, err
	}
	return &allocator{
		EtcdServer:      s,
		logger:          s.Logger(),
		allocationQueue: allocationQueue,
	}, nil
}

func (p *allocator) planApply(stopCh chan struct{}) {
	select {
	case <-p.EtcdServer.ReadyNotify():
		p.logger.Debug("plan apply can be run")
	}
	p.logger.Debug("start apply plan in leader node")
	var planIndexCh chan uint64
	var prevPlanResultIndex uint64
	poolSize := runtime.NumCPU() / 2
	if poolSize == 0 {
		poolSize = 1
	}
	pool := scheduler.NewEvaluatePool(poolSize, 64, p)
	defer pool.Shutdown()
	for {
		pending, err := p.allocationQueue.Dequeue(0)
		if err != nil {
			return
		}
		p.logger.Debug("get pending plan from plan Queue", zap.Reflect("pending plan", pending))
		select {
		case <-stopCh:
			return
		case idx := <-planIndexCh:
			// Previous plan committed. Discard snapshot and ensure
			// future snapshots include this plan. idx may be 0 if
			// plan failed to apply, so use max(prev, idx)
			prevPlanResultIndex = max(prevPlanResultIndex, idx)
			planIndexCh = nil
			// snap = nil
		default:
		}

		/*
			if snap != nil {
				// If snapshot doesn't contain the previous plan
				// result's index and the current plan's snapshot it,
				// discard it and get a new one below.
				minIndex := max(prevPlanResultIndex, pending.PlanAllocation.SnapshotIndex)
				if idx, err := snap.LatestIndex(); err != nil || idx < minIndex {
					snap = nil
				}
			}
			if planIndexCh == nil || snap == nil {
				snap, err = p.snapshotMinIndex(prevPlanResultIndex, pending.PlanAllocation.SnapshotIndex)
				if err != nil {
					p.logger.Error("failed to snapshot state", zap.Error(err))
					pending.Respond(nil, err)
					continue
				}
			}
		*/ // Evaluate the plan
		result, err := p.evaluatePlan(pool, pending.PlanAllocation, p.logger)
		if err != nil {
			p.logger.Error("failed to evaluate plan", zap.Error(err))
			pending.Respond(nil, err)
			continue
		}

		p.logger.Debug("the result from plan", zap.Reflect("result", result))
		// Fast-path the response if there is nothing to do
		if result.IsNoOp() {
			pending.Respond(result, nil)
			continue
		}

		// Ensure any parallel apply is complete before starting the next one.
		// This also limits how out of date our snapshot can be.
		// if planIndexCh != nil {
		// idx := <-planIndexCh
		// prevPlanResultIndex = max(prevPlanResultIndex, idx)
		//snap, err = p.snapshotMinIndex(prevPlanResultIndex, pending.PlanAllocation.SnapshotIndex)
		//if err != nil {
		//	p.logger.Error("failed to update snapshot state", zap.Error(err))
		//	pending.Respond(nil, err)
		//	continue
		//}
		// }

		// Dispatch the Raft transaction for the plan
		if err = p.applyPlanAllocation(pending.PlanAllocation, result); err != nil {
			p.logger.Error("failed to submit plan", zap.Error(err))
			pending.Respond(nil, err)
			continue
		}

		// Respond to the plan in async; receive plan's committed index via chan
		planIndexCh = make(chan uint64, 1)
		go p.asyncPlanWait(planIndexCh, result, pending)

	}
}

func (p *allocator) asyncPlanWait(indexCh chan<- uint64,
	result *domain.PlanResult, pending *scheduler.PendingAllocation) {
	// Respond to the plan
	index := p.committedIndex
	pending.Respond(result, nil)
	indexCh <- index
}

func (p *allocator) EvaluateNodePlan(planAllocation *domain.PlanAlloc, nodeID string) (bool, string, error) {
	if len(planAllocation.NodeAllocation[nodeID]) == 0 {
		return true, "", nil
	}

	// Get the node itself
	nodeResponse, err := p.EtcdServer.NodeDetail(p.ctx, &pb.NodeDetailRequest{Id: nodeID})
	if err != nil {
		return false, "", fmt.Errorf("failed to get node '%s': %v", nodeID, err)
	}
	pbNode := nodeResponse.Data
	node := domain.ConvertNodeFromPb(pbNode)
	//  节点不存在则出错
	if node == nil {
		return false, "node does not exist", nil
	} else if node.Status == constant.NodeStatusDisconnected {
		if isNodeValidForAllocate(planAllocation, node.ID) {
			return true, "", nil
		}
		return false, "node is disconnected and contains invalid updates", nil
	} else if node.Status != constant.NodeStatusReady {
		return false, "node is not ready for placements", nil
	}

	// Get the existing allocations that are non-terminal
	allocationResp, err := p.EtcdServer.AllocationList(p.ctx, &pb.ScheduleAllocationListRequest{NodeId: nodeID})
	if err != nil {
		return false, "", fmt.Errorf("failed to get existing allocations for '%s': %v", nodeID, err)
	}
	var existingAlloc []*domain.Allocation
	for _, item := range allocationResp.Data {
		existingAlloc = append(existingAlloc, domain.ConvertAllocation(item))
	}

	// If nodeAllocations is a subset of the existing allocations we can continue,
	// even if the node is not eligible, as only in-place updates or stop/evict are performed
	if domain.AllocSubset(existingAlloc, planAllocation.NodeAllocation[nodeID]) {
		return true, "", nil
	}
	if node.SchedulingEligibility == constant.NodeSchedulingIneligible {
		return false, "node is not eligible", nil
	}

	// Determine the proposed allocation by first removing allocations
	// that are planned evictions and adding the new allocations.
	var remove []*domain.Allocation
	if update := planAllocation.NodeUpdate[nodeID]; len(update) > 0 {
		remove = append(remove, update...)
	}

	// Remove any preempted allocs
	if preempted := planAllocation.NodePreemptions[nodeID]; len(preempted) > 0 {
		remove = append(remove, preempted...)
	}

	if updated := planAllocation.NodeAllocation[nodeID]; len(updated) > 0 {
		remove = append(remove, updated...)
	}
	proposed := domain.RemoveAllocs(existingAlloc, remove)
	proposed = append(proposed, planAllocation.NodeAllocation[nodeID]...)

	// Check if these allocations fit
	fit, reason, err := domain.AllocsFit(node, proposed, true)
	return fit, reason, err
}

func (p *allocator) evaluatePlan(pool *scheduler.EvaluatePool, planAllocation *domain.PlanAlloc, logger *zap.Logger) (*domain.PlanResult, error) {
	result := &domain.PlanResult{
		NodeUpdate:      make(map[string][]*domain.Allocation),
		NodeAllocation:  make(map[string][]*domain.Allocation),
		NodePreemptions: make(map[string][]*domain.Allocation),
	}
	nodeIDs := make(map[string]struct{})
	nodeIDList := make([]string, 0, len(planAllocation.NodeUpdate)+len(planAllocation.NodeAllocation))
	for nodeID := range planAllocation.NodeUpdate {
		if _, ok := nodeIDs[nodeID]; !ok {
			nodeIDs[nodeID] = struct{}{}
			nodeIDList = append(nodeIDList, nodeID)
		}
	}
	for nodeID := range planAllocation.NodeAllocation {
		if _, ok := nodeIDs[nodeID]; !ok {
			nodeIDs[nodeID] = struct{}{}
			nodeIDList = append(nodeIDList, nodeID)
		}
	}

	// Setup a multierror to handle potentially getting many
	// errors since we are processing in parallel.
	var mErr []error
	// handleResult is used to process the result of evaluateNodePlan
	handleResult := func(nodeID string, fit bool, reason string, err error) (cancel bool) {
		// Evaluate the planAllocation for this node
		if err != nil {
			mErr = append(mErr, err)
			return true
		}
		if !fit {
			// Log the reason why the node's allocations could not be made
			if reason != "" {
				ns := ""
				if planAllocation.Plan != nil {
					ns = planAllocation.Plan.Namespace
				}
				logger.Warn("planAllocation for node rejected",
					zap.String("node_id", nodeID),
					zap.String("reason", reason),
					zap.String("eval_id", planAllocation.EvalID),
					zap.String("namespace", ns))
			}

			// If we require all-at-once scheduling, there is no point
			// to continue the evaluation, as we've already failed.
			if planAllocation.AllAtOnce {
				result.NodeUpdate = nil
				result.NodeAllocation = nil
				result.NodePreemptions = nil
				return true
			}
			// Skip this node, since it cannot be used.
			return
		}

		// Add this to the planAllocation result
		if nodeUpdate := planAllocation.NodeUpdate[nodeID]; len(nodeUpdate) > 0 {
			result.NodeUpdate[nodeID] = nodeUpdate
		}
		if nodeAlloc := planAllocation.NodeAllocation[nodeID]; len(nodeAlloc) > 0 {
			result.NodeAllocation[nodeID] = nodeAlloc
		}

		if nodePreemptions := planAllocation.NodePreemptions[nodeID]; nodePreemptions != nil {
			// Do a pass over preempted allocs in the planAllocation to check
			// whether the alloc is already in a terminal state
			var filteredNodePreemptions []*domain.Allocation
			for _, preemptedAlloc := range nodePreemptions {
				allocResp, err := p.EtcdServer.AllocationDetail(p.ctx, &pb.ScheduleAllocationDetailRequest{Id: preemptedAlloc.ID})
				if err != nil {
					mErr = append(mErr, err)
					continue
				}
				alloc := domain.ConvertAllocation(allocResp.Data)
				if alloc != nil && !alloc.TerminalStatus() {
					filteredNodePreemptions = append(filteredNodePreemptions, preemptedAlloc)
				}
			}

			result.NodePreemptions[nodeID] = filteredNodePreemptions
		}

		return
	}

	// Get the pool channels
	req := pool.RequestCh()
	resp := pool.ResultCh()
	outstanding := 0
	didCancel := false

	// Evaluate each node in the planAllocation, handling results as they are ready to
	// avoid blocking.
OUTER:
	for len(nodeIDList) > 0 {
		nodeID := nodeIDList[0]
		select {
		case req <- scheduler.EvaluateRequest{planAllocation, nodeID}:
			outstanding++
			nodeIDList = nodeIDList[1:]
		case r := <-resp:
			outstanding--

			// Handle a result that allows us to cancel evaluation,
			// which may save time processing additional entries.
			if cancel := handleResult(r.NodeID, r.Fit, r.Reason, r.Err); cancel {
				didCancel = true
				break OUTER
			}
		}
	}
	// Drain the remaining results
	for outstanding > 0 {
		r := <-resp
		if !didCancel {
			if cancel := handleResult(r.NodeID, r.Fit, r.Reason, r.Err); cancel {
				didCancel = true
			}
		}
		outstanding--
	}
	var errResult error
	if mErr != nil && len(mErr) > 0 {
		errResult = mErr[0]
		logger.Debug("the error for return is ", zap.Error(errResult))
	} else {
		logger.Debug("finish evaluate planAllocation success")
	}
	return result, errResult
}

// applyPlanAllocation is used to apply the allocation result and to return the alloc index
func (p *allocator) applyPlanAllocation(planAlloc *domain.PlanAlloc, result *domain.PlanResult) error {
	// Setup the update request
	req := ApplyPlanResultsRequest{
		AllocUpdateRequest: AllocUpdateRequest{
			Plan: planAlloc.Plan,
		},
		EvalID: planAlloc.EvalID,
	}

	preemptedJobIDs := make(map[domain.NamespacedID]struct{})
	now := xtime.NewFormatTime(time.Now())
	req.AllocsStopped = make([]*domain.AllocationDiff, 0, len(result.NodeUpdate))
	req.AllocsUpdated = make([]*domain.Allocation, 0, len(result.NodeAllocation))
	req.AllocsPreempted = make([]*domain.AllocationDiff, 0, len(result.NodePreemptions))

	for _, updateList := range result.NodeUpdate {
		for _, stoppedAlloc := range updateList {
			req.AllocsStopped = append(req.AllocsStopped, normalizeStoppedAlloc(stoppedAlloc, now))
		}
	}

	for _, allocList := range result.NodeAllocation {
		req.AllocsUpdated = append(req.AllocsUpdated, allocList...)
	}

	// Set the time the alloc was applied for the first time. This can be used
	// to approximate the scheduling time.
	updateAllocTimestamps(req.AllocsUpdated, now)

	for _, preemptions := range result.NodePreemptions {
		for _, preemptedAlloc := range preemptions {
			req.AllocsPreempted = append(req.AllocsPreempted, normalizePreemptedAlloc(preemptedAlloc, now))
			// Gather jobids to create follow up evals
			appendNamespacedJobID(preemptedJobIDs, preemptedAlloc)
		}
	}

	var evals []*schedulepb.Evaluation
	for preemptedJobID := range preemptedJobIDs {
		planDetail, _ := p.EtcdServer.PlanDetail(p.ctx, &pb.SchedulePlanDetailRequest{Id: preemptedJobID.ID})
		// plan, _ := p.ScheduleRepository().PlanByID(nil, preemptedJobID.Namespace, preemptedJobID.ID)
		if planDetail != nil && planDetail.Data != nil {
			plan := planDetail.Data
			eval := &schedulepb.Evaluation{
				Id:          ulid.Generate(),
				Namespace:   plan.Namespace,
				TriggeredBy: constant.EvalTriggerPreemption,
				PlanId:      plan.Id,
				Type:        plan.Type,
				Priority:    plan.Priority,
				Status:      constant.EvalStatusPending,
				CreateTime:  timestamppb.TimestampNow(),
				UpdateTime:  timestamppb.TimestampNow(),
			}
			evals = append(evals, eval)
		}
	}

	planReq := &pb.ScheduleAllocationAddRequest{
		EvaluationPreemption: evals,
	}
	if req.AllocsUpdated != nil && len(req.AllocsUpdated) > 0 {
		var allocationUpdated []*schedulepb.Allocation
		for _, allocation := range req.AllocsUpdated {
			p.logger.Debug("the allocation update is", zap.Reflect("allo", allocation))
			allocationUpdated = append(allocationUpdated, domain.ConvertAllocationToPb(allocation))
		}
		p.logger.Debug("the size of allo updated", zap.Int("size", len(allocationUpdated)))
		planReq.AllocationUpdated = allocationUpdated
	}
	if req.AllocsStopped != nil && len(req.AllocsStopped) > 0 {
		allocations := make([]*schedulepb.Allocation, len(req.AllocsStopped))
		for _, allocation := range req.AllocsStopped {
			allocations = append(allocations, convertAllocationDiffToPb(allocation))
		}
		planReq.AllocationStopped = allocations
	}
	if req.AllocsPreempted != nil && len(req.AllocsPreempted) > 0 {
		allocations := make([]*schedulepb.Allocation, len(req.AllocsPreempted))
		for _, allocation := range req.AllocsPreempted {
			allocations = append(allocations, convertAllocationDiffToPb(allocation))
		}
		planReq.AllocationPreempted = allocations
	}
	// Dispatch the Raft transaction
	_, err := p.EtcdServer.AllocationAdd(p.ctx, planReq)
	if err != nil {
		return err
	}
	return nil
}

func convertAllocationDiffToPb(alloc *domain.AllocationDiff) *schedulepb.Allocation {
	return &schedulepb.Allocation{
		Id:                alloc.ID,
		ClientStatus:      alloc.ClientStatus,
		ClientDescription: alloc.ClientDescription,
		PlanId:            alloc.PlanID,
	}
}

func normalizeStoppedAlloc(stoppedAlloc *domain.Allocation, now xtime.FormatTime) *domain.AllocationDiff {
	return &domain.AllocationDiff{
		ID:                 stoppedAlloc.ID,
		DesiredDescription: stoppedAlloc.DesiredDescription,
		ClientStatus:       stoppedAlloc.ClientStatus,
		PlanID:             stoppedAlloc.PlanID,
		ModifyTime:         now,
		FollowupEvalID:     stoppedAlloc.FollowupEvalID,
	}
}

func normalizePreemptedAlloc(preemptedAlloc *domain.Allocation, now xtime.FormatTime) *domain.AllocationDiff {
	return &domain.AllocationDiff{
		ID:                    preemptedAlloc.ID,
		PreemptedByAllocation: preemptedAlloc.PreemptedByAllocation,
		ModifyTime:            now,
		PlanID:                preemptedAlloc.PlanID,
	}
}
func updateAllocTimestamps(allocations []*domain.Allocation, timestamp xtime.FormatTime) {
	for _, alloc := range allocations {
		if alloc.CreateTime == "" {
			alloc.CreateTime = timestamp
		}
		alloc.ModifyTime = timestamp
	}
}

func appendNamespacedJobID(jobIDs map[domain.NamespacedID]struct{}, alloc *domain.Allocation) {
	id := domain.NamespacedID{Namespace: alloc.Namespace, ID: alloc.PlanID}
	if _, ok := jobIDs[id]; !ok {
		jobIDs[id] = struct{}{}
	}
}

// isNodeValidForAllocate 节点是否适合分配任务
func isNodeValidForAllocate(planAllocation *domain.PlanAlloc, nodeID string) bool {
	for _, alloc := range planAllocation.NodeAllocation[nodeID] {
		if alloc.ClientStatus != constant.AllocClientStatusUnknown {
			return false
		}
	}
	return true
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

type ApplyPlanResultsRequest struct {
	AllocUpdateRequest

	// EvalID is the eval ID of the plan being applied
	EvalID string

	NodePreemptions []*domain.Allocation

	// AllocsPreempted is a slice of allocation diffs from other lower priority jobs
	// that are preempted. Preempted allocations are marked as evicted.
	AllocsPreempted []*domain.AllocationDiff

	// PreemptionEvals is a slice of follow up evals for jobs whose allocations
	// have been preempted to place allocs in this plan
	PreemptionEvals []*domain.Evaluation
}

type AllocUpdateRequest struct {
	Alloc []*domain.Allocation
	// Allocations to stop. Contains only the diff, not the entire allocation
	AllocsStopped []*domain.AllocationDiff

	// New or updated allocations
	AllocsUpdated []*domain.Allocation

	// Evals is the list of new evaluations to create
	// Evals are valid only when used in the Raft RPC
	Evals []*domain.Evaluation

	// Plan is the shared parent plan of the allocations.
	// It is pulled out since it is common to reduce payload size.
	Plan *domain.Plan
}
