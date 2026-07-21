// Copyright 2016 The etcd Authors
//
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

package etcdserver

import (
	"bytes"
	"context"
	"fmt"
	"github.com/coreos/go-semver/semver"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"sort"
	"strconv"
	"strings"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/helper/ulid"
	"yunli.com/jobpool/api/v2/membershippb"
	"yunli.com/jobpool/api/v2/mvccpb"
	"yunli.com/jobpool/api/v2/schedulepb"
	"yunli.com/jobpool/client/pkg/v2/types"
	"yunli.com/jobpool/pkg/v2/traceutil"
	"yunli.com/jobpool/server/v2/auth"
	"yunli.com/jobpool/server/v2/etcdserver/api"
	"yunli.com/jobpool/server/v2/etcdserver/api/membership"
	"yunli.com/jobpool/server/v2/lease"
	"yunli.com/jobpool/server/v2/mvcc"
)

const (
	v3Version = "v3"
)

type applyResult struct {
	resp proto.Message
	err  error
	// physc signals the physical effect of the request has completed in addition
	// to being logically reflected by the node. Currently only used for
	// Compaction requests.
	physc <-chan struct{}
	trace *traceutil.Trace
}

// applierV3Internal is the interface for processing internal V3 raft request
type applierV3Internal interface {
	ClusterVersionSet(r *membershippb.ClusterVersionSetRequest, shouldApplyV3 membership.ShouldApplyV3)
	ClusterMemberAttrSet(r *membershippb.ClusterMemberAttrSetRequest, shouldApplyV3 membership.ShouldApplyV3)
	DowngradeInfoSet(r *membershippb.DowngradeInfoSetRequest, shouldApplyV3 membership.ShouldApplyV3)
}

// applierV3 is the interface for processing V3 raft messages
type applierV3 interface {
	Apply(r *pb.InternalRaftRequest, shouldApplyV3 membership.ShouldApplyV3) *applyResult

	Put(ctx context.Context, txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error)
	Range(ctx context.Context, txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error)
	DeleteRange(txn mvcc.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(ctx context.Context, rt *pb.TxnRequest) (*pb.TxnResponse, *traceutil.Trace, error)
	Compaction(compaction *pb.CompactionRequest) (*pb.CompactionResponse, <-chan struct{}, *traceutil.Trace, error)

	LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error)
	LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error)

	LeaseCheckpoint(lc *pb.LeaseCheckpointRequest) (*pb.LeaseCheckpointResponse, error)

	Alarm(*pb.AlarmRequest) (*pb.AlarmResponse, error)

	Authenticate(r *pb.InternalAuthenticateRequest) (*pb.AuthenticateResponse, error)

	AuthEnable() (*pb.AuthEnableResponse, error)
	AuthDisable() (*pb.AuthDisableResponse, error)
	AuthStatus() (*pb.AuthStatusResponse, error)

	UserAdd(ua *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ua *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ua *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ua *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ua *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ua *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ua *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ua *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ua *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ua *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ua *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ua *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ua *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
	// scheduler start
	PlanList(ua *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error)
	PlanAdd(ua *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error)
	PlanUpdate(ua *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error)
	PlanDelete(ua *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error)
	PlanDetail(ua *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error)
	PlanOnline(ua *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error)
	PlanOffline(ua *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error)
	NameSpaceAdd(ua *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error)
	NameSpaceList(ua *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error)
	NameSpaceDelete(ua *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error)
	NameSpaceDetail(ua *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error)
	NameSpaceUpdate(ua *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error)
	JobAdd(ua *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error)
	JobList(ua *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error)
	JobExist(ua *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error)
	JobUpdate(ua *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error)
	JobDelete(ua *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error)
	JobDetail(ua *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error)
	EvalAdd(ua *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error)
	EvalList(ua *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error)
	EvalUpdate(ua *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error)
	EvalDelete(ua *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error)
	EvalDetail(ua *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error)
	AllocationAdd(ua *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error)
	AllocationList(ua *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error)
	SimpleAllocationList(ua *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error)
	AllocationUpdate(ua *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error)
	AllocationDelete(ua *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error)
	AllocationDetail(ua *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error)
	// nodes
	NodeAdd(ua *pb.NodeAddRequest) (*pb.NodeAddResponse, error)
	NodeList(ua *pb.NodeListRequest) (*pb.NodeListResponse, error)
	NodeUpdate(ua *pb.NodeUpdateRequest) (*pb.NodeUpdateResponse, error)
	NodeDelete(ua *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error)
	NodeDetail(ua *pb.NodeDetailRequest) (*pb.NodeDetailResponse, error)
	// queue and something
	PlanAllocationEnqueue(ua *pb.PlanAllocationEnqueueRequest) (*pb.PlanAllocationEnqueueResponse, error)
}

type checkReqFunc func(mvcc.ReadView, *pb.RequestOp) error

type applierV3backend struct {
	s *EtcdServer

	checkPut   checkReqFunc
	checkRange checkReqFunc
}

func (s *EtcdServer) newApplierV3Backend() applierV3 {
	base := &applierV3backend{s: s}
	base.checkPut = func(rv mvcc.ReadView, req *pb.RequestOp) error {
		return base.checkRequestPut(rv, req)
	}
	base.checkRange = func(rv mvcc.ReadView, req *pb.RequestOp) error {
		return base.checkRequestRange(rv, req)
	}
	return base
}

func (s *EtcdServer) newApplierV3Internal() applierV3Internal {
	base := &applierV3backend{s: s}
	return base
}

func (s *EtcdServer) newApplierV3() applierV3 {
	return newAuthApplierV3(
		s.AuthStore(),
		newQuotaApplierV3(s, s.newApplierV3Backend()),
		s.lessor,
	)
}

func (a *applierV3backend) Apply(r *pb.InternalRaftRequest, shouldApplyV3 membership.ShouldApplyV3) *applyResult {
	op := "unknown"
	ar := &applyResult{}
	defer func(start time.Time) {
		success := ar.err == nil || ar.err == mvcc.ErrCompacted
		applySec.WithLabelValues(v3Version, op, strconv.FormatBool(success)).Observe(time.Since(start).Seconds())
		warnOfExpensiveRequest(a.s.Logger(), a.s.Cfg.WarningApplyDuration, start, &pb.InternalRaftStringer{Request: r}, ar.resp, ar.err)
		if !success {
			warnOfFailedRequest(a.s.Logger(), start, &pb.InternalRaftStringer{Request: r}, ar.resp, ar.err)
		}
	}(time.Now())

	switch {
	case r.ClusterVersionSet != nil: // Implemented in 3.5.x
		op = "ClusterVersionSet"
		a.s.applyV3Internal.ClusterVersionSet(r.ClusterVersionSet, shouldApplyV3)
		return nil
	case r.ClusterMemberAttrSet != nil:
		op = "ClusterMemberAttrSet" // Implemented in 3.5.x
		a.s.applyV3Internal.ClusterMemberAttrSet(r.ClusterMemberAttrSet, shouldApplyV3)
		return nil
	case r.DowngradeInfoSet != nil:
		op = "DowngradeInfoSet" // Implemented in 3.5.x
		a.s.applyV3Internal.DowngradeInfoSet(r.DowngradeInfoSet, shouldApplyV3)
		return nil
	}

	if !shouldApplyV3 {
		return nil
	}

	// call into a.s.applyV3.F instead of a.F so upper appliers can check individual calls
	switch {
	case r.Range != nil:
		op = "Range"
		ar.resp, ar.err = a.s.applyV3.Range(context.TODO(), nil, r.Range)
	case r.Put != nil:
		op = "Put"
		ar.resp, ar.trace, ar.err = a.s.applyV3.Put(context.TODO(), nil, r.Put)
	case r.DeleteRange != nil:
		op = "DeleteRange"
		ar.resp, ar.err = a.s.applyV3.DeleteRange(nil, r.DeleteRange)
	case r.Txn != nil:
		op = "Txn"
		ar.resp, ar.trace, ar.err = a.s.applyV3.Txn(context.TODO(), r.Txn)
	case r.Compaction != nil:
		op = "Compaction"
		ar.resp, ar.physc, ar.trace, ar.err = a.s.applyV3.Compaction(r.Compaction)
	case r.LeaseGrant != nil:
		op = "LeaseGrant"
		ar.resp, ar.err = a.s.applyV3.LeaseGrant(r.LeaseGrant)
	case r.LeaseRevoke != nil:
		op = "LeaseRevoke"
		ar.resp, ar.err = a.s.applyV3.LeaseRevoke(r.LeaseRevoke)
	case r.LeaseCheckpoint != nil:
		op = "LeaseCheckpoint"
		ar.resp, ar.err = a.s.applyV3.LeaseCheckpoint(r.LeaseCheckpoint)
	case r.Alarm != nil:
		op = "Alarm"
		ar.resp, ar.err = a.s.applyV3.Alarm(r.Alarm)
	case r.Authenticate != nil:
		op = "Authenticate"
		ar.resp, ar.err = a.s.applyV3.Authenticate(r.Authenticate)
	case r.AuthEnable != nil:
		op = "AuthEnable"
		ar.resp, ar.err = a.s.applyV3.AuthEnable()
	case r.AuthDisable != nil:
		op = "AuthDisable"
		ar.resp, ar.err = a.s.applyV3.AuthDisable()
	case r.AuthStatus != nil:
		ar.resp, ar.err = a.s.applyV3.AuthStatus()
	case r.AuthUserAdd != nil:
		op = "AuthUserAdd"
		ar.resp, ar.err = a.s.applyV3.UserAdd(r.AuthUserAdd)
	case r.AuthUserDelete != nil:
		op = "AuthUserDelete"
		ar.resp, ar.err = a.s.applyV3.UserDelete(r.AuthUserDelete)
	case r.AuthUserChangePassword != nil:
		op = "AuthUserChangePassword"
		ar.resp, ar.err = a.s.applyV3.UserChangePassword(r.AuthUserChangePassword)
	case r.AuthUserGrantRole != nil:
		op = "AuthUserGrantRole"
		ar.resp, ar.err = a.s.applyV3.UserGrantRole(r.AuthUserGrantRole)
	case r.AuthUserGet != nil:
		op = "AuthUserGet"
		ar.resp, ar.err = a.s.applyV3.UserGet(r.AuthUserGet)
	case r.AuthUserRevokeRole != nil:
		op = "AuthUserRevokeRole"
		ar.resp, ar.err = a.s.applyV3.UserRevokeRole(r.AuthUserRevokeRole)
	case r.AuthRoleAdd != nil:
		op = "AuthRoleAdd"
		ar.resp, ar.err = a.s.applyV3.RoleAdd(r.AuthRoleAdd)
	case r.AuthRoleGrantPermission != nil:
		op = "AuthRoleGrantPermission"
		ar.resp, ar.err = a.s.applyV3.RoleGrantPermission(r.AuthRoleGrantPermission)
	case r.AuthRoleGet != nil:
		op = "AuthRoleGet"
		ar.resp, ar.err = a.s.applyV3.RoleGet(r.AuthRoleGet)
	case r.AuthRoleRevokePermission != nil:
		op = "AuthRoleRevokePermission"
		ar.resp, ar.err = a.s.applyV3.RoleRevokePermission(r.AuthRoleRevokePermission)
	case r.AuthRoleDelete != nil:
		op = "AuthRoleDelete"
		ar.resp, ar.err = a.s.applyV3.RoleDelete(r.AuthRoleDelete)
	case r.AuthUserList != nil:
		op = "AuthUserList"
		ar.resp, ar.err = a.s.applyV3.UserList(r.AuthUserList)
	case r.AuthRoleList != nil:
		op = "AuthRoleList"
		ar.resp, ar.err = a.s.applyV3.RoleList(r.AuthRoleList)
		// the scheduler business
	case r.SchedulePlanList != nil:
		op = "SchedulePlanList"
		ar.resp, ar.err = a.s.applyV3.PlanList(r.SchedulePlanList)
	case r.SchedulePlanAdd != nil:
		op = "SchedulePlanAdd"
		ar.resp, ar.err = a.s.applyV3.PlanAdd(r.SchedulePlanAdd)
	case r.SchedulePlanUpdate != nil:
		op = "SchedulePlanUpdate"
		ar.resp, ar.err = a.s.applyV3.PlanUpdate(r.SchedulePlanUpdate)
	case r.SchedulePlanDelete != nil:
		op = "SchedulePlanDelete"
		ar.resp, ar.err = a.s.applyV3.PlanDelete(r.SchedulePlanDelete)
	case r.SchedulePlanDetail != nil:
		op = "SchedulePlanDetail"
		ar.resp, ar.err = a.s.applyV3.PlanDetail(r.SchedulePlanDetail)
	case r.SchedulePlanOnline != nil:
		op = "SchedulePlanOnline"
		ar.resp, ar.err = a.s.applyV3.PlanOnline(r.SchedulePlanOnline)
	case r.SchedulePlanOffline != nil:
		op = "SchedulePlanOffline"
		ar.resp, ar.err = a.s.applyV3.PlanOffline(r.SchedulePlanOffline)
		// namespaces
	case r.ScheduleNamespaceAdd != nil:
		op = "ScheduleNamespaceAdd"
		ar.resp, ar.err = a.s.applyV3.NameSpaceAdd(r.ScheduleNamespaceAdd)
	case r.ScheduleNamespaceList != nil:
		op = "ScheduleNamespaceList"
		ar.resp, ar.err = a.s.applyV3.NameSpaceList(r.ScheduleNamespaceList)
	case r.ScheduleNamespaceDelete != nil:
		op = "ScheduleNamespaceDelete"
		ar.resp, ar.err = a.s.applyV3.NameSpaceDelete(r.ScheduleNamespaceDelete)
	case r.ScheduleNamespaceDetail != nil:
		op = "ScheduleNamespaceDetail"
		ar.resp, ar.err = a.s.applyV3.NameSpaceDetail(r.ScheduleNamespaceDetail)
	case r.ScheduleNamespaceUpdate != nil:
		op = "ScheduleNamespaceUpdate"
		ar.resp, ar.err = a.s.applyV3.NameSpaceUpdate(r.ScheduleNamespaceUpdate)
		// jobs
	case r.ScheduleJobList != nil:
		op = "ScheduleJobList"
		ar.resp, ar.err = a.s.applyV3.JobList(r.ScheduleJobList)
	case r.ScheduleJobAdd != nil:
		op = "ScheduleJobAdd"
		ar.resp, ar.err = a.s.applyV3.JobAdd(r.ScheduleJobAdd)
	case r.ScheduleJobStatusUpdate != nil:
		op = "ScheduleJobStatusUpdate"
		ar.resp, ar.err = a.s.applyV3.JobUpdate(r.ScheduleJobStatusUpdate)
	case r.ScheduleJobDelete != nil:
		op = "ScheduleJobDelete"
		ar.resp, ar.err = a.s.applyV3.JobDelete(r.ScheduleJobDelete)
	case r.ScheduleJobDetail != nil:
		op = "ScheduleJobDetail"
		ar.resp, ar.err = a.s.applyV3.JobDetail(r.ScheduleJobDetail)
	case r.ScheduleJobExist != nil:
		op = "ScheduleJobExist"
		ar.resp, ar.err = a.s.applyV3.JobExist(r.ScheduleJobExist)
		// evals
	case r.ScheduleEvalList != nil:
		op = "ScheduleEvalList"
		ar.resp, ar.err = a.s.applyV3.EvalList(r.ScheduleEvalList)
	case r.ScheduleEvalAdd != nil:
		op = "ScheduleEvalAdd"
		ar.resp, ar.err = a.s.applyV3.EvalAdd(r.ScheduleEvalAdd)
	case r.ScheduleEvalStatusUpdate != nil:
		op = "ScheduleEvalStatusUpdate"
		ar.resp, ar.err = a.s.applyV3.EvalUpdate(r.ScheduleEvalStatusUpdate)
	case r.ScheduleEvalDelete != nil:
		op = "ScheduleEvalDelete"
		ar.resp, ar.err = a.s.applyV3.EvalDelete(r.ScheduleEvalDelete)
	case r.ScheduleEvalDetail != nil:
		op = "ScheduleEvalDetail"
		ar.resp, ar.err = a.s.applyV3.EvalDetail(r.ScheduleEvalDetail)
		// allocations
	case r.ScheduleAllocationList != nil:
		op = "ScheduleAllocationList"
		ar.resp, ar.err = a.s.applyV3.AllocationList(r.ScheduleAllocationList)
	case r.ScheduleAllocationAdd != nil:
		op = "ScheduleAllocationAdd"
		ar.resp, ar.err = a.s.applyV3.AllocationAdd(r.ScheduleAllocationAdd)
	case r.ScheduleAllocationStatusUpdate != nil:
		op = "ScheduleAllocationStatusUpdate"
		ar.resp, ar.err = a.s.applyV3.AllocationUpdate(r.ScheduleAllocationStatusUpdate)
	case r.ScheduleAllocationDelete != nil:
		op = "ScheduleAllocationDelete"
		ar.resp, ar.err = a.s.applyV3.AllocationDelete(r.ScheduleAllocationDelete)
	case r.ScheduleAllocationDetail != nil:
		op = "ScheduleAllocationDetail"
		ar.resp, ar.err = a.s.applyV3.AllocationDetail(r.ScheduleAllocationDetail)
		//simple
	case r.ScheduleSimpleAllocationList != nil:
		op = "ScheduleSimpleAllocationList"
		ar.resp, ar.err = a.s.applyV3.SimpleAllocationList(r.ScheduleSimpleAllocationList)
		// nodes
	case r.NodeList != nil:
		op = "NodeList"
		ar.resp, ar.err = a.s.applyV3.NodeList(r.NodeList)
	case r.NodeAdd != nil:
		op = "NodeAdd"
		ar.resp, ar.err = a.s.applyV3.NodeAdd(r.NodeAdd)
	case r.NodeUpdate != nil:
		op = "NodeUpdate"
		ar.resp, ar.err = a.s.applyV3.NodeUpdate(r.NodeUpdate)
	case r.NodeDelete != nil:
		op = "NodeDelete"
		ar.resp, ar.err = a.s.applyV3.NodeDelete(r.NodeDelete)
	case r.NodeDetail != nil:
		op = "NodeDetail"
		ar.resp, ar.err = a.s.applyV3.NodeDetail(r.NodeDetail)
	// others
	case r.ScheduleEvalDequeue != nil:
		op = "ScheduleEvalDequeue"
		// not support the scheduleEvalDequeue
		a.s.lg.Warn("not support eval dequeue in apply method")
	case r.PlanAllocationEnqueue != nil:
		op = "PlanAllocationEnqueue"
		ar.resp, ar.err = a.s.applyV3.PlanAllocationEnqueue(r.PlanAllocationEnqueue)

	default:
		a.s.lg.Panic("not implemented apply", zap.Stringer("raft-request", r))
	}
	return ar
}

func (a *applierV3backend) Put(ctx context.Context, txn mvcc.TxnWrite, p *pb.PutRequest) (resp *pb.PutResponse, trace *traceutil.Trace, err error) {
	resp = &pb.PutResponse{}
	resp.Header = &pb.ResponseHeader{}
	trace = traceutil.Get(ctx)
	// create put tracing if the trace in context is empty
	if trace.IsEmpty() {
		trace = traceutil.New("put",
			a.s.Logger(),
			traceutil.Field{Key: "key", Value: string(p.Key)},
			traceutil.Field{Key: "req_size", Value: p.Size()},
		)
	}
	val, leaseID := p.Value, lease.LeaseID(p.Lease)
	if txn == nil {
		if leaseID != lease.NoLease {
			if l := a.s.lessor.Lookup(leaseID); l == nil {
				return nil, nil, lease.ErrLeaseNotFound
			}
		}
		txn = a.s.KV().Write(trace)
		defer txn.End()
	}

	var rr *mvcc.RangeResult
	if p.IgnoreValue || p.IgnoreLease || p.PrevKv {
		trace.StepWithFunction(func() {
			rr, err = txn.Range(context.TODO(), p.Key, nil, mvcc.RangeOptions{})
		}, "get previous kv pair")

		if err != nil {
			return nil, nil, err
		}
	}
	if p.IgnoreValue || p.IgnoreLease {
		if rr == nil || len(rr.KVs) == 0 {
			// ignore_{lease,value} flag expects previous key-value pair
			return nil, nil, ErrKeyNotFound
		}
	}
	if p.IgnoreValue {
		val = rr.KVs[0].Value
	}
	if p.IgnoreLease {
		leaseID = lease.LeaseID(rr.KVs[0].Lease)
	}
	if p.PrevKv {
		if rr != nil && len(rr.KVs) != 0 {
			resp.PrevKv = &rr.KVs[0]
		}
	}

	resp.Header.Revision = txn.Put(p.Key, val, leaseID)
	trace.AddField(traceutil.Field{Key: "response_revision", Value: resp.Header.Revision})
	return resp, trace, nil
}

func (a *applierV3backend) DeleteRange(txn mvcc.TxnWrite, dr *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp := &pb.DeleteRangeResponse{}
	resp.Header = &pb.ResponseHeader{}
	end := mkGteRange(dr.RangeEnd)

	if txn == nil {
		txn = a.s.kv.Write(traceutil.TODO())
		defer txn.End()
	}

	if dr.PrevKv {
		rr, err := txn.Range(context.TODO(), dr.Key, end, mvcc.RangeOptions{})
		if err != nil {
			return nil, err
		}
		if rr != nil {
			resp.PrevKvs = make([]*mvccpb.KeyValue, len(rr.KVs))
			for i := range rr.KVs {
				resp.PrevKvs[i] = &rr.KVs[i]
			}
		}
	}

	resp.Deleted, resp.Header.Revision = txn.DeleteRange(dr.Key, end)
	return resp, nil
}

func (a *applierV3backend) Range(ctx context.Context, txn mvcc.TxnRead, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	trace := traceutil.Get(ctx)

	resp := &pb.RangeResponse{}
	resp.Header = &pb.ResponseHeader{}

	if txn == nil {
		txn = a.s.kv.Read(mvcc.ConcurrentReadTxMode, trace)
		defer txn.End()
	}

	limit := r.Limit
	if r.SortOrder != pb.RangeRequest_NONE ||
		r.MinModRevision != 0 || r.MaxModRevision != 0 ||
		r.MinCreateRevision != 0 || r.MaxCreateRevision != 0 {
		// fetch everything; sort and truncate afterwards
		limit = 0
	}
	if limit > 0 {
		// fetch one extra for 'more' flag
		limit = limit + 1
	}

	ro := mvcc.RangeOptions{
		Limit: limit,
		Rev:   r.Revision,
		Count: r.CountOnly,
	}

	rr, err := txn.Range(ctx, r.Key, mkGteRange(r.RangeEnd), ro)
	if err != nil {
		return nil, err
	}

	if r.MaxModRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.ModRevision > r.MaxModRevision }
		pruneKVs(rr, f)
	}
	if r.MinModRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.ModRevision < r.MinModRevision }
		pruneKVs(rr, f)
	}
	if r.MaxCreateRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.CreateRevision > r.MaxCreateRevision }
		pruneKVs(rr, f)
	}
	if r.MinCreateRevision != 0 {
		f := func(kv *mvccpb.KeyValue) bool { return kv.CreateRevision < r.MinCreateRevision }
		pruneKVs(rr, f)
	}

	sortOrder := r.SortOrder
	if r.SortTarget != pb.RangeRequest_KEY && sortOrder == pb.RangeRequest_NONE {
		// Since current mvcc.Range implementation returns results
		// sorted by keys in lexiographically ascending order,
		// sort ASCEND by default only when target is not 'KEY'
		sortOrder = pb.RangeRequest_ASCEND
	}
	if sortOrder != pb.RangeRequest_NONE {
		var sorter sort.Interface
		switch {
		case r.SortTarget == pb.RangeRequest_KEY:
			sorter = &kvSortByKey{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VERSION:
			sorter = &kvSortByVersion{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_CREATE:
			sorter = &kvSortByCreate{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_MOD:
			sorter = &kvSortByMod{&kvSort{rr.KVs}}
		case r.SortTarget == pb.RangeRequest_VALUE:
			sorter = &kvSortByValue{&kvSort{rr.KVs}}
		}
		switch {
		case sortOrder == pb.RangeRequest_ASCEND:
			sort.Sort(sorter)
		case sortOrder == pb.RangeRequest_DESCEND:
			sort.Sort(sort.Reverse(sorter))
		}
	}

	if r.Limit > 0 && len(rr.KVs) > int(r.Limit) {
		rr.KVs = rr.KVs[:r.Limit]
		resp.More = true
	}
	trace.Step("filter and sort the key-value pairs")
	resp.Header.Revision = rr.Rev
	resp.Count = int64(rr.Count)
	resp.Kvs = make([]*mvccpb.KeyValue, len(rr.KVs))
	for i := range rr.KVs {
		if r.KeysOnly {
			rr.KVs[i].Value = nil
		}
		resp.Kvs[i] = &rr.KVs[i]
	}
	trace.Step("assemble the response")
	return resp, nil
}

func (a *applierV3backend) Txn(ctx context.Context, rt *pb.TxnRequest) (*pb.TxnResponse, *traceutil.Trace, error) {
	lg := a.s.Logger()
	trace := traceutil.Get(ctx)
	if trace.IsEmpty() {
		trace = traceutil.New("transaction", a.s.Logger())
		ctx = context.WithValue(ctx, traceutil.TraceKey, trace)
	}
	isWrite := !isTxnReadonly(rt)

	// When the transaction contains write operations, we use ReadTx instead of
	// ConcurrentReadTx to avoid extra overhead of copying buffer.
	var txn mvcc.TxnWrite
	if isWrite && a.s.Cfg.ExperimentalTxnModeWriteWithSharedBuffer {
		txn = mvcc.NewReadOnlyTxnWrite(a.s.KV().Read(mvcc.SharedBufReadTxMode, trace))
	} else {
		txn = mvcc.NewReadOnlyTxnWrite(a.s.KV().Read(mvcc.ConcurrentReadTxMode, trace))
	}

	var txnPath []bool
	trace.StepWithFunction(
		func() {
			txnPath = compareToPath(txn, rt)
		},
		"compare",
	)

	if isWrite {
		trace.AddField(traceutil.Field{Key: "read_only", Value: false})
		if _, err := checkRequests(txn, rt, txnPath, a.checkPut); err != nil {
			txn.End()
			return nil, nil, err
		}
	}
	if _, err := checkRequests(txn, rt, txnPath, a.checkRange); err != nil {
		txn.End()
		return nil, nil, err
	}
	trace.Step("check requests")
	txnResp, _ := newTxnResp(rt, txnPath)

	// When executing mutable txn ops, etcd must hold the txn lock so
	// readers do not see any intermediate results. Since writes are
	// serialized on the raft loop, the revision in the read view will
	// be the revision of the write txn.
	if isWrite {
		txn.End()
		txn = a.s.KV().Write(trace)
	}
	_, err := a.applyTxn(ctx, txn, rt, txnPath, txnResp)
	if err != nil {
		if isWrite {
			// end txn to release locks before panic
			txn.End()
			// When txn with write operations starts it has to be successful
			// We don't have a way to recover state in case of write failure
			lg.Panic("unexpected error during txn with writes", zap.Error(err))
		} else {
			lg.Error("unexpected error during readonly txn", zap.Error(err))
		}
	}
	rev := txn.Rev()
	if len(txn.Changes()) != 0 {
		rev++
	}
	txn.End()

	txnResp.Header.Revision = rev
	trace.AddField(
		traceutil.Field{Key: "number_of_response", Value: len(txnResp.Responses)},
		traceutil.Field{Key: "response_revision", Value: txnResp.Header.Revision},
	)
	return txnResp, trace, err
}

// newTxnResp allocates a txn response for a txn request given a path.
func newTxnResp(rt *pb.TxnRequest, txnPath []bool) (txnResp *pb.TxnResponse, txnCount int) {
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	resps := make([]*pb.ResponseOp, len(reqs))
	txnResp = &pb.TxnResponse{
		Responses: resps,
		Succeeded: txnPath[0],
		Header:    &pb.ResponseHeader{},
	}
	for i, req := range reqs {
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseRange{}}
		case *pb.RequestOp_RequestPut:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponsePut{}}
		case *pb.RequestOp_RequestDeleteRange:
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseDeleteRange{}}
		case *pb.RequestOp_RequestTxn:
			resp, txns := newTxnResp(tv.RequestTxn, txnPath[1:])
			resps[i] = &pb.ResponseOp{Response: &pb.ResponseOp_ResponseTxn{ResponseTxn: resp}}
			txnPath = txnPath[1+txns:]
			txnCount += txns + 1
		default:
		}
	}
	return txnResp, txnCount
}

func compareToPath(rv mvcc.ReadView, rt *pb.TxnRequest) []bool {
	txnPath := make([]bool, 1)
	ops := rt.Success
	if txnPath[0] = applyCompares(rv, rt.Compare); !txnPath[0] {
		ops = rt.Failure
	}
	for _, op := range ops {
		tv, ok := op.Request.(*pb.RequestOp_RequestTxn)
		if !ok || tv.RequestTxn == nil {
			continue
		}
		txnPath = append(txnPath, compareToPath(rv, tv.RequestTxn)...)
	}
	return txnPath
}

func applyCompares(rv mvcc.ReadView, cmps []*pb.Compare) bool {
	for _, c := range cmps {
		if !applyCompare(rv, c) {
			return false
		}
	}
	return true
}

// applyCompare applies the compare request.
// If the comparison succeeds, it returns true. Otherwise, returns false.
func applyCompare(rv mvcc.ReadView, c *pb.Compare) bool {
	// TODO: possible optimizations
	// * chunk reads for large ranges to conserve memory
	// * rewrite rules for common patterns:
	//	ex. "[a, b) createrev > 0" => "limit 1 /\ kvs > 0"
	// * caching
	rr, err := rv.Range(context.TODO(), c.Key, mkGteRange(c.RangeEnd), mvcc.RangeOptions{})
	if err != nil {
		return false
	}
	if len(rr.KVs) == 0 {
		if c.Target == pb.Compare_VALUE {
			// Always fail if comparing a value on a key/keys that doesn't exist;
			// nil == empty string in grpc; no way to represent missing value
			return false
		}
		return compareKV(c, mvccpb.KeyValue{})
	}
	for _, kv := range rr.KVs {
		if !compareKV(c, kv) {
			return false
		}
	}
	return true
}

func compareKV(c *pb.Compare, ckv mvccpb.KeyValue) bool {
	var result int
	rev := int64(0)
	switch c.Target {
	case pb.Compare_VALUE:
		v := []byte{}
		if tv, _ := c.TargetUnion.(*pb.Compare_Value); tv != nil {
			v = tv.Value
		}
		result = bytes.Compare(ckv.Value, v)
	case pb.Compare_CREATE:
		if tv, _ := c.TargetUnion.(*pb.Compare_CreateRevision); tv != nil {
			rev = tv.CreateRevision
		}
		result = compareInt64(ckv.CreateRevision, rev)
	case pb.Compare_MOD:
		if tv, _ := c.TargetUnion.(*pb.Compare_ModRevision); tv != nil {
			rev = tv.ModRevision
		}
		result = compareInt64(ckv.ModRevision, rev)
	case pb.Compare_VERSION:
		if tv, _ := c.TargetUnion.(*pb.Compare_Version); tv != nil {
			rev = tv.Version
		}
		result = compareInt64(ckv.Version, rev)
	case pb.Compare_LEASE:
		if tv, _ := c.TargetUnion.(*pb.Compare_Lease); tv != nil {
			rev = tv.Lease
		}
		result = compareInt64(ckv.Lease, rev)
	}
	switch c.Result {
	case pb.Compare_EQUAL:
		return result == 0
	case pb.Compare_NOT_EQUAL:
		return result != 0
	case pb.Compare_GREATER:
		return result > 0
	case pb.Compare_LESS:
		return result < 0
	}
	return true
}

func (a *applierV3backend) applyTxn(ctx context.Context, txn mvcc.TxnWrite, rt *pb.TxnRequest, txnPath []bool, tresp *pb.TxnResponse) (txns int, err error) {
	trace := traceutil.Get(ctx)
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}

	for i, req := range reqs {
		respi := tresp.Responses[i].Response
		switch tv := req.Request.(type) {
		case *pb.RequestOp_RequestRange:
			trace.StartSubTrace(
				traceutil.Field{Key: "req_type", Value: "range"},
				traceutil.Field{Key: "range_begin", Value: string(tv.RequestRange.Key)},
				traceutil.Field{Key: "range_end", Value: string(tv.RequestRange.RangeEnd)})
			resp, err := a.Range(ctx, txn, tv.RequestRange)
			if err != nil {
				return 0, fmt.Errorf("applyTxn: failed Range: %w", err)
			}
			respi.(*pb.ResponseOp_ResponseRange).ResponseRange = resp
			trace.StopSubTrace()
		case *pb.RequestOp_RequestPut:
			trace.StartSubTrace(
				traceutil.Field{Key: "req_type", Value: "put"},
				traceutil.Field{Key: "key", Value: string(tv.RequestPut.Key)},
				traceutil.Field{Key: "req_size", Value: tv.RequestPut.Size()})
			resp, _, err := a.Put(ctx, txn, tv.RequestPut)
			if err != nil {
				return 0, fmt.Errorf("applyTxn: failed Put: %w", err)
			}
			respi.(*pb.ResponseOp_ResponsePut).ResponsePut = resp
			trace.StopSubTrace()
		case *pb.RequestOp_RequestDeleteRange:
			resp, err := a.DeleteRange(txn, tv.RequestDeleteRange)
			if err != nil {
				return 0, fmt.Errorf("applyTxn: failed DeleteRange: %w", err)
			}
			respi.(*pb.ResponseOp_ResponseDeleteRange).ResponseDeleteRange = resp
		case *pb.RequestOp_RequestTxn:
			resp := respi.(*pb.ResponseOp_ResponseTxn).ResponseTxn
			applyTxns, err := a.applyTxn(ctx, txn, tv.RequestTxn, txnPath[1:], resp)
			if err != nil {
				// don't wrap the error. It's a recursive call and err should be already wrapped
				return 0, err
			}
			txns += applyTxns + 1
			txnPath = txnPath[applyTxns+1:]
		default:
			// empty union
		}
	}
	return txns, nil
}

func (a *applierV3backend) Compaction(compaction *pb.CompactionRequest) (*pb.CompactionResponse, <-chan struct{}, *traceutil.Trace, error) {
	resp := &pb.CompactionResponse{}
	resp.Header = &pb.ResponseHeader{}
	trace := traceutil.New("compact",
		a.s.Logger(),
		traceutil.Field{Key: "revision", Value: compaction.Revision},
	)

	ch, err := a.s.KV().Compact(trace, compaction.Revision)
	if err != nil {
		return nil, ch, nil, err
	}
	// get the current revision. which key to get is not important.
	rr, _ := a.s.KV().Range(context.TODO(), []byte("compaction"), nil, mvcc.RangeOptions{})
	resp.Header.Revision = rr.Rev
	return resp, ch, trace, err
}

func (a *applierV3backend) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	l, err := a.s.lessor.Grant(lease.LeaseID(lc.ID), lc.TTL)
	resp := &pb.LeaseGrantResponse{}
	if err == nil {
		resp.ID = int64(l.ID)
		resp.TTL = l.TTL()
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) LeaseRevoke(lc *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	err := a.s.lessor.Revoke(lease.LeaseID(lc.ID))
	return &pb.LeaseRevokeResponse{Header: newHeader(a.s)}, err
}

func (a *applierV3backend) LeaseCheckpoint(lc *pb.LeaseCheckpointRequest) (*pb.LeaseCheckpointResponse, error) {
	for _, c := range lc.Checkpoints {
		err := a.s.lessor.Checkpoint(lease.LeaseID(c.ID), c.Remaining_TTL)
		if err != nil {
			return &pb.LeaseCheckpointResponse{Header: newHeader(a.s)}, err
		}
	}
	return &pb.LeaseCheckpointResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) Alarm(ar *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp := &pb.AlarmResponse{}
	oldCount := len(a.s.alarmStore.Get(ar.Alarm))

	lg := a.s.Logger()
	switch ar.Action {
	case pb.AlarmRequest_GET:
		resp.Alarms = a.s.alarmStore.Get(ar.Alarm)
	case pb.AlarmRequest_ACTIVATE:
		if ar.Alarm == pb.AlarmType_NONE {
			break
		}
		m := a.s.alarmStore.Activate(types.ID(ar.MemberID), ar.Alarm)
		if m == nil {
			break
		}
		resp.Alarms = append(resp.Alarms, m)
		activated := oldCount == 0 && len(a.s.alarmStore.Get(m.Alarm)) == 1
		if !activated {
			break
		}

		lg.Warn("alarm raised", zap.String("alarm", m.Alarm.String()), zap.String("from", types.ID(m.MemberID).String()))
		switch m.Alarm {
		case pb.AlarmType_CORRUPT:
			a.s.applyV3 = newApplierV3Corrupt(a)
		case pb.AlarmType_NOSPACE:
			a.s.applyV3 = newApplierV3Capped(a)
		default:
			lg.Panic("unimplemented alarm activation", zap.String("alarm", fmt.Sprintf("%+v", m)))
		}
	case pb.AlarmRequest_DEACTIVATE:
		m := a.s.alarmStore.Deactivate(types.ID(ar.MemberID), ar.Alarm)
		if m == nil {
			break
		}
		resp.Alarms = append(resp.Alarms, m)
		deactivated := oldCount > 0 && len(a.s.alarmStore.Get(ar.Alarm)) == 0
		if !deactivated {
			break
		}

		switch m.Alarm {
		case pb.AlarmType_NOSPACE, pb.AlarmType_CORRUPT:
			// TODO: check kv hash before deactivating CORRUPT?
			lg.Warn("alarm disarmed", zap.String("alarm", m.Alarm.String()), zap.String("from", types.ID(m.MemberID).String()))
			a.s.applyV3 = a.s.newApplierV3()
		default:
			lg.Warn("unimplemented alarm deactivation", zap.String("alarm", fmt.Sprintf("%+v", m)))
		}
	default:
		return nil, nil
	}
	return resp, nil
}

type applierV3Capped struct {
	applierV3
	q backendQuota
}

// newApplierV3Capped creates an applyV3 that will reject Puts and transactions
// with Puts so that the number of keys in the store is capped.
func newApplierV3Capped(base applierV3) applierV3 { return &applierV3Capped{applierV3: base} }

func (a *applierV3Capped) Put(ctx context.Context, txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error) {
	return nil, nil, ErrNoSpace
}

func (a *applierV3Capped) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, *traceutil.Trace, error) {
	if a.q.Cost(r) > 0 {
		return nil, nil, ErrNoSpace
	}
	return a.applierV3.Txn(ctx, r)
}

func (a *applierV3Capped) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	return nil, ErrNoSpace
}

func (a *applierV3backend) AuthEnable() (*pb.AuthEnableResponse, error) {
	err := a.s.AuthStore().AuthEnable()
	if err != nil {
		return nil, err
	}
	return &pb.AuthEnableResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) AuthDisable() (*pb.AuthDisableResponse, error) {
	a.s.AuthStore().AuthDisable()
	return &pb.AuthDisableResponse{Header: newHeader(a.s)}, nil
}

func (a *applierV3backend) AuthStatus() (*pb.AuthStatusResponse, error) {
	enabled := a.s.AuthStore().IsAuthEnabled()
	authRevision := a.s.AuthStore().Revision()
	return &pb.AuthStatusResponse{Header: newHeader(a.s), Enabled: enabled, AuthRevision: authRevision}, nil
}

func (a *applierV3backend) Authenticate(r *pb.InternalAuthenticateRequest) (*pb.AuthenticateResponse, error) {
	ctx := context.WithValue(context.WithValue(a.s.ctx, auth.AuthenticateParamIndex{}, a.s.consistIndex.ConsistentIndex()), auth.AuthenticateParamSimpleTokenPrefix{}, r.SimpleToken)
	resp, err := a.s.AuthStore().Authenticate(ctx, r.Name, r.Password)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserAdd(r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	resp, err := a.s.AuthStore().UserAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserDelete(r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := a.s.AuthStore().UserDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserChangePassword(r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	resp, err := a.s.AuthStore().UserChangePassword(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserGrantRole(r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := a.s.AuthStore().UserGrantRole(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := a.s.AuthStore().UserGet(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserRevokeRole(r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := a.s.AuthStore().UserRevokeRole(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleAdd(r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := a.s.AuthStore().RoleAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleGrantPermission(r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := a.s.AuthStore().RoleGrantPermission(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := a.s.AuthStore().RoleGet(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleRevokePermission(r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := a.s.AuthStore().RoleRevokePermission(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleDelete(r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := a.s.AuthStore().RoleDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) UserList(r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := a.s.AuthStore().UserList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) RoleList(r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := a.s.AuthStore().RoleList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) ClusterVersionSet(r *membershippb.ClusterVersionSetRequest, shouldApplyV3 membership.ShouldApplyV3) {
	a.s.cluster.SetVersion(semver.Must(semver.NewVersion(r.Ver)), api.UpdateCapability, shouldApplyV3)
}

func (a *applierV3backend) ClusterMemberAttrSet(r *membershippb.ClusterMemberAttrSetRequest, shouldApplyV3 membership.ShouldApplyV3) {
	a.s.cluster.UpdateAttributes(
		types.ID(r.Member_ID),
		membership.Attributes{
			Name:       r.MemberAttributes.Name,
			ClientURLs: r.MemberAttributes.ClientUrls,
		},
		shouldApplyV3,
	)
}

func (a *applierV3backend) DowngradeInfoSet(r *membershippb.DowngradeInfoSetRequest, shouldApplyV3 membership.ShouldApplyV3) {
	d := membership.DowngradeInfo{Enabled: false}
	if r.Enabled {
		d = membership.DowngradeInfo{Enabled: true, TargetVersion: r.Ver}
	}
	a.s.cluster.SetDowngradeInfo(&d, shouldApplyV3)
}

func (a *applierV3backend) PlanList(r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error) {
	resp, err := a.s.ScheduleStore().PlanList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) PlanAdd(r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error) {
	resp, err := a.s.ScheduleStore().PlanAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	plan := domain.ConvertPlanFromRequest(r)
	if err := a.planStatusChanged(plan); err != nil {
		return nil, err
	}
	return resp, err
}

func (a *applierV3backend) PlanUpdate(r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error) {
	resp, err := a.s.ScheduleStore().PlanUpdate(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	plan := domain.ConvertPlan(resp.Data)
	if err = a.planStatusChanged(plan); err != nil {
		return nil, err
	}
	return resp, err
}

func (a *applierV3backend) PlanDelete(r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error) {
	resp, err := a.s.ScheduleStore().PlanDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) PlanDetail(r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := a.s.ScheduleStore().PlanDetail(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) PlanOnline(r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := a.s.ScheduleStore().PlanOnline(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	plan := domain.ConvertPlan(resp.Data)
	if err = a.planStatusChanged(plan); err != nil {
		return nil, err
	}
	return resp, err
}

func (a *applierV3backend) PlanOffline(r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := a.s.ScheduleStore().PlanOffline(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	plan := domain.ConvertPlan(resp.Data)
	if err = a.planStatusChanged(plan); err != nil {
		return nil, err
	}
	return resp, err
}

func (a *applierV3backend) planStatusChanged(plan *domain.Plan) error {
	if constant.PlanStatusRunning == plan.Status {
		if err := a.s.periodicDispatcher.Add(plan); err != nil {
			a.s.Logger().Error("add plan into periodic error", zap.Error(err))
			return err
		}
	} else if constant.PlanStatusDead == plan.Status {
		if err := a.s.periodicDispatcher.Remove(plan.Namespace, plan.ID); err != nil {
			a.s.Logger().Error("remove plan from periodic error", zap.Error(err))
			return err
		}
	}
	return nil
}

func (a *applierV3backend) NameSpaceUpdate(r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error) {
	resp, err := a.s.ScheduleStore().NameSpaceUpdate(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) NameSpaceAdd(r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error) {
	resp, err := a.s.ScheduleStore().NameSpaceAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}
func (a *applierV3backend) NameSpaceList(r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error) {
	resp, err := a.s.ScheduleStore().NameSpaceList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) NameSpaceDelete(r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	resp, err := a.s.ScheduleStore().NameSpaceDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) NameSpaceDetail(r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error) {
	resp, err := a.s.ScheduleStore().NameSpaceDetail(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) JobAdd(r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error) {
	resp, err := a.s.ScheduleStore().JobAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) JobList(r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error) {
	resp, err := a.s.ScheduleStore().JobList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) JobExist(r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error) {
	resp, err := a.s.ScheduleStore().JobExist(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) JobUpdate(r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error) {
	resp, err := a.s.ScheduleStore().JobUpdate(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) JobDelete(r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error) {
	resp, err := a.s.ScheduleStore().JobDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) JobDetail(r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error) {
	resp, err := a.s.ScheduleStore().JobDetail(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) EvalAdd(r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error) {
	// TODO if the queue is full, return first
	resp, err := a.s.ScheduleStore().EvalAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

/**
 * 操作队列（新增、修改的时候都需要)
 */
func (a *applierV3backend) evalQueueUpsert(eval *domain.Evaluation) {
	if a.s.isLeader() {
		// eval入队列
		if eval.ShouldEnqueue() {
			a.s.evalBroker.Enqueue(eval)
			a.s.jobBirdEye.Enqueue(eval)
		} else if eval.ShouldBlock() {
			a.s.blockedEvals.Block(eval)
			a.s.jobBirdEye.Enqueue(eval)
		} else if eval.Status == constant.EvalStatusComplete {
			a.s.blockedEvals.Untrack(eval.PlanID, eval.Namespace)
		}
	}
}

func (a *applierV3backend) EvalList(r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error) {
	resp, err := a.s.ScheduleStore().EvalList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) EvalUpdate(r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	resp, err := a.s.ScheduleStore().EvalUpdate(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	//if resp != nil && resp.Data != nil {
	//	eval := domain.ConvertEvaluationFromPb(resp.Data)
	//	a.evalQueueUpsert(eval)
	//}
	return resp, err
}

func (a *applierV3backend) EvalDelete(r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error) {
	resp, err := a.s.ScheduleStore().EvalDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) EvalDetail(r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error) {
	resp, err := a.s.ScheduleStore().EvalDetail(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) SimpleAllocationList(r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error) {
	resp, err := a.s.ScheduleStore().SimpleAllocationList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) AllocationAdd(r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error) {
	resp, err := a.s.ScheduleStore().AllocationAdd(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) AllocationList(r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error) {
	resp, err := a.s.ScheduleStore().AllocationList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) AllocationUpdate(r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	resp, err := a.s.ScheduleStore().AllocationUpdate(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	// 检查plan是否存在，plan被删除则需要处理
	if resp != nil && resp.Data != nil {
		allocUpdated := resp.Data
		if len(allocUpdated) > 0 {
			for _, alloc := range allocUpdated {
				if alloc.ClientStatus == constant.AllocClientStatusComplete ||
					alloc.ClientStatus == constant.AllocClientStatusFailed ||
					alloc.ClientStatus == constant.AllocClientStatusCancelled ||
					alloc.ClientStatus == constant.AllocClientStatusSkipped ||
					alloc.ClientStatus == constant.AllocClientStatusExpired {
					if err := a.s.jobBirdEye.DequeueJob(alloc.Namespace, alloc.JobId); err != nil {
						a.s.Logger().Error("dequeue job in eye view failed", zap.String("jobId", alloc.JobId), zap.Error(err))
					}
					// Unblock by no quota
					// TODO
					//quota, err := n.allocQuota(alloc.ID)
					//if err != nil {
					//	n.logger.Error("looking up quota associated with alloc failed", "alloc_id", alloc.ID, "error", err)
					//	return err
					//}
					//n.blockedEvals.UnblockClassAndQuota(node.ComputedClass, quota, index)
					//n.blockedEvals.UnblockNode(node.ID, index)
				}
				if alloc.ClientStatus == constant.AllocClientStatusRunning {
					if err := a.s.jobBirdEye.JobStartRunning(alloc.Namespace, alloc.JobId); err != nil {
						a.s.Logger().Error("change running job in eye view failed", zap.String("jobId", alloc.JobId), zap.Error(err))
					}
				}
				go a.dealWithJobDeregister(alloc)
			}
		}
	}
	return resp, err
}

// dealWithJobDeregister 如果计划已经被删除，则直接失败任务相关逻辑
func (a *applierV3backend) dealWithJobDeregister(alloc *schedulepb.Allocation) {
	// 注意 alloc 中的planid可能带有/periodic
	if alloc.PlanId == "" {
		return
	}
	// PeriodicLaunchSuffix 将含有这个后缀的去掉
	realPlanId := alloc.PlanId
	if strings.Contains(realPlanId, constant.PeriodicLaunchSuffix) {
		realPlanId = alloc.PlanId[0:strings.Index(realPlanId, constant.PeriodicLaunchSuffix)]
	}
	planDetailResp, _ := a.PlanDetail(&pb.SchedulePlanDetailRequest{
		Id:        realPlanId,
		Namespace: alloc.Namespace,
	})
	if planDetailResp == nil || planDetailResp.GetData() == nil {
		_, evalAddErr := a.EvalAdd(&pb.ScheduleEvalAddRequest{
			Id:          ulid.Generate(),
			Namespace:   alloc.Namespace,
			TriggeredBy: constant.EvalTriggerJobDeregister,
			PlanId:      realPlanId,
			JobId:       alloc.JobId,
			Type:        constant.PlanTypeService,
			Priority:    0,
			Status:      constant.EvalStatusPending,
		})
		if evalAddErr != nil {
			a.s.lg.Warn("add eval failed for plan deregister",
				zap.String("allocation-id", alloc.Id),
				zap.Error(evalAddErr))
		}
	}
}

func (a *applierV3backend) AllocationDelete(r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error) {
	resp, err := a.s.ScheduleStore().AllocationDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) AllocationDetail(r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error) {
	resp, err := a.s.ScheduleStore().AllocationDetail(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) NodeAdd(r *pb.NodeAddRequest) (*pb.NodeAddResponse, error) {
	resp, err := a.s.ScheduleStore().NodeAdd(r, a.s.Cfg.MinHeartbeatTTL)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	if constant.NodeStatusDown != r.Status {
		ttl, err := a.s.dispatcherHeartbeater.resetHeartbeatTimer(r.ID)
		if err != nil {
			a.s.Logger().Warn("heartbeat reset failed", zap.Error(err))
			return nil, err
		}
		resp.HeartbeatTtl = ttl.Milliseconds()
	}
	return resp, err
}

func (a *applierV3backend) NodeList(r *pb.NodeListRequest) (*pb.NodeListResponse, error) {
	resp, err := a.s.ScheduleStore().NodeList(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

// NodeUpdate 节点状态更新带来的运行中allocation需要进行失败处理
func (a *applierV3backend) NodeUpdate(r *pb.NodeUpdateRequest) (*pb.NodeUpdateResponse, error) {
	resp, err := a.s.ScheduleStore().NodeUpdate(r, a.s.Cfg.MinHeartbeatTTL)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	a.s.Logger().Debug("from node update", zap.String("node", r.ID), zap.String("status", r.Status))
	var ttl time.Duration
	switch r.Status {
	case constant.NodeStatusDown:
		// Determine if there are any Vault accessors on the node to cleanup
		a.s.Logger().Debug("TODO when node down delete the service and other")
		a.s.Logger().Debug("需要解注册一些内容")
	case constant.NodeStatusReady:
		a.s.blockedEvals.Unblock(r.ID, 0)
		ttl, _ = a.s.dispatcherHeartbeater.resetHeartbeatTimer(r.ID)
	default:
		ttl, _ = a.s.dispatcherHeartbeater.resetHeartbeatTimer(r.ID)
	}
	if ttl != 0 && resp != nil {
		resp.HeartbeatTtl = ttl.Milliseconds()
	}
	//if err := a.EvalWithNodeChange(r, resp); err != nil {
	//	a.s.Logger().Warn("eval with node change error", zap.Error(err))
	//	return nil, err
	//}
	// // at last check the status of node
	//	if domain.ShouldDrainNode(r.Status) || nodeStatusTransitionRequiresEval(r.Status, updatedNode.Status) {
	//		// create eval for new node ready and begin allocation plans
	//		_, err := as.createNodeEvals(r.ID)
	//		if err != nil {
	//			as.lg.Error("eval creation failed", zap.Error(err))
	//			return nil, err
	//		}
	//	}
	return resp, err
}

// TODO 将本方法迁移到server逻辑中
func (a *applierV3backend) EvalWithNodeChange(r *pb.NodeUpdateRequest, resp *pb.NodeUpdateResponse) error {
	var nodeNow *domain.Node
	if resp.Data != nil {
		nodeNow = domain.ConvertNodeFromPb(resp.Data)
		if domain.ShouldDrainNode(r.Status) || nodeStatusTransitionRequiresEval(r.Status, nodeNow.Status) {
			// create eval for new node ready and begin allocation plans
			allocResp, err := a.AllocationList(&pb.ScheduleAllocationListRequest{
				NodeId: r.ID,
				Status: fmt.Sprintf("%s,%s", constant.AllocClientStatusPending, constant.AllocClientStatusRunning),
			})
			if err != nil {
				return err
			}
			if allocResp != nil && allocResp.Data != nil && len(allocResp.Data) > 0 {
				planIDs := map[domain.NamespacedID]struct{}{}
				for _, alloc := range allocResp.Data {
					// 防止重复运行的map
					allocation := domain.ConvertAllocation(alloc)
					if _, ok := planIDs[allocation.PlanNamespacedID()]; ok {
						continue
					}
					if !allocation.MigrateStatus() {
						continue
					}
					planIDs[allocation.PlanNamespacedID()] = struct{}{}
					planResp, err := a.PlanDetail(&pb.SchedulePlanDetailRequest{
						Id:        alloc.PlanId,
						Namespace: alloc.Namespace,
					})
					if err != nil {
						return err
					}
					// Create a new eval
					eval := &pb.ScheduleEvalAddRequest{
						Id:          ulid.Generate(),
						Namespace:   allocation.Namespace,
						Priority:    planResp.Data.Priority,
						Type:        planResp.Data.Type,
						TriggeredBy: constant.EvalTriggerNodeUpdate,
						PlanId:      allocation.PlanID,
						JobId:       allocation.JobId,
						NodeId:      nodeNow.ID,
						Status:      constant.EvalStatusPending,
					}
					_, err = a.EvalAdd(eval)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func nodeStatusTransitionRequiresEval(newStatus, oldStatus string) bool {
	initToReady := oldStatus == constant.NodeStatusInit && newStatus == constant.NodeStatusReady
	terminalToReady := oldStatus == constant.NodeStatusDown && newStatus == constant.NodeStatusReady
	disconnectedToOther := oldStatus == constant.NodeStatusDisconnected && newStatus != constant.NodeStatusDisconnected
	otherToDisconnected := oldStatus != constant.NodeStatusDisconnected && newStatus == constant.NodeStatusDisconnected
	return initToReady || terminalToReady || disconnectedToOther || otherToDisconnected
}

func (a *applierV3backend) NodeDelete(r *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error) {
	resp, err := a.s.ScheduleStore().NodeDelete(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	a.s.dispatcherHeartbeater.clearHeartbeatTimer(r.Id)
	return resp, err
}

func (a *applierV3backend) NodeDetail(r *pb.NodeDetailRequest) (*pb.NodeDetailResponse, error) {
	resp, err := a.s.ScheduleStore().NodeDetail(r)
	if resp != nil {
		resp.Header = newHeader(a.s)
	}
	return resp, err
}

func (a *applierV3backend) PlanAllocationEnqueue(r *pb.PlanAllocationEnqueueRequest) (*pb.PlanAllocationEnqueueResponse, error) {
	if a.s.isLeader() {
		if r.Allocations == nil {
			return nil, fmt.Errorf("cannot submit nil plan allocation")
		}
		evalId := r.EvalId
		token := r.EvalToken
		if err := a.s.evalBroker.PauseNackTimeout(evalId, token); err != nil {
			return nil, err
		}
		defer a.s.evalBroker.ResumeNackTimeout(evalId, token)
		// Submit the plan to the queue
		plan := domain.ConvertPlanAllocation(r)
		future, err := a.s.allocationQueue.Enqueue(plan)
		if err != nil {
			return nil, err
		}

		// Wait for the results
		result, err := future.Wait()
		if err != nil {
			return nil, err
		}
		var allocs []*schedulepb.Allocation
		if result.NodeAllocation != nil && len(result.NodeAllocation) > 0 {
			// 后续拼装
			allocs = domain.ConvertPlanAllocationToPb(result.NodeAllocation)
		}
		return &pb.PlanAllocationEnqueueResponse{
			Header: newHeader(a.s),
			Data:   allocs,
		}, nil
	} else {
		return &pb.PlanAllocationEnqueueResponse{
			Header: newHeader(a.s),
		}, nil
	}
}

type quotaApplierV3 struct {
	applierV3
	q Quota
}

func newQuotaApplierV3(s *EtcdServer, app applierV3) applierV3 {
	return &quotaApplierV3{app, NewBackendQuota(s, "v3-applier")}
}

func (a *quotaApplierV3) Put(ctx context.Context, txn mvcc.TxnWrite, p *pb.PutRequest) (*pb.PutResponse, *traceutil.Trace, error) {
	ok := a.q.Available(p)
	resp, trace, err := a.applierV3.Put(ctx, txn, p)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, trace, err
}

func (a *quotaApplierV3) Txn(ctx context.Context, rt *pb.TxnRequest) (*pb.TxnResponse, *traceutil.Trace, error) {
	ok := a.q.Available(rt)
	resp, trace, err := a.applierV3.Txn(ctx, rt)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, trace, err
}

func (a *quotaApplierV3) LeaseGrant(lc *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	ok := a.q.Available(lc)
	resp, err := a.applierV3.LeaseGrant(lc)
	if err == nil && !ok {
		err = ErrNoSpace
	}
	return resp, err
}

type kvSort struct{ kvs []mvccpb.KeyValue }

func (s *kvSort) Swap(i, j int) {
	t := s.kvs[i]
	s.kvs[i] = s.kvs[j]
	s.kvs[j] = t
}
func (s *kvSort) Len() int { return len(s.kvs) }

type kvSortByKey struct{ *kvSort }

func (s *kvSortByKey) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Key, s.kvs[j].Key) < 0
}

type kvSortByVersion struct{ *kvSort }

func (s *kvSortByVersion) Less(i, j int) bool {
	return (s.kvs[i].Version - s.kvs[j].Version) < 0
}

type kvSortByCreate struct{ *kvSort }

func (s *kvSortByCreate) Less(i, j int) bool {
	return (s.kvs[i].CreateRevision - s.kvs[j].CreateRevision) < 0
}

type kvSortByMod struct{ *kvSort }

func (s *kvSortByMod) Less(i, j int) bool {
	return (s.kvs[i].ModRevision - s.kvs[j].ModRevision) < 0
}

type kvSortByValue struct{ *kvSort }

func (s *kvSortByValue) Less(i, j int) bool {
	return bytes.Compare(s.kvs[i].Value, s.kvs[j].Value) < 0
}

func checkRequests(rv mvcc.ReadView, rt *pb.TxnRequest, txnPath []bool, f checkReqFunc) (int, error) {
	txnCount := 0
	reqs := rt.Success
	if !txnPath[0] {
		reqs = rt.Failure
	}
	for _, req := range reqs {
		if tv, ok := req.Request.(*pb.RequestOp_RequestTxn); ok && tv.RequestTxn != nil {
			txns, err := checkRequests(rv, tv.RequestTxn, txnPath[1:], f)
			if err != nil {
				return 0, err
			}
			txnCount += txns + 1
			txnPath = txnPath[txns+1:]
			continue
		}
		if err := f(rv, req); err != nil {
			return 0, err
		}
	}
	return txnCount, nil
}

func (a *applierV3backend) checkRequestPut(rv mvcc.ReadView, reqOp *pb.RequestOp) error {
	tv, ok := reqOp.Request.(*pb.RequestOp_RequestPut)
	if !ok || tv.RequestPut == nil {
		return nil
	}
	req := tv.RequestPut
	if req.IgnoreValue || req.IgnoreLease {
		// expects previous key-value, error if not exist
		rr, err := rv.Range(context.TODO(), req.Key, nil, mvcc.RangeOptions{})
		if err != nil {
			return err
		}
		if rr == nil || len(rr.KVs) == 0 {
			return ErrKeyNotFound
		}
	}
	if lease.LeaseID(req.Lease) != lease.NoLease {
		if l := a.s.lessor.Lookup(lease.LeaseID(req.Lease)); l == nil {
			return lease.ErrLeaseNotFound
		}
	}
	return nil
}

func (a *applierV3backend) checkRequestRange(rv mvcc.ReadView, reqOp *pb.RequestOp) error {
	tv, ok := reqOp.Request.(*pb.RequestOp_RequestRange)
	if !ok || tv.RequestRange == nil {
		return nil
	}
	req := tv.RequestRange
	switch {
	case req.Revision == 0:
		return nil
	case req.Revision > rv.Rev():
		return mvcc.ErrFutureRev
	case req.Revision < rv.FirstRev():
		return mvcc.ErrCompacted
	}
	return nil
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// mkGteRange determines if the range end is a >= range. This works around grpc
// sending empty byte strings as nil; >= is encoded in the range end as '\0'.
// If it is a GTE range, then []byte{} is returned to indicate the empty byte
// string (vs nil being no byte string).
func mkGteRange(rangeEnd []byte) []byte {
	if len(rangeEnd) == 1 && rangeEnd[0] == 0 {
		return []byte{}
	}
	return rangeEnd
}

func noSideEffect(r *pb.InternalRaftRequest) bool {
	return r.Range != nil || r.AuthUserGet != nil || r.AuthRoleGet != nil || r.AuthStatus != nil
}

func removeNeedlessRangeReqs(txn *pb.TxnRequest) {
	f := func(ops []*pb.RequestOp) []*pb.RequestOp {
		j := 0
		for i := 0; i < len(ops); i++ {
			if _, ok := ops[i].Request.(*pb.RequestOp_RequestRange); ok {
				continue
			}
			ops[j] = ops[i]
			j++
		}

		return ops[:j]
	}

	txn.Success = f(txn.Success)
	txn.Failure = f(txn.Failure)
}

func pruneKVs(rr *mvcc.RangeResult, isPrunable func(*mvccpb.KeyValue) bool) {
	j := 0
	for i := range rr.KVs {
		rr.KVs[j] = rr.KVs[i]
		if !isPrunable(&rr.KVs[i]) {
			j++
		}
	}
	rr.KVs = rr.KVs[:j]
}

func newHeader(s *EtcdServer) *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: uint64(s.Cluster().ID()),
		MemberId:  uint64(s.ID()),
		Revision:  s.KV().Rev(),
		RaftTerm:  s.Term(),
	}
}
