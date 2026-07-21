// Copyright 2015 The etcd Authors
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
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"google.golang.org/grpc"
	"strconv"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/helper/cronutil"
	"yunli.com/jobpool/api/v2/helper/ulid"
	"yunli.com/jobpool/api/v2/membershippb"
	"yunli.com/jobpool/api/v2/schedulepb"
	"yunli.com/jobpool/pkg/v2/traceutil"
	"yunli.com/jobpool/server/v2/auth"
	"yunli.com/jobpool/server/v2/etcdserver/api/membership"
	"yunli.com/jobpool/server/v2/lease"
	"yunli.com/jobpool/server/v2/lease/leasehttp"
	"yunli.com/jobpool/server/v2/mvcc"
	"yunli.com/jobpool/server/v2/scheduler/schedulerhttp"

	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

const (
	// In the health case, there might be a small gap (10s of entries) between
	// the applied index and committed index.
	// However, if the committed entries are very heavy to apply, the gap might grow.
	// We should stop accepting new proposals if the gap growing to a certain point.
	maxGapBetweenApplyAndCommitIndex = 5000
	traceThreshold                   = 100 * time.Millisecond
	readIndexRetryTime               = 500 * time.Millisecond

	// The timeout for the node to catch up its applied index, and is used in
	// lease related operations, such as LeaseRenew and LeaseTimeToLive.
	applyTimeout = time.Second
)

type RaftKV interface {
	Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error)
	Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error)
	DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error)
	Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error)
	Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error)
}

type Lessor interface {
	// LeaseGrant sends LeaseGrant request to raft and apply it after committed.
	LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error)
	// LeaseRevoke sends LeaseRevoke request to raft and apply it after committed.
	LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error)

	// LeaseRenew renews the lease with given ID. The renewed TTL is returned. Or an error
	// is returned.
	LeaseRenew(ctx context.Context, id lease.LeaseID) (int64, error)

	// LeaseTimeToLive retrieves lease information.
	LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error)

	// LeaseLeases lists all leases.
	LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error)
}

type Authenticator interface {
	AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error)
	AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error)
	AuthStatus(ctx context.Context, r *pb.AuthStatusRequest) (*pb.AuthStatusResponse, error)
	Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error)
	UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error)
	UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error)
	UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error)
	UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error)
	UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error)
	UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error)
	RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error)
	RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error)
	RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error)
	RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error)
	RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error)
	UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error)
	RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error)
}

type ScheduleService interface {
	PlanAdd(ctx context.Context, r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error)
	PlanList(ctx context.Context, r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error)
	PlanUpdate(ctx context.Context, r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error)
	PlanDelete(ctx context.Context, r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error)
	PlanDetail(ctx context.Context, r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error)
	PlanOnline(ctx context.Context, r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error)
	PlanOffline(ctx context.Context, r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error)
	NameSpaceAdd(ctx context.Context, r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error)
	NameSpaceList(ctx context.Context, r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error)
	NameSpaceDelete(ctx context.Context, r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error)
	NameSpaceDetail(ctx context.Context, r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error)
	NameSpaceUpdate(ctx context.Context, r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error)
	JobAdd(ctx context.Context, r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error)
	JobList(ctx context.Context, r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error)
	JobExist(ctx context.Context, r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error)
	JobUpdate(ctx context.Context, r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error)
	JobDelete(ctx context.Context, r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error)
	JobDetail(ctx context.Context, r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error)
	EvalAdd(ctx context.Context, r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error)
	EvalList(ctx context.Context, r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error)
	EvalUpdate(ctx context.Context, r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error)
	EvalDelete(ctx context.Context, r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error)
	EvalDetail(ctx context.Context, r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error)
	AllocationAdd(ctx context.Context, r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error)
	AllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error)
	AllocationUpdate(ctx context.Context, r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error)
	AllocationDelete(ctx context.Context, r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error)
	AllocationDetail(ctx context.Context, r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error)
	SimpleAllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error)
	EvalDequeue(ctx context.Context, r *pb.ScheduleEvalDequeueRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalDequeueResponse, error)
	EvalAck(ctx context.Context, r *pb.ScheduleEvalAckRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalAckResponse, error)
	EvalNack(ctx context.Context, r *pb.ScheduleEvalNackRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalNackResponse, error)
	NodeAdd(ctx context.Context, r *pb.NodeAddRequest, opts ...grpc.CallOption) (*pb.NodeAddResponse, error)
	NodeList(ctx context.Context, r *pb.NodeListRequest, opts ...grpc.CallOption) (*pb.NodeListResponse, error)
	NodeUpdate(ctx context.Context, r *pb.NodeUpdateRequest, opts ...grpc.CallOption) (*pb.NodeUpdateResponse, error)
	NodeDelete(ctx context.Context, r *pb.NodeDeleteRequest, opts ...grpc.CallOption) (*pb.NodeDeleteResponse, error)
	NodeDetail(ctx context.Context, r *pb.NodeDetailRequest, opts ...grpc.CallOption) (*pb.NodeDetailResponse, error)
	PlanAllocationEnqueue(ctx context.Context, r *pb.PlanAllocationEnqueueRequest, opts ...grpc.CallOption) (*pb.PlanAllocationEnqueueResponse, error)
	QueueDetail(ctx context.Context, r *pb.QueueDetailRequest, opts ...grpc.CallOption) (*pb.QueueDetailResponse, error)
	QueueJobViewDetail(ctx context.Context, r *pb.QueueJobViewRequest, opts ...grpc.CallOption) (*pb.QueueJobViewResponse, error)
	QueueForbiddenUpdate(ctx context.Context, r *pb.QueueForbiddenRequest, opts ...grpc.CallOption) (*pb.QueueForbiddenResponse, error)
	QueueStatusDetail(ctx context.Context, r *pb.QueueStatusRequest, opts ...grpc.CallOption) (*pb.QueueStatusResponse, error)
}

func (s *EtcdServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	trace := traceutil.New("range",
		s.Logger(),
		traceutil.Field{Key: "range_begin", Value: string(r.Key)},
		traceutil.Field{Key: "range_end", Value: string(r.RangeEnd)},
	)
	ctx = context.WithValue(ctx, traceutil.TraceKey, trace)

	var resp *pb.RangeResponse
	var err error
	defer func(start time.Time) {
		warnOfExpensiveReadOnlyRangeRequest(s.Logger(), s.Cfg.WarningApplyDuration, start, r, resp, err)
		if resp != nil {
			trace.AddField(
				traceutil.Field{Key: "response_count", Value: len(resp.Kvs)},
				traceutil.Field{Key: "response_revision", Value: resp.Header.Revision},
			)
		}
		trace.LogIfLong(traceThreshold)
	}(time.Now())

	if !r.Serializable {
		err = s.linearizableReadNotify(ctx)
		trace.Step("agreement among raft nodes before linearized reading")
		if err != nil {
			return nil, err
		}
	}
	chk := func(ai *auth.AuthInfo) error {
		return s.authStore.IsRangePermitted(ai, r.Key, r.RangeEnd)
	}

	get := func() { resp, err = s.applyV3Base.Range(ctx, nil, r) }
	if serr := s.doSerialize(ctx, chk, get); serr != nil {
		err = serr
		return nil, err
	}
	return resp, err
}

func (s *EtcdServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	ctx = context.WithValue(ctx, traceutil.StartTimeKey, time.Now())
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Put: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.PutResponse), nil
}

func (s *EtcdServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{DeleteRange: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.DeleteRangeResponse), nil
}

func (s *EtcdServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if isTxnReadonly(r) {
		trace := traceutil.New("transaction",
			s.Logger(),
			traceutil.Field{Key: "read_only", Value: true},
		)
		ctx = context.WithValue(ctx, traceutil.TraceKey, trace)
		if !isTxnSerializable(r) {
			err := s.linearizableReadNotify(ctx)
			trace.Step("agreement among raft nodes before linearized reading")
			if err != nil {
				return nil, err
			}
		}
		var resp *pb.TxnResponse
		var err error
		chk := func(ai *auth.AuthInfo) error {
			return checkTxnAuth(s.authStore, ai, r)
		}

		defer func(start time.Time) {
			warnOfExpensiveReadOnlyTxnRequest(s.Logger(), s.Cfg.WarningApplyDuration, start, r, resp, err)
			trace.LogIfLong(traceThreshold)
		}(time.Now())

		get := func() { resp, _, err = s.applyV3Base.Txn(ctx, r) }
		if serr := s.doSerialize(ctx, chk, get); serr != nil {
			return nil, serr
		}
		return resp, err
	}

	ctx = context.WithValue(ctx, traceutil.StartTimeKey, time.Now())
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{Txn: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.TxnResponse), nil
}

func isTxnSerializable(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil || !r.Serializable {
			return false
		}
	}
	return true
}

func isTxnReadonly(r *pb.TxnRequest) bool {
	for _, u := range r.Success {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	for _, u := range r.Failure {
		if r := u.GetRequestRange(); r == nil {
			return false
		}
	}
	return true
}

func (s *EtcdServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	startTime := time.Now()
	result, err := s.processInternalRaftRequestOnce(ctx, pb.InternalRaftRequest{Compaction: r})
	trace := traceutil.TODO()
	if result != nil && result.trace != nil {
		trace = result.trace
		defer func() {
			trace.LogIfLong(traceThreshold)
		}()
		applyStart := result.trace.GetStartTime()
		result.trace.SetStartTime(startTime)
		trace.InsertStep(0, applyStart, "process raft request")
	}
	if r.Physical && result != nil && result.physc != nil {
		<-result.physc
		// The compaction is done deleting keys; the hash is now settled
		// but the data is not necessarily committed. If there's a crash,
		// the hash may revert to a hash prior to compaction completing
		// if the compaction resumes. Force the finished compaction to
		// commit so it won't resume following a crash.
		s.be.ForceCommit()
		trace.Step("physically apply compaction")
	}
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	resp := result.resp.(*pb.CompactionResponse)
	if resp == nil {
		resp = &pb.CompactionResponse{}
	}
	if resp.Header == nil {
		resp.Header = &pb.ResponseHeader{}
	}
	resp.Header.Revision = s.kv.Rev()
	trace.AddField(traceutil.Field{Key: "response_revision", Value: resp.Header.Revision})
	return resp, nil
}

func (s *EtcdServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	// no id given? choose one
	for r.ID == int64(lease.NoLease) {
		// only use positive int64 id's
		r.ID = int64(s.reqIDGen.Next() & ((1 << 63) - 1))
	}
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseGrant: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.LeaseGrantResponse), nil
}

func (s *EtcdServer) waitAppliedIndex() error {
	select {
	case <-s.ApplyWait():
	case <-s.stopping:
		return ErrStopped
	case <-time.After(applyTimeout):
		return ErrTimeoutWaitAppliedIndex
	}

	return nil
}

func (s *EtcdServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (*pb.LeaseRevokeResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{LeaseRevoke: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.LeaseRevokeResponse), nil
}

func (s *EtcdServer) LeaseRenew(ctx context.Context, id lease.LeaseID) (int64, error) {
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return 0, err
		}

		ttl, err := s.lessor.Renew(id)
		if err == nil { // already requested to primary lessor(leader)
			return ttl, nil
		}
		if err != lease.ErrNotPrimary {
			return -1, err
		}
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	// renewals don't go through raft; forward to leader manually
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return -1, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + leasehttp.LeasePrefix
			ttl, err := leasehttp.RenewHTTP(cctx, id, lurl, s.peerRt)
			if err == nil || err == lease.ErrLeaseNotFound {
				return ttl, err
			}
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}

	if cctx.Err() == context.DeadlineExceeded {
		return -1, ErrTimeout
	}
	return -1, ErrCanceled
}

func (s *EtcdServer) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (*pb.LeaseTimeToLiveResponse, error) {
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		// primary; timetolive directly from leader
		le := s.lessor.Lookup(lease.LeaseID(r.ID))
		if le == nil {
			return nil, lease.ErrLeaseNotFound
		}
		// TODO: fill out ResponseHeader
		resp := &pb.LeaseTimeToLiveResponse{Header: &pb.ResponseHeader{}, ID: r.ID, TTL: int64(le.Remaining().Seconds()), GrantedTTL: le.TTL()}
		if r.Keys {
			ks := le.Keys()
			kbs := make([][]byte, len(ks))
			for i := range ks {
				kbs[i] = []byte(ks[i])
			}
			resp.Keys = kbs
		}
		return resp, nil
	}

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	// forward to leader
	for cctx.Err() == nil {
		leader, err := s.waitLeader(cctx)
		if err != nil {
			return nil, err
		}
		for _, url := range leader.PeerURLs {
			lurl := url + leasehttp.LeaseInternalPrefix
			resp, err := leasehttp.TimeToLiveHTTP(cctx, lease.LeaseID(r.ID), r.Keys, lurl, s.peerRt)
			if err == nil {
				return resp.LeaseTimeToLiveResponse, nil
			}
			if err == lease.ErrLeaseNotFound {
				return nil, err
			}
		}
	}

	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (*pb.LeaseLeasesResponse, error) {
	ls := s.lessor.Leases()
	lss := make([]*pb.LeaseStatus, len(ls))
	for i := range ls {
		lss[i] = &pb.LeaseStatus{ID: int64(ls[i].ID)}
	}
	return &pb.LeaseLeasesResponse{Header: newHeader(s), Leases: lss}, nil
}

func (s *EtcdServer) waitLeader(ctx context.Context) (*membership.Member, error) {
	leader := s.cluster.Member(s.Leader())
	for leader == nil {
		// wait an election
		dur := time.Duration(s.Cfg.ElectionTicks) * time.Duration(s.Cfg.TickMs) * time.Millisecond
		select {
		case <-time.After(dur):
			leader = s.cluster.Member(s.Leader())
		case <-s.stopping:
			return nil, ErrStopped
		case <-ctx.Done():
			return nil, ErrNoLeader
		}
	}
	if leader == nil || len(leader.PeerURLs) == 0 {
		return nil, ErrNoLeader
	}
	return leader, nil
}

func (s *EtcdServer) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{Alarm: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AlarmResponse), nil
}

func (s *EtcdServer) AuthEnable(ctx context.Context, r *pb.AuthEnableRequest) (*pb.AuthEnableResponse, error) {
	resp, err := s.raftRequestOnce(ctx, pb.InternalRaftRequest{AuthEnable: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthEnableResponse), nil
}

func (s *EtcdServer) AuthDisable(ctx context.Context, r *pb.AuthDisableRequest) (*pb.AuthDisableResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthDisable: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthDisableResponse), nil
}

func (s *EtcdServer) AuthStatus(ctx context.Context, r *pb.AuthStatusRequest) (*pb.AuthStatusResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthStatus: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthStatusResponse), nil
}

func (s *EtcdServer) Authenticate(ctx context.Context, r *pb.AuthenticateRequest) (*pb.AuthenticateResponse, error) {
	if err := s.linearizableReadNotify(ctx); err != nil {
		return nil, err
	}

	lg := s.Logger()

	var resp proto.Message
	for {
		checkedRevision, err := s.AuthStore().CheckPassword(r.Name, r.Password)
		if err != nil {
			if err != auth.ErrAuthNotEnabled {
				lg.Warn(
					"invalid authentication was requested",
					zap.String("user", r.Name),
					zap.Error(err),
				)
			}
			return nil, err
		}

		st, err := s.AuthStore().GenTokenPrefix()
		if err != nil {
			return nil, err
		}

		// internalReq doesn't need to have Password because the above s.AuthStore().CheckPassword() already did it.
		// In addition, it will let a WAL entry not record password as a plain text.
		internalReq := &pb.InternalAuthenticateRequest{
			Name:        r.Name,
			SimpleToken: st,
		}

		resp, err = s.raftRequestOnce(ctx, pb.InternalRaftRequest{Authenticate: internalReq})
		if err != nil {
			return nil, err
		}
		if checkedRevision == s.AuthStore().Revision() {
			break
		}

		lg.Info("revision when password checked became stale; retrying")
	}

	return resp.(*pb.AuthenticateResponse), nil
}

func (s *EtcdServer) UserAdd(ctx context.Context, r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	if r.Options == nil || !r.Options.NoPassword {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(r.Password), s.authStore.BcryptCost())
		if err != nil {
			return nil, err
		}
		r.HashedPassword = base64.StdEncoding.EncodeToString(hashedPassword)
		r.Password = ""
	}

	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserAddResponse), nil
}

func (s *EtcdServer) UserDelete(ctx context.Context, r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserDeleteResponse), nil
}

func (s *EtcdServer) UserChangePassword(ctx context.Context, r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	if r.Password != "" {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(r.Password), s.authStore.BcryptCost())
		if err != nil {
			return nil, err
		}
		r.HashedPassword = base64.StdEncoding.EncodeToString(hashedPassword)
		r.Password = ""
	}

	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserChangePassword: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserChangePasswordResponse), nil
}

func (s *EtcdServer) UserGrantRole(ctx context.Context, r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserGrantRole: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserGrantRoleResponse), nil
}

func (s *EtcdServer) UserGet(ctx context.Context, r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserGet: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserGetResponse), nil
}

func (s *EtcdServer) UserList(ctx context.Context, r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserListResponse), nil
}

func (s *EtcdServer) UserRevokeRole(ctx context.Context, r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthUserRevokeRole: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthUserRevokeRoleResponse), nil
}

func (s *EtcdServer) RoleAdd(ctx context.Context, r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleAddResponse), nil
}

func (s *EtcdServer) RoleGrantPermission(ctx context.Context, r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleGrantPermission: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleGrantPermissionResponse), nil
}

func (s *EtcdServer) RoleGet(ctx context.Context, r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleGet: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleGetResponse), nil
}

func (s *EtcdServer) RoleList(ctx context.Context, r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleListResponse), nil
}

func (s *EtcdServer) RoleRevokePermission(ctx context.Context, r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleRevokePermission: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleRevokePermissionResponse), nil
}

func (s *EtcdServer) RoleDelete(ctx context.Context, r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{AuthRoleDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.AuthRoleDeleteResponse), nil
}

// schedule start

func (s *EtcdServer) PlanList(ctx context.Context, r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SchedulePlanListResponse), nil
}

func (s *EtcdServer) PlanAdd(ctx context.Context, r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error) {
	// 参数校验
	if r.Name == "" || r.Namespace == "" {
		return nil, domain.NewErr1001Blank("name和namespace")
	}
	if r.Type == "" {
		return nil, domain.NewErr1001Blank("type")
	} else if r.Type != constant.PlanTypeService && r.Type != constant.PlanTypeCore {
		return nil, domain.NewErr1002LimitValue("type", "service")
	}
	// 校验namespace是否存在
	scheduleNameSpaceDetailRequest := &pb.ScheduleNameSpaceDetailRequest{
		Name: r.Namespace,
	}
	// 校验cron是否正确
	if r.Periodic != nil && r.Periodic.Spec != "" {
		dtNow := time.Now()
		_, err := cronutil.Next(r.Periodic.Spec, dtNow)
		if err != nil {
			return nil, domain.NewErr1010Format("Spec", r.Periodic.Spec)
		}
	}
	_, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleNamespaceDetail: scheduleNameSpaceDetailRequest})
	if err != nil {
		return nil, err
	}
	// 无ID则分配
	if r.Id == "" {
		r.Id = ulid.Generate()
	}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SchedulePlanAddResponse), nil
}

func (s *EtcdServer) PlanUpdate(ctx context.Context, r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error) {
	scheduleNameSpaceDetailRequest := &pb.ScheduleNameSpaceDetailRequest{
		Name: r.Namespace,
	}
	// 校验cron是否正确
	if r.Periodic != nil && r.Periodic.Spec != "" {
		dtNow := time.Now()
		_, err := cronutil.Next(r.Periodic.Spec, dtNow)
		if err != nil {
			return nil, domain.NewErr1010Format("Spec", r.Periodic.Spec)
		}
	}
	_, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleNamespaceDetail: scheduleNameSpaceDetailRequest})
	if err != nil {
		return nil, err
	}
	_, err = s.checkPlanExist(ctx, r.Id, r.Namespace)
	if err != nil {
		return nil, err
	}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanUpdate: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SchedulePlanUpdateResponse), nil
}

func (s *EtcdServer) PlanDelete(ctx context.Context, r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error) {
	plan, err := s.checkPlanExist(ctx, r.Id, r.Namespace)
	if err != nil {
		return nil, err
	}
	if constant.PlanStatusRunning == plan.Status {
		return nil, domain.NewErr1107FailExecute("删除流程", r.Id, "已上线")
	}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SchedulePlanDeleteResponse), nil
}

func (s *EtcdServer) PlanDetail(ctx context.Context, r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error) {
	//resp, err := s.ScheduleStore().PlanDetail(r)
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanDetail: r})
	if err != nil {
		return nil, err
	}
	//if resp != nil {
	//	resp.Header = newHeader(s)
	//}
	//return resp, nil
	return resp.(*pb.SchedulePlanDetailResponse), nil
}

func (s *EtcdServer) PlanOnline(ctx context.Context, r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error) {
	plan, err := s.checkPlanExist(ctx, r.Id, r.Namespace)
	if err != nil {
		return nil, err
	}
	if constant.PlanStatusRunning == plan.Status {
		return nil, domain.NewErr1107FailExecute("上线流程", r.Id, "已上线")
	}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanOnline: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SchedulePlanDetailResponse), nil
}

func (s *EtcdServer) PlanOffline(ctx context.Context, r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error) {
	plan, err := s.checkPlanExist(ctx, r.Id, r.Namespace)
	if err != nil {
		return nil, err
	}
	if constant.PlanStatusDead == plan.Status {
		return nil, domain.NewErr1107FailExecute("下线流程", r.Id, "已下线")
	}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{SchedulePlanOffline: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SchedulePlanDetailResponse), nil
}

func (s *EtcdServer) checkPlanExist(ctx context.Context, planId string, namespace string) (*schedulepb.Plan, error) {
	planResp, err := s.PlanDetail(ctx, &pb.SchedulePlanDetailRequest{
		Id:        planId,
		Namespace: namespace,
	})
	if err != nil {
		return nil, err
	}
	plan := planResp.Data
	return plan, nil
}

func (s *EtcdServer) NameSpaceAdd(ctx context.Context, r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleNamespaceAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleNameSpaceAddResponse), nil
}

func (s *EtcdServer) NameSpaceUpdate(ctx context.Context, r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleNamespaceUpdate: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleNameSpaceUpdateResponse), nil
}

func (s *EtcdServer) NameSpaceList(ctx context.Context, r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleNamespaceList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleNameSpaceListResponse), nil
}

func (s *EtcdServer) NameSpaceDelete(ctx context.Context, r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleNamespaceDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleNameSpaceDeleteResponse), nil
}

func (s *EtcdServer) NameSpaceDetail(ctx context.Context, r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error) {
	resp, err := s.ScheduleStore().NameSpaceDetail(r)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

func (s *EtcdServer) JobAdd(ctx context.Context, r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleJobAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleJobAddResponse), nil
}

func (s *EtcdServer) JobList(ctx context.Context, r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error) {
	resp, err := s.ScheduleStore().JobList(r)
	// resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleJobList: r})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

func (s *EtcdServer) JobExist(ctx context.Context, r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error) {
	resp, err := s.ScheduleStore().JobExist(r)
	// resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleJobExist: r})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

func (s *EtcdServer) JobUpdate(ctx context.Context, r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error) {
	// 参数校验
	if !constant.JobStatusSet[r.Status] {
		var valueSample string
		for key, _ := range constant.JobStatusSet {
			valueSample += key + ","
		}
		if len(valueSample) > 0 {
			valueSample = valueSample[0 : len(valueSample)-1]
		}
		return nil, domain.NewErr1002LimitValue("参数status值", valueSample)
	}
	detailResponse, err := s.JobDetail(ctx, &pb.ScheduleJobDetailRequest{Id: r.Id})
	if err != nil {
		return nil, err
	}
	if detailResponse == nil || detailResponse.Data == nil {
		return nil, domain.NewErr1101None("任务", r.Id)
	}
	jobDetail := detailResponse.Data
	// 将job-update转换为allocation-update，保证allocation和job的状态一致性
	allocListResponse, err := s.AllocationList(ctx, &pb.ScheduleAllocationListRequest{
		JobId: r.Id,
	})
	if err != nil {
		return nil, err
	}
	if allocListResponse == nil || allocListResponse.Data == nil {
		return nil, domain.NewErr1101None("任务所属分配信息", r.Id)
	}
	var ids []string
	for _, item := range allocListResponse.Data {
		ids = append(ids, item.Id)
	}
	if len(ids) == 0 {
		_, err = s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleJobStatusUpdate: r})
		if err != nil {
			return nil, err
		}
	} else {
		request := &pb.ScheduleAllocationStatusUpdateRequest{
			Ids:         ids,
			Status:      r.Status,
			Description: r.Description,
		}
		// update job must by allocation
		_, err = s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleAllocationStatusUpdate: request})
		if err != nil {
			return nil, err
		}
	}
	jobDetail.Status = r.Status
	jobDetail.Info = r.Description
	jobStatusResponse := &pb.ScheduleJobStatusUpdateResponse{
		Header: newHeader(s),
		Data:   jobDetail,
	}
	return jobStatusResponse, nil
}

func (s *EtcdServer) JobDelete(ctx context.Context, r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleJobDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleJobDeleteResponse), nil
}

func (s *EtcdServer) JobDetail(ctx context.Context, r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error) {
	resp, err := s.ScheduleStore().JobDetail(r)
	// resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleJobDetail: r})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

func (s *EtcdServer) EvalAdd(ctx context.Context, r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error) {
	//if err := s.evalBroker.OutstandingReset(r.PreviousEval, r.EvalToken); err != nil {
	//	return nil, err
	//}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleEvalAdd: r})
	if err != nil {
		return nil, err
	}
	// 队列相关逻辑放这里
	eval := convertEvalFromRequest(r)
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		resp, err := s.queueOperator.EvalBrokerQueue(eval)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.EvalQueuePrefix
			resp, err := schedulerhttp.EvalQueueHTTP(cctx, eval, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Debug("eval queue from server ,the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	// return nil, ErrCanceled
	return resp.(*pb.ScheduleEvalAddResponse), nil
}

func (s *EtcdServer) EvalList(ctx context.Context, r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleEvalList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleEvalListResponse), nil
}

func (s *EtcdServer) EvalUpdate(ctx context.Context, r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	//if err := s.evalBroker.OutstandingReset(r.Id, r.EvalToken); err != nil {
	//	return nil, err
	//}
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleEvalStatusUpdate: r})
	if err != nil {
		return nil, err
	}
	// 队列相关逻辑放这里
	eval := domain.ConvertEvaluationFromPb(resp.(*pb.ScheduleEvalStatusUpdateResponse).Data)
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		_, err := s.queueOperator.EvalBrokerQueue(eval)
		if err != nil {
			return nil, err
		}
		return resp.(*pb.ScheduleEvalStatusUpdateResponse), nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.EvalQueuePrefix
			_, err := schedulerhttp.EvalQueueHTTP(cctx, eval, lurl, s.peerRt)
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp.(*pb.ScheduleEvalStatusUpdateResponse), err
			}
			s.Logger().Debug("eval queue from server ,the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return resp.(*pb.ScheduleEvalStatusUpdateResponse), nil
}

func (s *EtcdServer) EvalDelete(ctx context.Context, r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleEvalDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleEvalDeleteResponse), nil
}

func (s *EtcdServer) EvalDetail(ctx context.Context, r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error) {
	resp, err := s.ScheduleStore().EvalDetail(r)
	// resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleEvalDetail: r})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	// return resp.(*pb.ScheduleEvalDetailResponse), nil
	return resp, nil
}

func (s *EtcdServer) AllocationAdd(ctx context.Context, r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error) {
	// 1 写入planAllocation
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleAllocationAdd: r})
	if err != nil {
		return nil, err
	}
	// 2 写入Evaluation
	if r.EvaluationPreemption != nil && len(r.EvaluationPreemption) > 0 {
		for _, eval := range r.EvaluationPreemption {
			// TODO 槽位不够的情况
			evalDetail, err := s.EvalDetail(ctx, &pb.ScheduleEvalDetailRequest{Id: eval.Id})
			if err != nil {
				s.Logger().Warn("can't get eval detail by id", zap.String("eval-id", eval.Id))
			}
			if evalDetail.Data == nil {
				// add
				evalRequest := &pb.ScheduleEvalAddRequest{
					Id:          eval.Id,
					Namespace:   eval.Namespace,
					Priority:    eval.Priority,
					Type:        eval.Type,
					TriggeredBy: eval.TriggeredBy,
					JobId:       eval.JobId,
					PlanId:      eval.PlanId,
					Status:      eval.Status,
				}
				_, err = s.EvalAdd(ctx, evalRequest)
				if err != nil {
					s.Logger().Warn("add eval false", zap.Error(err))
				}
			} else {
				// update
				evalRequest := &pb.ScheduleEvalStatusUpdateRequest{
					Id:                eval.Id,
					Status:            eval.Status,
					StatusDescription: eval.StatusDescription,
				}
				_, err := s.EvalUpdate(ctx, evalRequest)
				if err != nil {
					s.Logger().Warn("update eval false", zap.Error(err))
				}
			}

		}
	}
	return resp.(*pb.ScheduleAllocationAddResponse), nil
}

func (s *EtcdServer) AllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error) {
	resp, err := s.ScheduleStore().AllocationList(r)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

func (s *EtcdServer) AllocationUpdate(ctx context.Context, r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleAllocationStatusUpdate: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleAllocationStatusUpdateResponse), nil
}

func (s *EtcdServer) AllocationDelete(ctx context.Context, r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleAllocationDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ScheduleAllocationDeleteResponse), nil
}

func (s *EtcdServer) AllocationDetail(ctx context.Context, r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error) {
	resp, err := s.ScheduleStore().AllocationDetail(r)
	// resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleAllocationDetail: r})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
	// return resp.(*pb.ScheduleAllocationDetailResponse), nil
}

func (s *EtcdServer) SimpleAllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error) {
	resp, err := s.ScheduleStore().SimpleAllocationList(r)
	// resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{ScheduleSimpleAllocationList: r})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

func (s *EtcdServer) EvalDequeue(ctx context.Context, r *pb.ScheduleEvalDequeueRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalDequeueResponse, error) {
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		resp, err := s.queueOperator.EvalBrokerDequeue(r)
		if err != nil {
			return nil, err
		}
		if resp != nil {
			resp.Header = newHeader(s)
		}
		return resp, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.EvalDequeuePrefix
			resp, err := schedulerhttp.EvalDequeueHTTP(cctx, r.Schedulers, r.Timeout, r.Namespace, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Debug("eval dequeue from server ,the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

// ack require from queue
func (s *EtcdServer) EvalAck(ctx context.Context, r *pb.ScheduleEvalAckRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalAckResponse, error) {
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		if err := s.queueOperator.EvalBrokerAck(r.Id, r.Token); err != nil {
			return nil, err
		}
		return &pb.ScheduleEvalAckResponse{
			Header: newHeader(s),
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.EvalAckPrefix
			resp, err := schedulerhttp.EvalAckHTTP(cctx, r.Id, r.Token, r.Namespace, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) EvalNack(ctx context.Context, r *pb.ScheduleEvalNackRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalNackResponse, error) {
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		if err := s.queueOperator.EvalBrokerNack(r.Id, r.Token); err != nil {
			return nil, err
		}
		return &pb.ScheduleEvalNackResponse{
			Header: newHeader(s),
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.EvalNackPrefix
			resp, err := schedulerhttp.EvalNackHTTP(cctx, r.Id, r.Token, r.Namespace, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) NodeAdd(ctx context.Context, r *pb.NodeAddRequest, opts ...grpc.CallOption) (*pb.NodeAddResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{NodeAdd: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeAddResponse), nil
}

func (s *EtcdServer) NodeList(ctx context.Context, r *pb.NodeListRequest, opts ...grpc.CallOption) (*pb.NodeListResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{NodeList: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeListResponse), nil
}

func (s *EtcdServer) NodeUpdate(ctx context.Context, r *pb.NodeUpdateRequest, opts ...grpc.CallOption) (*pb.NodeUpdateResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{NodeUpdate: r})
	if err != nil {
		return nil, err
	}
	// 将旧的eval绑定到node上的进行处理
	var nodeNow *domain.Node
	response := resp.(*pb.NodeUpdateResponse)
	if response.Data != nil {
		nodeNow = domain.ConvertNodeFromPb(response.Data)
		if domain.ShouldDrainNode(r.Status) || nodeStatusTransitionRequiresEval(r.Status, nodeNow.Status) {
			// create eval for new node ready and begin allocation plans
			allocResp, err := s.AllocationList(ctx, &pb.ScheduleAllocationListRequest{
				NodeId: r.ID,
				Status: fmt.Sprintf("%s,%s", constant.AllocClientStatusPending, constant.AllocClientStatusRunning),
			})
			if err != nil {
				return nil, err
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
					planResp, err := s.PlanDetail(ctx, &pb.SchedulePlanDetailRequest{
						Id:        alloc.PlanId,
						Namespace: alloc.Namespace,
					})
					if err != nil {
						return nil, err
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
					_, err = s.EvalAdd(ctx, eval)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return resp.(*pb.NodeUpdateResponse), nil
}

func (s *EtcdServer) NodeDelete(ctx context.Context, r *pb.NodeDeleteRequest, opts ...grpc.CallOption) (*pb.NodeDeleteResponse, error) {
	resp, err := s.raftRequest(ctx, pb.InternalRaftRequest{NodeDelete: r})
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeDeleteResponse), nil
}

func (s *EtcdServer) NodeDetail(ctx context.Context, r *pb.NodeDetailRequest, opts ...grpc.CallOption) (*pb.NodeDetailResponse, error) {
	resp, err := s.ScheduleStore().NodeDetail(r)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		resp.Header = newHeader(s)
	}
	return resp, nil
}

// 仅仅入队列
func (s *EtcdServer) PlanAllocationEnqueue(ctx context.Context, r *pb.PlanAllocationEnqueueRequest, opts ...grpc.CallOption) (*pb.PlanAllocationEnqueueResponse, error) {
	if s.isLeader() {
		if err := s.waitAppliedIndex(); err != nil {
			return nil, err
		}
		allocs, err := s.queueOperator.AllocationEnqueue(r)
		if err != nil {
			return nil, err
		}
		return &pb.PlanAllocationEnqueueResponse{
			Header: newHeader(s),
			Data:   allocs,
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.PlanAllocationAddPrefix
			s.Logger().Debug("the request is ", zap.Reflect("request", r))
			resp, err := schedulerhttp.PlanAllocationAddHTTP(cctx, r, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(10 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

// 查看队列情况
func (s *EtcdServer) QueueDetail(ctx context.Context, r *pb.QueueDetailRequest, opts ...grpc.CallOption) (*pb.QueueDetailResponse, error) {
	if s.isLeader() {
		data, err := s.queueOperator.QueueStats(r.Type)
		if err != nil {
			return nil, err
		}
		return &pb.QueueDetailResponse{
			Header: newHeader(s),
			Data:   data,
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.QueueDetailPrefix
			s.Logger().Debug("the request is ", zap.Reflect("request", r))
			resp, err := schedulerhttp.QueueDetailHTTP(cctx, r, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) QueueJobViewDetail(ctx context.Context, r *pb.QueueJobViewRequest, opts ...grpc.CallOption) (*pb.QueueJobViewResponse, error) {
	if s.isLeader() {
		data, err := s.queueOperator.QueueJobViewStats(r.Namespace)
		if err != nil {
			return nil, err
		}
		return &pb.QueueJobViewResponse{
			Header: newHeader(s),
			Data:   data,
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.QueueJobViewDetailPrefix
			s.Logger().Debug("the request is ", zap.Reflect("request", r))
			resp, err := schedulerhttp.QueueJobViewDetailHTTP(cctx, r, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) QueueForbiddenUpdate(ctx context.Context, r *pb.QueueForbiddenRequest, opts ...grpc.CallOption) (*pb.QueueForbiddenResponse, error) {
	if s.isLeader() {
		err := s.revokeLeadership()
		if err != nil {
			return nil, err
		}
		data, err := s.queueOperator.QueueStatusDetail()
		if err != nil {
			return nil, err
		}
		return &pb.QueueForbiddenResponse{
			Header: newHeader(s),
			Data:   data,
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.QueueForbiddenPrefix
			s.Logger().Debug("the request is ", zap.Reflect("request", r))
			resp, err := schedulerhttp.QueueForbiddenUpdateHTTP(cctx, r, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) QueueStatusDetail(ctx context.Context, r *pb.QueueStatusRequest, opts ...grpc.CallOption) (*pb.QueueStatusResponse, error) {
	if s.isLeader() {
		data, err := s.queueOperator.QueueStatusDetail()
		if err != nil {
			return nil, err
		}
		return &pb.QueueStatusResponse{
			Header: newHeader(s),
			Data:   data,
		}, nil
	}
	// I am a flower then request from leader
	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()
	for cctx.Err() == nil {
		leader, lerr := s.waitLeader(cctx)
		if lerr != nil {
			return nil, lerr
		}
		for _, url := range leader.PeerURLs {
			lurl := url + schedulerhttp.QueueStatusPrefix
			s.Logger().Debug("the request is ", zap.Reflect("request", r))
			resp, err := schedulerhttp.QueueStatusDetailHTTP(cctx, r, lurl, s.peerRt)
			if resp != nil {
				resp.Header = newHeader(s)
			}
			if err == nil || err == schedulerhttp.ErrScheduleNotFound {
				return resp, err
			}
			s.Logger().Info("the err", zap.Error(err))
		}
		// Throttle in case of e.g. connection problems.
		time.Sleep(50 * time.Millisecond)
	}
	if cctx.Err() == context.DeadlineExceeded {
		return nil, ErrTimeout
	}
	return nil, ErrCanceled
}

func (s *EtcdServer) raftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	result, err := s.processInternalRaftRequestOnce(ctx, r)
	if err != nil {
		return nil, err
	}
	if result.err != nil {
		return nil, result.err
	}
	if startTime, ok := ctx.Value(traceutil.StartTimeKey).(time.Time); ok && result.trace != nil {
		applyStart := result.trace.GetStartTime()
		// The trace object is created in apply. Here reset the start time to trace
		// the raft request time by the difference between the request start time
		// and apply start time
		result.trace.SetStartTime(startTime)
		result.trace.InsertStep(0, applyStart, "process raft request")
		result.trace.LogIfLong(traceThreshold)
	}
	return result.resp, nil
}

func (s *EtcdServer) raftRequest(ctx context.Context, r pb.InternalRaftRequest) (proto.Message, error) {
	return s.raftRequestOnce(ctx, r)
}

// doSerialize handles the auth logic, with permissions checked by "chk", for a serialized request "get". Returns a non-nil error on authentication failure.
func (s *EtcdServer) doSerialize(ctx context.Context, chk func(*auth.AuthInfo) error, get func()) error {
	trace := traceutil.Get(ctx)
	ai, err := s.AuthInfoFromCtx(ctx)
	if err != nil {
		return err
	}
	if ai == nil {
		// chk expects non-nil AuthInfo; use empty credentials
		ai = &auth.AuthInfo{}
	}
	if err = chk(ai); err != nil {
		return err
	}
	trace.Step("get authentication metadata")
	// fetch response for serialized request
	get()
	// check for stale token revision in case the auth store was updated while
	// the request has been handled.
	if ai.Revision != 0 && ai.Revision != s.authStore.Revision() {
		return auth.ErrAuthOldRevision
	}
	return nil
}

func (s *EtcdServer) processInternalRaftRequestOnce(ctx context.Context, r pb.InternalRaftRequest) (*applyResult, error) {
	ai := s.getAppliedIndex()
	ci := s.getCommittedIndex()
	if ci > ai+maxGapBetweenApplyAndCommitIndex {
		return nil, ErrTooManyRequests
	}

	r.Header = &pb.RequestHeader{
		ID: s.reqIDGen.Next(),
	}

	// check authinfo if it is not InternalAuthenticateRequest
	if r.Authenticate == nil {
		authInfo, err := s.AuthInfoFromCtx(ctx)
		if err != nil {
			return nil, err
		}
		if authInfo != nil {
			r.Header.Username = authInfo.Username
			r.Header.AuthRevision = authInfo.Revision
		}
	}

	data, err := r.Marshal()
	if err != nil {
		return nil, err
	}

	if len(data) > int(s.Cfg.MaxRequestBytes) {
		return nil, ErrRequestTooLarge
	}

	id := r.ID
	if id == 0 {
		id = r.Header.ID
	}
	ch := s.w.Register(id)

	cctx, cancel := context.WithTimeout(ctx, s.Cfg.ReqTimeout())
	defer cancel()

	start := time.Now()
	err = s.r.Propose(cctx, data)
	if err != nil {
		proposalsFailed.Inc()
		s.w.Trigger(id, nil) // GC wait
		return nil, err
	}
	proposalsPending.Inc()
	defer proposalsPending.Dec()

	select {
	case x := <-ch:
		return x.(*applyResult), nil
	case <-cctx.Done():
		proposalsFailed.Inc()
		s.w.Trigger(id, nil) // GC wait
		return nil, s.parseProposeCtxErr(cctx.Err(), start)
	case <-s.done:
		return nil, ErrStopped
	}
}

// Watchable returns a watchable interface attached to the etcdserver.
func (s *EtcdServer) Watchable() mvcc.WatchableKV { return s.KV() }

func (s *EtcdServer) linearizableReadLoop() {
	for {
		requestId := s.reqIDGen.Next()
		leaderChangedNotifier := s.LeaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			continue
		case <-s.readwaitc:
		case <-s.stopping:
			return
		}

		// as a single loop is can unlock multiple reads, it is not very useful
		// to propagate the trace from Txn or Range.
		trace := traceutil.New("linearizableReadLoop", s.Logger())

		nextnr := newNotifier()
		s.readMu.Lock()
		nr := s.readNotifier
		s.readNotifier = nextnr
		s.readMu.Unlock()

		confirmedIndex, err := s.requestCurrentIndex(leaderChangedNotifier, requestId)
		if isStopped(err) {
			return
		}
		if err != nil {
			nr.notify(err)
			continue
		}

		trace.Step("read index received")

		trace.AddField(traceutil.Field{Key: "readStateIndex", Value: confirmedIndex})

		appliedIndex := s.getAppliedIndex()
		trace.AddField(traceutil.Field{Key: "appliedIndex", Value: strconv.FormatUint(appliedIndex, 10)})

		if appliedIndex < confirmedIndex {
			select {
			case <-s.applyWait.Wait(confirmedIndex):
			case <-s.stopping:
				return
			}
		}
		// unblock all l-reads requested at indices before confirmedIndex
		nr.notify(nil)
		trace.Step("applied index is now lower than readState.Index")

		trace.LogAllStepsIfLong(traceThreshold)
	}
}

func isStopped(err error) bool {
	return err == raft.ErrStopped || err == ErrStopped
}

func (s *EtcdServer) requestCurrentIndex(leaderChangedNotifier <-chan struct{}, requestId uint64) (uint64, error) {
	err := s.sendReadIndex(requestId)
	if err != nil {
		return 0, err
	}

	lg := s.Logger()
	errorTimer := time.NewTimer(s.Cfg.ReqTimeout())
	defer errorTimer.Stop()
	retryTimer := time.NewTimer(readIndexRetryTime)
	defer retryTimer.Stop()

	firstCommitInTermNotifier := s.FirstCommitInTermNotify()

	for {
		select {
		case rs := <-s.r.readStateC:
			requestIdBytes := uint64ToBigEndianBytes(requestId)
			gotOwnResponse := bytes.Equal(rs.RequestCtx, requestIdBytes)
			if !gotOwnResponse {
				// a previous request might time out. now we should ignore the response of it and
				// continue waiting for the response of the current requests.
				responseId := uint64(0)
				if len(rs.RequestCtx) == 8 {
					responseId = binary.BigEndian.Uint64(rs.RequestCtx)
				}
				lg.Warn(
					"ignored out-of-date read index response; local node read indexes queueing up and waiting to be in sync with leader",
					zap.Uint64("sent-request-id", requestId),
					zap.Uint64("received-request-id", responseId),
				)
				slowReadIndex.Inc()
				continue
			}
			return rs.Index, nil
		case <-leaderChangedNotifier:
			readIndexFailed.Inc()
			// return a retryable error.
			return 0, ErrLeaderChanged
		case <-firstCommitInTermNotifier:
			firstCommitInTermNotifier = s.FirstCommitInTermNotify()
			lg.Info("first commit in current term: resending ReadIndex request")
			err := s.sendReadIndex(requestId)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(readIndexRetryTime)
			continue
		case <-retryTimer.C:
			lg.Warn(
				"waiting for ReadIndex response took too long, retrying",
				zap.Uint64("sent-request-id", requestId),
				zap.Duration("retry-timeout", readIndexRetryTime),
			)
			err := s.sendReadIndex(requestId)
			if err != nil {
				return 0, err
			}
			retryTimer.Reset(readIndexRetryTime)
			continue
		case <-errorTimer.C:
			lg.Warn(
				"timed out waiting for read index response (local node might have slow network)",
				zap.Duration("timeout", s.Cfg.ReqTimeout()),
			)
			slowReadIndex.Inc()
			return 0, ErrTimeout
		case <-s.stopping:
			return 0, ErrStopped
		}
	}
}

func uint64ToBigEndianBytes(number uint64) []byte {
	byteResult := make([]byte, 8)
	binary.BigEndian.PutUint64(byteResult, number)
	return byteResult
}

func (s *EtcdServer) sendReadIndex(requestIndex uint64) error {
	ctxToSend := uint64ToBigEndianBytes(requestIndex)

	cctx, cancel := context.WithTimeout(context.Background(), s.Cfg.ReqTimeout())
	err := s.r.ReadIndex(cctx, ctxToSend)
	cancel()
	if err == raft.ErrStopped {
		return err
	}
	if err != nil {
		lg := s.Logger()
		lg.Warn("failed to get read index from Raft", zap.Error(err))
		readIndexFailed.Inc()
		return err
	}
	return nil
}

func (s *EtcdServer) LinearizableReadNotify(ctx context.Context) error {
	return s.linearizableReadNotify(ctx)
}

func (s *EtcdServer) linearizableReadNotify(ctx context.Context) error {
	s.readMu.RLock()
	nc := s.readNotifier
	s.readMu.RUnlock()

	// signal linearizable loop for current notify if it hasn't been already
	select {
	case s.readwaitc <- struct{}{}:
	default:
	}

	// wait for read state notification
	select {
	case <-nc.c:
		return nc.err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		return ErrStopped
	}
}

func (s *EtcdServer) AuthInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error) {
	authInfo, err := s.AuthStore().AuthInfoFromCtx(ctx)
	if authInfo != nil || err != nil {
		return authInfo, err
	}
	if !s.Cfg.ClientCertAuthEnabled {
		return nil, nil
	}
	authInfo = s.AuthStore().AuthInfoFromTLS(ctx)
	return authInfo, nil
}

func (s *EtcdServer) Downgrade(ctx context.Context, r *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	switch r.Action {
	case pb.DowngradeRequest_VALIDATE:
		return s.downgradeValidate(ctx, r.Version)
	case pb.DowngradeRequest_ENABLE:
		return s.downgradeEnable(ctx, r)
	case pb.DowngradeRequest_CANCEL:
		return s.downgradeCancel(ctx)
	default:
		return nil, ErrUnknownMethod
	}
}

func (s *EtcdServer) downgradeValidate(ctx context.Context, v string) (*pb.DowngradeResponse, error) {
	resp := &pb.DowngradeResponse{}

	targetVersion, err := convertToClusterVersion(v)
	if err != nil {
		return nil, err
	}

	// gets leaders commit index and wait for local store to finish applying that index
	// to avoid using stale downgrade information
	err = s.linearizableReadNotify(ctx)
	if err != nil {
		return nil, err
	}

	cv := s.ClusterVersion()
	if cv == nil {
		return nil, ErrClusterVersionUnavailable
	}
	resp.Version = cv.String()

	allowedTargetVersion := membership.AllowedDowngradeVersion(cv)
	if !targetVersion.Equal(*allowedTargetVersion) {
		return nil, ErrInvalidDowngradeTargetVersion
	}

	downgradeInfo := s.cluster.DowngradeInfo()
	if downgradeInfo.Enabled {
		// Todo: return the downgrade status along with the error msg
		return nil, ErrDowngradeInProcess
	}
	return resp, nil
}

func (s *EtcdServer) downgradeEnable(ctx context.Context, r *pb.DowngradeRequest) (*pb.DowngradeResponse, error) {
	// validate downgrade capability before starting downgrade
	v := r.Version
	lg := s.Logger()
	if resp, err := s.downgradeValidate(ctx, v); err != nil {
		lg.Warn("reject downgrade request", zap.Error(err))
		return resp, err
	}
	targetVersion, err := convertToClusterVersion(v)
	if err != nil {
		lg.Warn("reject downgrade request", zap.Error(err))
		return nil, err
	}

	raftRequest := membershippb.DowngradeInfoSetRequest{Enabled: true, Ver: targetVersion.String()}
	_, err = s.raftRequest(ctx, pb.InternalRaftRequest{DowngradeInfoSet: &raftRequest})
	if err != nil {
		lg.Warn("reject downgrade request", zap.Error(err))
		return nil, err
	}
	resp := pb.DowngradeResponse{Version: s.ClusterVersion().String()}
	return &resp, nil
}

func (s *EtcdServer) downgradeCancel(ctx context.Context) (*pb.DowngradeResponse, error) {
	// gets leaders commit index and wait for local store to finish applying that index
	// to avoid using stale downgrade information
	if err := s.linearizableReadNotify(ctx); err != nil {
		return nil, err
	}

	downgradeInfo := s.cluster.DowngradeInfo()
	if !downgradeInfo.Enabled {
		return nil, ErrNoInflightDowngrade
	}

	raftRequest := membershippb.DowngradeInfoSetRequest{Enabled: false}
	_, err := s.raftRequest(ctx, pb.InternalRaftRequest{DowngradeInfoSet: &raftRequest})
	if err != nil {
		return nil, err
	}
	resp := pb.DowngradeResponse{Version: s.ClusterVersion().String()}
	return &resp, nil
}

// leaderPeriodicLoop 主节点变换之后的调度逻辑
func (s *EtcdServer) leaderPeriodicLoop() {
	for {
		leaderChangedNotifier := s.LeaderChangedNotify()
		select {
		case <-leaderChangedNotifier:
			// do the leader changed logic
			if s.isLeader() {
				s.Logger().Info("i am a leader,receive leader changed message, do something...")
				s.leaderLoop(s.stopping)
			} else {
				s.Logger().Info("i am not a leader,receive leader changed message, do something revoke...")
				if err := s.revokeLeadership(); err != nil {
					s.Logger().Error("failed to revoke leadership", zap.Error(err))
				}
			}
		case <-s.stopping:
			return
		}
	}
}

func (s *EtcdServer) holdLeaderQueueStatus() {
	ticker := time.NewTicker(monitorQueueInterval)
	defer ticker.Stop()
	retryTimes := 0
	for {
		select {
		case <-s.stopping:
			return
		case <-ticker.C:
			// 每5秒钟检查一下是否leader，是否开启了队列
			if s.isLeader() {
				data, err := s.queueOperator.QueueStatusDetail()
				if err != nil {
					s.Logger().Error("failed to get queue status", zap.Error(err))
					time.Sleep(5 * time.Second)
					continue
				}
				if data.AllocationQueue == "off" || data.EvalBroker == "off" || data.BlockedEvals == "off" {
					s.Logger().Warn("the jobpool queue status has some exception")
					retryTimes++
					time.Sleep(1 * time.Second)
					if retryTimes >= 5 {
						s.Logger().Info("i am a leader,repair the queue in my node, do something...")
						s.leaderLoop(s.stopping)
						retryTimes = 0
					}
				}
			}
		}
	}
}
