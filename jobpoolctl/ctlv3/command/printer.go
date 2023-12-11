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

package command

import (
	"errors"
	"fmt"
	"strings"

	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	v3 "yunli.com/jobpool/client/v2"
	"yunli.com/jobpool/pkg/v2/cobrautl"

	"github.com/dustin/go-humanize"
)

type printer interface {
	Del(v3.DeleteResponse)
	Get(v3.GetResponse)
	Put(v3.PutResponse)
	Txn(v3.TxnResponse)
	Watch(v3.WatchResponse)

	Grant(r v3.LeaseGrantResponse)
	Revoke(id v3.LeaseID, r v3.LeaseRevokeResponse)
	KeepAlive(r v3.LeaseKeepAliveResponse)
	TimeToLive(r v3.LeaseTimeToLiveResponse, keys bool)
	Leases(r v3.LeaseLeasesResponse)

	MemberAdd(v3.MemberAddResponse)
	MemberRemove(id uint64, r v3.MemberRemoveResponse)
	MemberUpdate(id uint64, r v3.MemberUpdateResponse)
	MemberPromote(id uint64, r v3.MemberPromoteResponse)
	MemberList(v3.MemberListResponse)

	EndpointHealth([]epHealth)
	EndpointStatus([]epStatus)
	EndpointHashKV([]epHashKV)
	MoveLeader(leader, target uint64, r v3.MoveLeaderResponse)

	Alarm(v3.AlarmResponse)

	RoleAdd(role string, r v3.AuthRoleAddResponse)
	RoleGet(role string, r v3.AuthRoleGetResponse)
	RoleDelete(role string, r v3.AuthRoleDeleteResponse)
	RoleList(v3.AuthRoleListResponse)
	RoleGrantPermission(role string, r v3.AuthRoleGrantPermissionResponse)
	RoleRevokePermission(role string, key string, end string, r v3.AuthRoleRevokePermissionResponse)

	UserAdd(user string, r v3.AuthUserAddResponse)
	UserGet(user string, r v3.AuthUserGetResponse)
	UserList(r v3.AuthUserListResponse)
	UserChangePassword(v3.AuthUserChangePasswordResponse)
	UserGrantRole(user string, role string, r v3.AuthUserGrantRoleResponse)
	UserRevokeRole(user string, role string, r v3.AuthUserRevokeRoleResponse)
	UserDelete(user string, r v3.AuthUserDeleteResponse)

	AuthStatus(r v3.AuthStatusResponse)

	PlanList(r pb.SchedulePlanListResponse) // schedule start
	PlanDetail(r pb.SchedulePlanDetailResponse)
	JobList(r pb.ScheduleJobListResponse) // schedule start
	JobDetail(r pb.ScheduleJobDetailResponse)
	JobStatusUpdate(r pb.ScheduleJobStatusUpdateResponse)
	NamespaceList(r pb.ScheduleNameSpaceListResponse) // schedule ns
	NamespaceAdd(ns string, response pb.ScheduleNameSpaceAddResponse)
	NamespaceDelete(ns string, response pb.ScheduleNameSpaceDeleteResponse)
	EvalList(r pb.ScheduleEvalListResponse) // schedule eval
	EvalDetail(r pb.ScheduleEvalDetailResponse)
	AllocationList(r pb.ScheduleAllocationListResponse) // schedule allocation
	AllocationDetail(r pb.ScheduleAllocationDetailResponse)
}

func NewPrinter(printerType string, isHex bool) printer {
	switch printerType {
	case "simple":
		return &simplePrinter{isHex: isHex}
	case "fields":
		return &fieldsPrinter{newPrinterUnsupported("fields")}
	case "json":
		return newJSONPrinter(isHex)
	case "protobuf":
		return newPBPrinter()
	case "table":
		return &tablePrinter{newPrinterUnsupported("table")}
	}
	return nil
}

type printerRPC struct {
	printer
	p func(interface{})
}

func (p *printerRPC) Del(r v3.DeleteResponse)  { p.p((*pb.DeleteRangeResponse)(&r)) }
func (p *printerRPC) Get(r v3.GetResponse)     { p.p((*pb.RangeResponse)(&r)) }
func (p *printerRPC) Put(r v3.PutResponse)     { p.p((*pb.PutResponse)(&r)) }
func (p *printerRPC) Txn(r v3.TxnResponse)     { p.p((*pb.TxnResponse)(&r)) }
func (p *printerRPC) Watch(r v3.WatchResponse) { p.p(&r) }

func (p *printerRPC) Grant(r v3.LeaseGrantResponse)                      { p.p(r) }
func (p *printerRPC) Revoke(id v3.LeaseID, r v3.LeaseRevokeResponse)     { p.p(r) }
func (p *printerRPC) KeepAlive(r v3.LeaseKeepAliveResponse)              { p.p(r) }
func (p *printerRPC) TimeToLive(r v3.LeaseTimeToLiveResponse, keys bool) { p.p(&r) }
func (p *printerRPC) Leases(r v3.LeaseLeasesResponse)                    { p.p(&r) }

func (p *printerRPC) MemberAdd(r v3.MemberAddResponse) { p.p((*pb.MemberAddResponse)(&r)) }
func (p *printerRPC) MemberRemove(id uint64, r v3.MemberRemoveResponse) {
	p.p((*pb.MemberRemoveResponse)(&r))
}
func (p *printerRPC) MemberUpdate(id uint64, r v3.MemberUpdateResponse) {
	p.p((*pb.MemberUpdateResponse)(&r))
}
func (p *printerRPC) MemberList(r v3.MemberListResponse) { p.p((*pb.MemberListResponse)(&r)) }
func (p *printerRPC) Alarm(r v3.AlarmResponse)           { p.p((*pb.AlarmResponse)(&r)) }
func (p *printerRPC) MoveLeader(leader, target uint64, r v3.MoveLeaderResponse) {
	p.p((*pb.MoveLeaderResponse)(&r))
}
func (p *printerRPC) RoleAdd(_ string, r v3.AuthRoleAddResponse) { p.p((*pb.AuthRoleAddResponse)(&r)) }
func (p *printerRPC) RoleGet(_ string, r v3.AuthRoleGetResponse) { p.p((*pb.AuthRoleGetResponse)(&r)) }
func (p *printerRPC) RoleDelete(_ string, r v3.AuthRoleDeleteResponse) {
	p.p((*pb.AuthRoleDeleteResponse)(&r))
}
func (p *printerRPC) RoleList(r v3.AuthRoleListResponse) { p.p((*pb.AuthRoleListResponse)(&r)) }
func (p *printerRPC) RoleGrantPermission(_ string, r v3.AuthRoleGrantPermissionResponse) {
	p.p((*pb.AuthRoleGrantPermissionResponse)(&r))
}
func (p *printerRPC) RoleRevokePermission(_ string, _ string, _ string, r v3.AuthRoleRevokePermissionResponse) {
	p.p((*pb.AuthRoleRevokePermissionResponse)(&r))
}
func (p *printerRPC) UserAdd(_ string, r v3.AuthUserAddResponse) { p.p((*pb.AuthUserAddResponse)(&r)) }
func (p *printerRPC) UserGet(_ string, r v3.AuthUserGetResponse) { p.p((*pb.AuthUserGetResponse)(&r)) }
func (p *printerRPC) UserList(r v3.AuthUserListResponse)         { p.p((*pb.AuthUserListResponse)(&r)) }
func (p *printerRPC) UserChangePassword(r v3.AuthUserChangePasswordResponse) {
	p.p((*pb.AuthUserChangePasswordResponse)(&r))
}
func (p *printerRPC) UserGrantRole(_ string, _ string, r v3.AuthUserGrantRoleResponse) {
	p.p((*pb.AuthUserGrantRoleResponse)(&r))
}
func (p *printerRPC) UserRevokeRole(_ string, _ string, r v3.AuthUserRevokeRoleResponse) {
	p.p((*pb.AuthUserRevokeRoleResponse)(&r))
}
func (p *printerRPC) UserDelete(_ string, r v3.AuthUserDeleteResponse) {
	p.p((*pb.AuthUserDeleteResponse)(&r))
}
func (p *printerRPC) AuthStatus(r v3.AuthStatusResponse) {
	p.p((*pb.AuthStatusResponse)(&r))
}

// schedule
func (p *printerRPC) PlanList(r pb.SchedulePlanListResponse) { p.p((*pb.SchedulePlanListResponse)(&r)) }

func (p *printerRPC) PlanDetail(r pb.SchedulePlanDetailResponse) {
	p.p(&r)
}

func (p *printerRPC) JobList(r pb.ScheduleJobListResponse) { p.p((*pb.ScheduleJobListResponse)(&r)) }
func (p *printerRPC) NamespaceList(r pb.ScheduleNameSpaceListResponse) {
	p.p((*pb.ScheduleNameSpaceListResponse)(&r))
}
func (p *printerRPC) NamespaceAdd(ns string, r pb.ScheduleNameSpaceAddResponse) {
	p.p((*pb.ScheduleNameSpaceAddResponse)(&r))
}
func (p *printerRPC) NamespaceDelete(ns string, r pb.ScheduleNameSpaceDeleteResponse) {
	p.p(&r)
}
func (p *printerRPC) EvalList(r pb.ScheduleEvalListResponse) { p.p((*pb.ScheduleEvalListResponse)(&r)) }
func (p *printerRPC) AllocationList(r pb.ScheduleAllocationListResponse) {
	p.p((*pb.ScheduleAllocationListResponse)(&r))
}

type printerUnsupported struct{ printerRPC }

func newPrinterUnsupported(n string) printer {
	f := func(interface{}) {
		cobrautl.ExitWithError(cobrautl.ExitBadFeature, errors.New(n+" not supported as output format"))
	}
	return &printerUnsupported{printerRPC{nil, f}}
}

func (p *printerUnsupported) EndpointHealth([]epHealth) { p.p(nil) }
func (p *printerUnsupported) EndpointStatus([]epStatus) { p.p(nil) }
func (p *printerUnsupported) EndpointHashKV([]epHashKV) { p.p(nil) }

func (p *printerUnsupported) MoveLeader(leader, target uint64, r v3.MoveLeaderResponse) { p.p(nil) }

func makeMemberListTable(r v3.MemberListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Status", "Name", "Peer Addrs", "Client Addrs", "Is Learner"}
	for _, m := range r.Members {
		status := "started"
		if len(m.Name) == 0 {
			status = "unstarted"
		}
		isLearner := "false"
		if m.IsLearner {
			isLearner = "true"
		}
		rows = append(rows, []string{
			fmt.Sprintf("%x", m.ID),
			status,
			m.Name,
			strings.Join(m.PeerURLs, ","),
			strings.Join(m.ClientURLs, ","),
			isLearner,
		})
	}
	return hdr, rows
}

func makeEndpointHealthTable(healthList []epHealth) (hdr []string, rows [][]string) {
	hdr = []string{"endpoint", "health", "took", "error"}
	for _, h := range healthList {
		rows = append(rows, []string{
			h.Ep,
			fmt.Sprintf("%v", h.Health),
			h.Took,
			h.Error,
		})
	}
	return hdr, rows
}

func makeEndpointStatusTable(statusList []epStatus) (hdr []string, rows [][]string) {
	hdr = []string{"endpoint", "ID", "version", "db size", "is leader", "is learner", "raft term",
		"raft index", "raft applied index", "errors"}
	for _, status := range statusList {
		rows = append(rows, []string{
			status.Ep,
			fmt.Sprintf("%x", status.Resp.Header.MemberId),
			status.Resp.Version,
			humanize.Bytes(uint64(status.Resp.DbSize)),
			fmt.Sprint(status.Resp.Leader == status.Resp.Header.MemberId),
			fmt.Sprint(status.Resp.IsLearner),
			fmt.Sprint(status.Resp.RaftTerm),
			fmt.Sprint(status.Resp.RaftIndex),
			fmt.Sprint(status.Resp.RaftAppliedIndex),
			fmt.Sprint(strings.Join(status.Resp.Errors, ", ")),
		})
	}
	return hdr, rows
}

func makeEndpointHashKVTable(hashList []epHashKV) (hdr []string, rows [][]string) {
	hdr = []string{"endpoint", "hash"}
	for _, h := range hashList {
		rows = append(rows, []string{
			h.Ep,
			fmt.Sprint(h.Resp.Hash),
		})
	}
	return hdr, rows
}

func makePlanListTable(r pb.SchedulePlanListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Status", "Name", "Type", "Namespace", "Priority", "Synchronous", "Desc", "Spec", "Launch time", "Create time", "Update time"}
	for _, m := range r.Data {
		isSyn := "false"
		if m.Synchronous {
			isSyn = "true"
		}
		launch := ""
		if m.LaunchTime != nil {
			launch = m.LaunchTime.String()
		}
		rows = append(rows, []string{
			m.Id,
			m.Status,
			m.Name,
			m.Type,
			m.Namespace,
			fmt.Sprintf("%d", m.Priority),
			isSyn,
			m.Description,
			m.Periodic.Spec,
			launch,
			m.CreateTime.String(),
			m.UpdateTime.String(),
		})
	}
	return hdr, rows
}

func makePlanDetailTable(r pb.SchedulePlanDetailResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Status", "Name", "Type", "Namespace", "Priority", "Synchronous", "Desc", "Spec", "Launch time", "Create time", "Update time"}
	m := r.Data
	isSyn := "false"
	if m.Synchronous {
		isSyn = "true"
	}
	launch := ""
	if m.LaunchTime != nil {
		launch = m.LaunchTime.String()
	}
	rows = append(rows, []string{
		m.Id,
		m.Status,
		m.Name,
		m.Type,
		m.Namespace,
		fmt.Sprintf("%d", m.Priority),
		isSyn,
		m.Description,
		m.Periodic.Spec,
		launch,
		m.CreateTime.String(),
		m.UpdateTime.String(),
	})
	return hdr, rows
}

func makeJobListTable(r pb.ScheduleJobListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Status", "Name", "Type", "Namespace", "Plan id", "Derived Plan", "Timeout", "Info", "Create time", "Update time"}
	for _, m := range r.Data {
		rows = append(rows, []string{
			m.Id,
			m.Status,
			m.Name,
			m.Type,
			m.Namespace,
			m.PlanId,
			m.DerivedPlanId,
			fmt.Sprintf("%d", m.Timeout),
			m.Info,
			m.CreateTime.String(),
			m.UpdateTime.String(),
		})
	}
	return hdr, rows
}

func makeNamespaceListTable(r pb.ScheduleNameSpaceListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"Name"}
	for _, m := range r.Data {
		rows = append(rows, []string{
			m.Name,
		})
	}
	return hdr, rows
}

func makeEvalListTable(r pb.ScheduleEvalListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Status", "Type", "Namespace", "Plan id", "Job id", "Triggered", "Priority", "Create time", "Update time"}
	for _, m := range r.Data {
		rows = append(rows, []string{
			m.Id,
			m.Status,
			m.Type,
			m.Namespace,
			m.PlanId,
			m.JobId,
			m.TriggeredBy,
			fmt.Sprintf("%d", m.Priority),
			m.CreateTime.String(),
			m.UpdateTime.String(),
		})
	}
	return hdr, rows
}

func makeAllocationListTable(r pb.ScheduleAllocationListResponse) (hdr []string, rows [][]string) {
	hdr = []string{"ID", "Status", "Name", "Namespace", "Plan id", "Job id", "Eval id", "Node id", "Info", "Create time", "Update time"}
	for _, m := range r.Data {
		rows = append(rows, []string{
			m.Id,
			m.ClientStatus,
			m.Name,
			m.Namespace,
			m.PlanId,
			m.JobId,
			m.EvalId,
			m.NodeId,
			m.ClientDescription,
			m.CreateTime.String(),
			m.UpdateTime.String(),
		})
	}
	return hdr, rows
}
