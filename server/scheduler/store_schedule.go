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
	"encoding/binary"
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
	auth "yunli.com/jobpool/server/v2/auth"
	"yunli.com/jobpool/server/v2/mvcc/backend"
	"yunli.com/jobpool/server/v2/mvcc/buckets"
)

var (
	revisionKey = []byte("scheduleRevision")
)

const (
	revBytesLen = 8
)

type ScheduleStore interface {
	Close() error

	Recover(b backend.Backend)

	PlanList(r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error)
	PlanAdd(r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error)
	PlanUpdate(r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error)
	PlanDelete(r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error)
	PlanDetail(r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error)
	PlanOnline(r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error)
	PlanOffline(r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error)
	NameSpaceAdd(r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error)
	NameSpaceList(r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error)
	NameSpaceDelete(r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error)
	NameSpaceDetail(r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error)
	NameSpaceUpdate(r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error)
	LaunchUpsert(id string, namespace string, launch time.Time) (*schedulepb.Launch, error)
	LaunchDetail(id string, namespace string) (*timestamppb.Timestamp, error)
	JobAdd(r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error)
	JobList(r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error)
	JobUpdate(r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error)
	JobDelete(r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error)
	JobDetail(r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error)
	JobExist(r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error)
	EvalAdd(r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error)
	EvalList(r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error)
	EvalUpdate(r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error)
	EvalDelete(r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error)
	EvalDetail(r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error)
	AllocationAdd(r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error)
	AllocationList(r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error)
	AllocationUpdate(r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error)
	AllocationDelete(r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error)
	AllocationDetail(r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error)
	SimpleAllocationList(r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error)
	NodeAdd(r *pb.NodeAddRequest, ttl time.Duration) (*pb.NodeAddResponse, error)
	NodeList(r *pb.NodeListRequest) (*pb.NodeListResponse, error)
	NodeUpdate(r *pb.NodeUpdateRequest, ttl time.Duration) (*pb.NodeUpdateResponse, error)
	NodeDelete(r *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error)
	NodeDetail(r *pb.NodeDetailRequest) (*pb.NodeDetailResponse, error)
	RunningAllocsByNamespace(namespace string) ([]*domain.Allocation, error) // for worker
	PlanByID(namespace string, id string) (*domain.Plan, error)
	JobByID(id string) (*domain.Job, error)
	NodeByID(id string) (*domain.Node, error)
	Nodes() ([]*domain.Node, error)
	UnhealthAllocsByPlan(namespace string, id string, b bool) ([]*domain.Allocation, error)
}

type scheduleStore struct {
	// atomic operations; need 64-bit align, or 32-bit tests will crash
	revision uint64

	lg         *zap.Logger
	be         backend.Backend
	enabled    bool
	enabledMu  sync.RWMutex
	bcryptCost int // the algorithm cost / strength for hashing auth passwords
}

func NewScheduleStore(lg *zap.Logger, be backend.Backend, bcryptCost int) *scheduleStore {
	if lg == nil {
		lg = zap.NewNop()
	}

	if bcryptCost < bcrypt.MinCost || bcryptCost > bcrypt.MaxCost {
		lg.Warn(
			"use default bcrypt cost instead of the invalid given cost",
			zap.Int("min-cost", bcrypt.MinCost),
			zap.Int("max-cost", bcrypt.MaxCost),
			zap.Int("default-cost", bcrypt.DefaultCost),
			zap.Int("given-cost", bcryptCost),
		)
		bcryptCost = bcrypt.DefaultCost
	}

	tx := be.BatchTx()
	tx.LockOutsideApply()

	tx.UnsafeCreateBucket(buckets.Schedule)
	tx.UnsafeCreateBucket(buckets.SchedulePlans)
	tx.UnsafeCreateBucket(buckets.ScheduleNameSpaces)
	tx.UnsafeCreateBucket(buckets.ScheduleJobs)
	tx.UnsafeCreateBucket(buckets.ScheduleEvaluations)
	tx.UnsafeCreateBucket(buckets.ScheduleAllocations)
	tx.UnsafeCreateBucket(buckets.Node)
	tx.UnsafeCreateBucket(buckets.ScheduleLaunches)

	as := &scheduleStore{
		revision:   getScheduleRevision(tx),
		lg:         lg,
		be:         be,
		enabled:    false,
		bcryptCost: bcryptCost,
	}

	if as.Revision() == 0 {
		as.commitRevision(tx)
	}

	// as.refreshRangePermCache(tx)

	tx.Unlock()
	be.ForceCommit()

	return as
}

// schedule相关业务逻辑
func (as *scheduleStore) PlanList(r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()

	plans := getAllPlans(as.lg, tx, r.Namespace, &SchedulerFilter{
		Id:        r.Id,
		Name:      r.Name,
		Namespace: r.Namespace,
		Status:    r.Status,
	}, r.Synchronous)
	tx.Unlock()

	// 分页操作
	pageResponse := auth.GetPage(plans, auth.PageRequest{
		PageNumber: r.PageNumber,
		PageSize:   r.PageSize,
	})
	//参数转换
	data := make([]*schedulepb.Plan, reflect.ValueOf(pageResponse.Data).Len())
	for i := 0; i < reflect.ValueOf(pageResponse.Data).Len(); i++ {
		data[i] = pageResponse.Data.([]*schedulepb.Plan)[i]
		tm, _ := as.LaunchDetail(data[i].Id, data[i].Namespace)
		if tm != nil {
			data[i].LaunchTime = tm
		}
	}
	resp := &pb.SchedulePlanListResponse{
		Data:          data,
		TotalElements: pageResponse.TotalElements,
		TotalPages:    pageResponse.TotalPages,
	}
	return resp, nil
}

func (as *scheduleStore) PlanAdd(r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error) {
	if len(r.Name) == 0 {
		return nil, domain.NewErr1001Blank("name")
	}
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	plan := getPlan(as.lg, tx, r.Id, r.Namespace)
	if plan != nil {
		return nil, domain.NewErr1100Exist("plan", r.Id)
	}

	newPlan := &schedulepb.Plan{
		Id:          r.Id,
		Name:        r.Name,
		Type:        r.Type,
		Namespace:   r.Namespace,
		Priority:    r.Priority,
		Stop:        r.Stop,
		Synchronous: r.Synchronous,
		Status:      r.Status,
		Description: r.Description,
		Parameters:  r.Parameters,
		Periodic:    r.Periodic,
		CreateTime:  timestamppb.TimestampNow(),
		UpdateTime:  timestamppb.TimestampNow(),
	}
	putPlan(as.lg, tx, newPlan)

	as.commitRevision(tx)
	// TODO
	// as.refreshRangePermCache(tx)

	as.lg.Debug("added a plan", zap.String("plan-name", r.Name))
	return &pb.SchedulePlanAddResponse{
		Data: newPlan,
	}, nil
}

func getPlan(lg *zap.Logger, tx backend.ReadTx, id string, namespace string) *schedulepb.Plan {
	_, vs := tx.UnsafeRange(buckets.SchedulePlans, []byte(fmt.Sprintf("%s_%s", namespace, id)), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	plan := &schedulepb.Plan{}
	err := plan.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'schedulepb.Plan'",
			zap.String("plan-id", id),
			zap.Error(err),
		)
	}
	return plan
}

func existPlanByName(lg *zap.Logger, tx backend.ReadTx, name string, namespace string) *schedulepb.Plan {
	var vs [][]byte
	_, vs = tx.UnsafeRangeStart(buckets.SchedulePlans, []byte(namespace), -1)
	if len(vs) == 0 {
		return nil
	}
	for i := range vs {
		plan := &schedulepb.Plan{}
		err := plan.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Plan'", zap.Error(err))
		}
		if plan.Name == name {
			return plan
		}
	}
	return nil
}

func getAllPlans(lg *zap.Logger, tx backend.ReadTx, namespace string, filter *SchedulerFilter, synchronous int32) []*schedulepb.Plan {
	var vs [][]byte
	_, vs = tx.UnsafeRangeStart(buckets.SchedulePlans, []byte(namespace), -1)
	if len(vs) == 0 {
		return nil
	}
	var plans PlanByCreateDesc
	for i := range vs {
		plan := &schedulepb.Plan{}
		err := plan.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Plan'", zap.Error(err))
		}
		if filter != nil {
			if filter.Id != "" && plan.Id != "" && filter.Id != plan.Id {
				continue
			}
			if filter.Name != "" && !strings.Contains(plan.Name, filter.Name) {
				continue
			}
			if filter.Status != "" && plan.Status != "" && filter.Status != plan.Status {
				continue
			}
		}
		if synchronous != 0 {
			if synchronous == 1 && !plan.Synchronous {
				continue
			}
			if synchronous == 2 && plan.Synchronous {
				continue
			}
		}
		plans = append(plans, plan)
	}
	sort.Sort(plans)
	return plans
}

func putPlan(lg *zap.Logger, tx backend.BatchTx, plan *schedulepb.Plan) {
	plan.UpdateTime = timestamppb.TimestampNow()
	b, err := plan.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'schedulepb.PlanAllocation'", zap.Error(err))
	}
	tx.UnsafePut(buckets.SchedulePlans, []byte(fmt.Sprintf("%s_%s", plan.Namespace, plan.Id)), b)
}

func (as *scheduleStore) commitRevision(tx backend.BatchTx) {
	atomic.AddUint64(&as.revision, 1)
	revBytes := make([]byte, revBytesLen)
	binary.BigEndian.PutUint64(revBytes, as.Revision())
	tx.UnsafePut(buckets.Schedule, revisionKey, revBytes)
}

func getScheduleRevision(tx backend.ReadTx) uint64 {
	_, vs := tx.UnsafeRange(buckets.Schedule, revisionKey, nil, 0)
	if len(vs) != 1 {
		// this can happen in the initialization phase
		return 0
	}
	return binary.BigEndian.Uint64(vs[0])
}

func (as *scheduleStore) Revision() uint64 {
	return atomic.LoadUint64(&as.revision)
}

func (as *scheduleStore) Close() error {
	as.enabledMu.Lock()
	defer as.enabledMu.Unlock()
	if !as.enabled {
		return nil
	}
	return nil
}

func (as *scheduleStore) setRevision(rev uint64) {
	atomic.StoreUint64(&as.revision, rev)
}

func (as *scheduleStore) Recover(be backend.Backend) {
	enabled := false
	as.be = be
	tx := be.ReadTx()
	tx.Lock()
	as.setRevision(getRevision(tx))
	// as.refreshRangePermCache(tx)

	tx.Unlock()
	as.enabledMu.Lock()
	as.enabled = enabled
	as.enabledMu.Unlock()
}

func getRevision(tx backend.ReadTx) uint64 {
	_, vs := tx.UnsafeRange(buckets.Auth, revisionKey, nil, 0)
	if len(vs) != 1 {
		// this can happen in the initialization phase
		return 0
	}
	return binary.BigEndian.Uint64(vs[0])
}

func (as *scheduleStore) PlanUpdate(r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	plan := getPlan(as.lg, tx, r.Id, r.Namespace)
	if plan == nil {
		return nil, domain.NewErr1101None("plan", r.Id)
	}
	updatedPlan := plan
	if r.Name != "" && r.Name != plan.Name {
		updatedPlan.Name = r.Name
	}
	if r.Type != "" && r.Type != plan.Type {
		updatedPlan.Type = r.Type
	}
	if r.Namespace != "" && r.Namespace != plan.Namespace {
		updatedPlan.Namespace = r.Namespace
	}
	if r.Priority != 0 && r.Priority != plan.Priority {
		updatedPlan.Priority = r.Priority
	}
	if &r.Synchronous != nil && r.Synchronous != plan.Synchronous {
		updatedPlan.Synchronous = r.Synchronous
	}
	if r.Description != "" && r.Description != plan.Description {
		updatedPlan.Description = r.Description
	}
	if r.Parameters != "" && r.Parameters != plan.Parameters {
		updatedPlan.Parameters = r.Parameters
	}
	if r.Periodic != nil {
		if plan.Periodic != nil {
			if r.Periodic.Spec != "" && r.Periodic.Spec != plan.Periodic.Spec {
				updatedPlan.Periodic.Spec = r.Periodic.Spec
			}
			if r.Periodic.TimeZone != "" && r.Periodic.TimeZone != plan.Periodic.TimeZone {
				updatedPlan.Periodic.TimeZone = r.Periodic.TimeZone
			}
			if r.Periodic.StartTime != nil && r.Periodic.StartTime != plan.Periodic.StartTime {
				updatedPlan.Periodic.StartTime = r.Periodic.StartTime
			}
			if r.Periodic.EndTime != nil && r.Periodic.EndTime != plan.Periodic.EndTime {
				updatedPlan.Periodic.EndTime = r.Periodic.EndTime
			}
			if r.Periodic.Enabled != false && r.Periodic.Enabled != plan.Periodic.Enabled {
				updatedPlan.Periodic.Enabled = r.Periodic.Enabled
			}
		} else {
			updatedPlan.Priority = r.Priority
		}
	}
	if r.Status != "" && r.Status != plan.Status {
		// 状态更改的一致性
		updatedPlan.Status = r.Status
		if constant.PlanStatusDead == r.Status {
			if updatedPlan.Periodic != nil {
				updatedPlan.Periodic.Enabled = false
			}
			updatedPlan.Stop = true
		} else {
			if updatedPlan.Periodic != nil && !updatedPlan.Periodic.Enabled {
				updatedPlan.Periodic.Enabled = true
			}
			updatedPlan.Stop = false
		}
	}
	putPlan(as.lg, tx, updatedPlan)
	as.commitRevision(tx)
	as.lg.Debug(
		"changed a info of a plan",
		zap.String("plan-name", r.Name),
		zap.String("plan-id", r.Id),
	)
	return &pb.SchedulePlanUpdateResponse{
		Data: updatedPlan,
	}, nil
}

func (as *scheduleStore) PlanDelete(r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	plan := getPlan(as.lg, tx, r.Id, r.Namespace)
	if plan == nil {
		return nil, domain.NewErr1101None("plan", r.Id)
	}
	delPlan(tx, r.Id, r.Namespace)
	as.commitRevision(tx)
	as.lg.Debug(
		"deleted a plan",
		zap.String("plan-name", plan.Name),
		zap.String("plan-id", r.Id),
	)
	return &pb.SchedulePlanDeleteResponse{}, nil
}

func delPlan(tx backend.BatchTx, planId string, namespace string) {
	tx.UnsafeDelete(buckets.SchedulePlans, []byte(fmt.Sprintf("%s_%s", namespace, planId)))
}

func (as *scheduleStore) PlanDetail(r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	plan := getPlan(as.lg, tx, r.Id, r.Namespace)
	tx.Unlock()
	if plan == nil {
		return nil, domain.NewErr1101None("plan", r.Id)
	}
	tm, _ := as.LaunchDetail(plan.Id, plan.Namespace)
	if tm != nil {
		plan.LaunchTime = tm
	}
	var resp pb.SchedulePlanDetailResponse
	resp.Data = plan
	return &resp, nil
}

func (as *scheduleStore) PlanOnline(r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	plan := getPlan(as.lg, tx, r.Id, r.Namespace)
	if plan == nil {
		return nil, domain.NewErr1101None("plan", r.Id)
	}
	updatedPlan := plan
	if r.Description != "" && r.Description != plan.Description {
		updatedPlan.Description = r.Description
	}
	updatedPlan.Status = constant.PlanStatusRunning
	updatedPlan.Stop = false
	if updatedPlan.Periodic != nil {
		updatedPlan.Periodic.Enabled = true
	}
	putPlan(as.lg, tx, updatedPlan)
	as.commitRevision(tx)
	as.lg.Debug(
		"online of a plan",
		zap.String("plan-name", updatedPlan.Name),
		zap.String("plan-id", r.Id),
		zap.String("plan-status", updatedPlan.Status),
	)
	return &pb.SchedulePlanDetailResponse{
		Data: updatedPlan,
	}, nil
}

func (as *scheduleStore) PlanOffline(r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	plan := getPlan(as.lg, tx, r.Id, r.Namespace)
	if plan == nil {
		return nil, domain.NewErr1101None("plan", r.Id)
	}
	updatedPlan := plan
	if r.Description != "" && r.Description != plan.Description {
		updatedPlan.Description = r.Description
	}
	updatedPlan.Status = constant.PlanStatusDead
	updatedPlan.Stop = true
	if updatedPlan.Periodic != nil {
		updatedPlan.Periodic.Enabled = false
	}
	putPlan(as.lg, tx, updatedPlan)
	as.commitRevision(tx)
	as.lg.Debug(
		"offline of a plan",
		zap.String("plan-name", updatedPlan.Name),
		zap.String("plan-id", r.Id),
		zap.String("plan-status", updatedPlan.Status),
	)
	return &pb.SchedulePlanDetailResponse{
		Data: updatedPlan,
	}, nil
}

func (as *scheduleStore) NameSpaceAdd(r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error) {
	if len(r.Name) == 0 {
		return nil, auth.ErrUserEmpty
	}

	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	// namespace格式校验操作
	if checkNameSpaceFormat(r.Name) {
		return nil, domain.NewErr1010Format("namespace", r.Name)
	}
	space := getNameSpace(as.lg, tx, r.Name)
	if space != nil {
		return nil, domain.NewErr1100Exist("namespace", r.Name)
	}

	newPlan := &schedulepb.NameSpace{
		Name: r.Name,
	}
	putNameSpace(as.lg, tx, newPlan)

	as.commitRevision(tx)
	// TODO
	// as.refreshRangePermCache(tx)

	as.lg.Debug("added a namespace", zap.String("namespace-name", r.Name))
	return &pb.ScheduleNameSpaceAddResponse{}, nil
}

/**
 * namespace格式校验操作
 */
func checkNameSpaceFormat(name string) bool {
	//name必须由小写英文、下划线、数字随意组合，且首写必须是小写英文字母 则报错
	if len(name) == 0 {
		return true
	}
	if name[0] < 'a' || name[0] > 'z' {
		return true
	}
	for _, c := range name {
		if c >= 'a' && c <= 'z' {
			continue
		}
		if c >= '0' && c <= '9' {
			continue
		}
		if c == '_' {
			continue
		}
		if c == '-' {
			continue
		}
		return true
	}
	return false
}

func getNameSpace(lg *zap.Logger, tx backend.ReadTx, namespace string) *schedulepb.NameSpace {
	_, vs := tx.UnsafeRange(buckets.ScheduleNameSpaces, []byte(fmt.Sprintf("%s", namespace)), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	space := &schedulepb.NameSpace{}
	err := space.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'schedulepb.NameSpace'",
			zap.String("NameSpace-Name", namespace),
			zap.Error(err),
		)
	}
	return space
}

func getAllNameSpaces(lg *zap.Logger, tx backend.ReadTx, namespace string) []*schedulepb.NameSpace {
	var vs [][]byte
	_, vs = tx.UnsafeRangeStart(buckets.ScheduleNameSpaces, []byte(namespace), -1)
	if len(vs) == 0 {
		return nil
	}

	spaces := make([]*schedulepb.NameSpace, len(vs))
	for i := range vs {
		space := &schedulepb.NameSpace{}
		err := space.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.NameSpace'", zap.Error(err))
		}
		spaces[i] = space
	}
	return spaces
}

func putNameSpace(lg *zap.Logger, tx backend.BatchTx, namespace *schedulepb.NameSpace) {
	b, err := namespace.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'schedulepb.NameSpace'", zap.Error(err))
	}
	tx.UnsafePut(buckets.ScheduleNameSpaces, []byte(fmt.Sprintf("%s", namespace.Name)), b)
}

func (as *scheduleStore) NameSpaceUpdate(r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	// namespace格式校验操作
	if checkNameSpaceFormat(r.Name) {
		return nil, domain.NewErr1010Format("namespace", r.Name)
	}
	// 判断oldName是否存在,如果不存在则报错
	space := getNameSpace(as.lg, tx, r.OldName)
	if space == nil {
		return nil, domain.NewErr1101None("namespace", r.OldName)
	}
	// 判断newName是否存在,如果存在则报错
	if getNameSpace(as.lg, tx, r.Name) != nil {
		return nil, domain.NewErr1100Exist("namespace", r.Name)
	}

	// 判断名称是否一致
	if r.Name != "" && r.Name != space.Name {
		space.Name = r.Name
		delNameSpace(tx, r.OldName)
	}
	putNameSpace(as.lg, tx, space)
	as.commitRevision(tx)
	as.lg.Debug(
		"changed a info of a namespace",
		zap.String("namespace-name", r.Name),
	)
	return &pb.ScheduleNameSpaceUpdateResponse{}, nil
}

func (as *scheduleStore) NameSpaceDelete(r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	space := getNameSpace(as.lg, tx, r.Name)
	if space == nil {
		return nil, domain.NewErr1101None("namespace", r.Name)
	}
	delNameSpace(tx, r.Name)
	as.commitRevision(tx)
	as.lg.Debug(
		"deleted a namespace",
		zap.String("namespace-name", space.Name),
	)
	return &pb.ScheduleNameSpaceDeleteResponse{}, nil
}

func delNameSpace(tx backend.BatchTx, name string) {
	tx.UnsafeDelete(buckets.ScheduleNameSpaces, []byte(name))
}

func (as *scheduleStore) NameSpaceList(r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	spaces := getAllNameSpaces(as.lg, tx, r.Name)
	tx.Unlock()

	// 分页操作
	pageResponse := auth.GetPage(spaces, auth.PageRequest{
		PageNumber: r.PageNumber,
		PageSize:   r.PageSize,
	})

	resp := &pb.ScheduleNameSpaceListResponse{
		Data:          pageResponse.Data.([]*schedulepb.NameSpace),
		TotalElements: pageResponse.TotalElements,
		TotalPages:    pageResponse.TotalPages,
	}
	return resp, nil
}

func (as *scheduleStore) NameSpaceDetail(r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	space := getNameSpace(as.lg, tx, r.Name)
	tx.Unlock()
	if space == nil {
		return nil, domain.NewErr1101None("namespace", r.Name)
	}
	var resp pb.ScheduleNameSpaceDetailResponse
	resp.Data = space
	return &resp, nil
}

// launch
func (as *scheduleStore) LaunchUpsert(id string, namespace string, launch time.Time) (*schedulepb.Launch, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	exist := getLaunchById(as.lg, tx, id, namespace)
	if exist != nil {
		exist.Launch, _ = timestamppb.TimestampProto(launch)
	} else {
		exist := &schedulepb.Launch{
			Id:        id,
			Namespace: namespace,
		}
		exist.Launch, _ = timestamppb.TimestampProto(launch)
	}
	putLaunch(as.lg, tx, exist)
	as.commitRevision(tx)
	as.lg.Debug("added a launch", zap.String("launch-id", id))
	return exist, nil
}

func (as *scheduleStore) LaunchDetail(id string, namespace string) (*timestamppb.Timestamp, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	launch := getLaunchById(as.lg, tx, id, namespace)
	tx.Unlock()
	if launch == nil {
		return nil, domain.NewErr1101None("launch", id)
	}
	if launch.Launch != nil {
		return launch.Launch, nil
	} else {
		return nil, nil
	}
}

func putLaunch(lg *zap.Logger, tx backend.BatchTx, l *schedulepb.Launch) {
	b, err := l.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'schedulepb.Launch'", zap.Error(err))
	}
	tx.UnsafePut(buckets.ScheduleLaunches, []byte(l.Id), b)
}
func getLaunchById(lg *zap.Logger, tx backend.ReadTx, id string, namespace string) *schedulepb.Launch {
	_, vs := tx.UnsafeRange(buckets.ScheduleLaunches, []byte(id), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	launch := &schedulepb.Launch{}
	err := launch.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'schedulepb.Launch'",
			zap.String("Launch-id", id),
			zap.Error(err),
		)
	}
	return launch
}

func (as *scheduleStore) JobAdd(r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error) {
	if len(r.Id) == 0 {
		return nil, domain.NewErr1001Blank("id")
	}

	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	job := getJob(as.lg, tx, r.Id)
	if job != nil {
		return nil, domain.NewErr1100Exist("job", r.Id)
	}
	newJob := &schedulepb.Job{
		Id:            r.Id,
		PlanId:        r.PlanId,
		DerivedPlanId: r.DerivedPlanId,
		Name:          r.Name,
		Type:          r.Type,
		Namespace:     r.Namespace,
		OperatorId:    r.OperatorId,
		Timeout:       r.Timeout,
		Status:        r.Status,
		Parameters:    r.Parameters,
		Info:          r.Info,
		CreateTime:    r.CreateTime,
		UpdateTime:    r.UpdateTime,
	}
	putJob(as.lg, tx, newJob)
	// add launch for query launch time by plan
	putLaunch(as.lg, tx, &schedulepb.Launch{
		Id:        r.PlanId,
		Namespace: r.Namespace,
		Launch:    r.CreateTime,
	})
	as.commitRevision(tx)
	as.lg.Debug("added a job", zap.String("job-name", r.Name))
	return &pb.ScheduleJobAddResponse{
		Data: newJob,
	}, nil
}

func (as *scheduleStore) JobList(r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	jobs := getAllJobs(as.lg, tx, &SchedulerFilter{
		Id:        r.Id,
		Name:      r.Name,
		Namespace: r.Namespace,
		Status:    r.Status,
		PlanId:    r.PlanId,
		End:       r.EndTime,
		Start:     r.StartTime,
	})
	tx.Unlock()
	var data []*schedulepb.Job
	for _, job := range jobs {
		if r.Name != "" && job.Name != r.Name {
			continue
		}
		data = append(data, job)
	}

	// 分页操作
	pageResponse := auth.GetPage(data, auth.PageRequest{
		PageNumber: r.PageNumber,
		PageSize:   r.PageSize,
	})

	resp := &pb.ScheduleJobListResponse{
		Data:          pageResponse.Data.([]*schedulepb.Job),
		TotalElements: pageResponse.TotalElements,
		TotalPages:    pageResponse.TotalPages,
	}

	return resp, nil
}

func (as *scheduleStore) JobUpdate(r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	job := getJob(as.lg, tx, r.Id)
	if job == nil {
		return nil, domain.NewErr1101None("job", r.Id)
	}
	updatedJob := job
	if r.Description != "" && r.Description != job.Info {
		updatedJob.Info = r.Description
	}
	if r.Status != "" && r.Status != job.Status {
		updatedJob.Status = r.Status
		if constant.JobStatusComplete == r.Status {
			updatedJob.Info = ""
		}
	}
	updatedJob.UpdateTime = timestamppb.TimestampNow()
	putJob(as.lg, tx, updatedJob)
	as.commitRevision(tx)
	as.lg.Debug(
		"changed a info of a job",
		zap.String("job-name", job.Name),
		zap.String("job-id", r.Id),
	)
	return &pb.ScheduleJobStatusUpdateResponse{
		Data: updatedJob,
	}, nil
}

func (as *scheduleStore) JobDelete(r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	job := getJob(as.lg, tx, r.Id)
	if job == nil {
		return nil, domain.NewErr1101None("job", r.Id)
	}
	delJob(tx, r.Id)
	as.commitRevision(tx)
	as.lg.Debug(
		"deleted a job",
		zap.String("job-name", job.Name),
		zap.String("job-id", r.Id),
	)
	return &pb.ScheduleJobDeleteResponse{}, nil
}

func (as *scheduleStore) JobDetail(r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	job := getJob(as.lg, tx, r.Id)
	tx.Unlock()
	if job == nil {
		return nil, domain.NewErr1101None("job", r.Id)
	}
	var resp pb.ScheduleJobDetailResponse
	resp.Data = job
	return &resp, nil
}

func (as *scheduleStore) JobExist(r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	count := getAllJobCount(as.lg, tx, &SchedulerFilter{
		Id:        r.Id,
		Name:      r.Name,
		Namespace: r.Namespace,
		Status:    r.Status,
		PlanId:    r.PlanId,
		End:       r.EndTime,
		Start:     r.StartTime,
	})
	tx.Unlock()
	resp := &pb.ScheduleJobExistResponse{
		Count: count,
	}

	return resp, nil
}

func getJob(lg *zap.Logger, tx backend.ReadTx, id string) *schedulepb.Job {
	_, vs := tx.UnsafeRange(buckets.ScheduleJobs, []byte(id), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	job := &schedulepb.Job{}
	err := job.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'schedulepb.Job'",
			zap.String("Job-id", id),
			zap.Error(err),
		)
	}
	return job
}

func getAllJobs(lg *zap.Logger, tx backend.ReadTx, filter *SchedulerFilter) []*schedulepb.Job {
	var vs [][]byte
	err := tx.UnsafeForEach(buckets.ScheduleJobs, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		lg.Panic("failed to get jobs",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return nil
	}

	var jobs JobByCreateDesc
	for i := range vs {
		job := &schedulepb.Job{}
		err := job.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Job'", zap.Error(err))
		}
		if filter != nil {
			if filter.Namespace != "" && job.Namespace != "" && filter.Namespace != job.Namespace {
				continue
			}
			if filter.Id != "" && job.Id != "" && filter.Id != job.Id {
				continue
			}
			if filter.PlanId != "" && job.PlanId != "" && filter.PlanId != job.PlanId {
				continue
			}
			if filter.Status != "" && job.Status != "" && !strings.Contains(filter.Status, job.Status) {
				continue
			}
			if filter.End != nil && job.CreateTime != nil && job.CreateTime.Compare(filter.End) > 0 {
				continue
			}
			if filter.Start != nil && job.CreateTime != nil && job.CreateTime.Compare(filter.Start) <= 0 {
				continue
			}
			if filter.Name != "" && !strings.Contains(job.Name, filter.Name) {
				continue
			}
		}
		jobs = append(jobs, job)
	}
	sort.Sort(jobs)
	return []*schedulepb.Job(jobs)
}

func getAllJobCount(lg *zap.Logger, tx backend.ReadTx, filter *SchedulerFilter) int32 {
	var vs [][]byte
	err := tx.UnsafeForEach(buckets.ScheduleJobs, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		lg.Panic("failed to get job count",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return 0
	}
	for i := range vs {
		job := &schedulepb.Job{}
		err := job.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Job'", zap.Error(err))
		}
		if filter != nil {
			if filter.Namespace != "" && job.Namespace != "" && filter.Namespace != job.Namespace {
				continue
			}
			if filter.Id != "" && job.Id != "" && filter.Id != job.Id {
				continue
			}
			if filter.PlanId != "" && job.PlanId != "" && filter.PlanId != job.PlanId {
				continue
			}
			if filter.Status != "" && job.Status != "" && !strings.Contains(filter.Status, job.Status) {
				continue
			}
			if filter.End != nil && job.CreateTime != nil && job.CreateTime.Compare(filter.End) > 0 {
				continue
			}
			if filter.Start != nil && job.CreateTime != nil && job.CreateTime.Compare(filter.Start) <= 0 {
				continue
			}
			if filter.Name != "" && !strings.Contains(job.Name, filter.Name) {
				continue
			}
		}
		return 1
	}
	return 0
}

func putJob(lg *zap.Logger, tx backend.BatchTx, job *schedulepb.Job) {
	b, err := job.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'schedulepb.Job'", zap.Error(err))
	}
	tx.UnsafePut(buckets.ScheduleJobs, []byte(job.Id), b)
}

func delJob(tx backend.BatchTx, jobId string) {
	tx.UnsafeDelete(buckets.ScheduleJobs, []byte(jobId))
}

func (as *scheduleStore) EvalAdd(r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error) {
	if len(r.Id) == 0 {
		return nil, domain.NewErr1001Blank("id")
	}

	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	eval := getEval(as.lg, tx, r.Id)
	if eval != nil {
		return nil, domain.NewErr1100Exist("eval", r.Id)
	}
	newEval := &schedulepb.Evaluation{
		Id:                r.Id,
		PlanId:            r.PlanId,
		Namespace:         r.Namespace,
		Priority:          r.Priority,
		TriggeredBy:       r.TriggeredBy,
		JobId:             r.JobId,
		NodeId:            r.NodeId,
		StatusDescription: r.StatusDescription,
		Type:              r.Type,
		Status:            r.Status,
		CreateTime:        timestamppb.TimestampNow(),
		UpdateTime:        timestamppb.TimestampNow(),
	}
	putEval(as.lg, tx, newEval)
	as.commitRevision(tx)
	as.lg.Debug("added a eval", zap.String("eval-id", r.Id))
	return &pb.ScheduleEvalAddResponse{
		Data: newEval,
	}, nil
}

func (as *scheduleStore) EvalList(r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	filter := &SchedulerFilter{
		Id:        r.Id,
		Namespace: r.Namespace,
		PlanId:    r.PlanId,
		JobId:     r.JobId,
		Status:    r.Status,
		Start:     r.StartTime,
		End:       r.EndTime,
	}
	evals := getAllEvals(as.lg, tx, filter)
	tx.Unlock()
	// 分页操作
	pageResponse := auth.GetPage(evals, auth.PageRequest{
		PageNumber: r.PageNumber,
		PageSize:   r.PageSize,
	})

	resp := &pb.ScheduleEvalListResponse{
		Data:          pageResponse.Data.([]*schedulepb.Evaluation),
		TotalElements: pageResponse.TotalElements,
		TotalPages:    pageResponse.TotalPages,
	}

	return resp, nil
}

func (as *scheduleStore) EvalUpdate(r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	eval := getEval(as.lg, tx, r.Id)
	if eval == nil {
		return nil, domain.NewErr1101None("eval", r.Id)
	}
	updatedEval := eval
	if r.Status != "" && r.Status != eval.Status {
		updatedEval.Status = r.Status
	}
	putEval(as.lg, tx, updatedEval)
	as.commitRevision(tx)
	as.lg.Debug(
		"changed the info of eval",
		zap.String("eval-id", r.Id),
		zap.Reflect("eval", eval),
	)
	return &pb.ScheduleEvalStatusUpdateResponse{
		Data: updatedEval,
	}, nil
}

func (as *scheduleStore) EvalDelete(r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	eval := getEval(as.lg, tx, r.Id)
	if eval == nil {
		return &pb.ScheduleEvalDeleteResponse{}, nil
		// return nil, domain.NewErr1101None("eval", r.Id)
	}
	delEval(tx, r.Id)
	as.commitRevision(tx)
	as.lg.Debug(
		"deleted a eval",
		zap.String("eval-id", r.Id),
	)
	return &pb.ScheduleEvalDeleteResponse{}, nil
}

func (as *scheduleStore) EvalDetail(r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	eval := getEval(as.lg, tx, r.Id)
	tx.Unlock()
	if eval == nil {
		return nil, domain.NewErr1101None("eval", r.Id)
	}
	var resp pb.ScheduleEvalDetailResponse
	resp.Data = eval
	return &resp, nil
}

func getEval(lg *zap.Logger, tx backend.ReadTx, id string) *schedulepb.Evaluation {
	_, vs := tx.UnsafeRange(buckets.ScheduleEvaluations, []byte(id), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	eval := &schedulepb.Evaluation{}
	err := eval.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'schedulepb.Evaluation'",
			zap.String("eval-id", id),
			zap.Error(err),
		)
	}
	return eval
}

func getAllEvals(lg *zap.Logger, tx backend.ReadTx, filter *SchedulerFilter) []*schedulepb.Evaluation {
	var vs [][]byte
	err := tx.UnsafeForEach(buckets.ScheduleEvaluations, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		lg.Panic("failed to get evals",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return nil
	}

	var evals EvalByCreateDesc
	for i := range vs {
		eval := &schedulepb.Evaluation{}
		err := eval.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Evaluation'", zap.Error(err))
		}
		if filter != nil {
			if filter.Id != "" && filter.Id != eval.Id {
				continue
			}
			if filter.Status != "" && filter.Status != eval.Status {
				continue
			}
			if filter.Namespace != "" && filter.Namespace != eval.Namespace {
				continue
			}
			if filter.PlanId != "" && filter.PlanId != eval.PlanId {
				continue
			}
			if filter.JobId != "" && filter.JobId != eval.JobId {
				continue
			}
			if filter.End != nil && eval.CreateTime != nil && eval.CreateTime.Compare(filter.End) > 0 {
				continue
			}
			if filter.Start != nil && eval.CreateTime != nil && eval.CreateTime.Compare(filter.Start) <= 0 {
				continue
			}
		}
		evals = append(evals, eval)
	}
	sort.Sort(evals)
	return []*schedulepb.Evaluation(evals)
}

func getAllEvalsWithFillter(lg *zap.Logger, tx backend.ReadTx, planId string, jobId string, status string) []*schedulepb.Evaluation {
	var vs [][]byte
	err := tx.UnsafeForEach(buckets.ScheduleEvaluations, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		lg.Panic("failed to get evals",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return nil
	}

	var evals EvalByCreateDesc
	for i := range vs {
		eval := &schedulepb.Evaluation{}
		err := eval.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Evaluation'", zap.Error(err))
		}
		if status != "" && eval.Status != status {
			continue
		}
		if planId != "" && eval.PlanId != planId {
			continue
		}
		if jobId != "" && eval.JobId != jobId {
			continue
		}
		evals = append(evals, eval)
	}
	sort.Sort(evals)
	return []*schedulepb.Evaluation(evals)
}

func putEval(lg *zap.Logger, tx backend.BatchTx, eval *schedulepb.Evaluation) {
	b, err := eval.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'schedulepb.Evaluation'", zap.Error(err))
	}
	tx.UnsafePut(buckets.ScheduleEvaluations, []byte(eval.Id), b)
}

func delEval(tx backend.BatchTx, evalId string) {
	tx.UnsafeDelete(buckets.ScheduleEvaluations, []byte(evalId))
}

func (as *scheduleStore) AllocationAdd(r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error) {
	numAllocs := 0
	as.lg.Debug("the request is", zap.Reflect("alloc request", r))

	if r.AllocationUpdated != nil && len(r.AllocationUpdated) > 0 {
		numAllocs += len(r.AllocationUpdated)
	}
	if r.AllocationPreempted != nil && len(r.AllocationPreempted) > 0 {
		numAllocs += len(r.AllocationPreempted)
	}
	if r.AllocationStopped != nil && len(r.AllocationStopped) > 0 {
		numAllocs += len(r.AllocationStopped)
	}
	allocsToUpsert := make([]*schedulepb.Allocation, 0, numAllocs)
	allocsToUpsert = append(allocsToUpsert, r.AllocationUpdated...)
	allocsToUpsert = append(allocsToUpsert, r.AllocationPreempted...)
	allocsToUpsert = append(allocsToUpsert, r.AllocationStopped...)
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	// upsert allocation
	for _, item := range allocsToUpsert {
		exist := getAlloc(as.lg, tx, item.Id)
		if exist == nil {
			putAlloc(as.lg, tx, item)
		} else {
			exist.ClientStatus = item.ClientStatus
			exist.DesiredStatus = item.DesiredStatus
			exist.ClientDescription = item.ClientDescription
			putAlloc(as.lg, tx, exist)
		}
	}
	// upsert evaluation
	for _, eval := range r.EvaluationPreemption {
		exist := getEval(as.lg, tx, eval.Id)
		if exist == nil {
			exist = eval
		} else {
			if eval.Status != "" {
				exist.Status = eval.Status
			}
			if eval.StatusDescription != "" {
				exist.StatusDescription = eval.StatusDescription
			}
			exist.UpdateTime = timestamppb.TimestampNow()
		}
		if exist.Status == constant.EvalStatusComplete {
			// 相同planId的eval且blocked的全部更新为canceled
			blockedEvals := getAllEvalsWithFillter(as.lg, tx, exist.PlanId, "", constant.EvalStatusBlocked)
			if blockedEvals != nil && len(blockedEvals) > 0 {
				for _, blockedEval := range blockedEvals {
					blockedEval.Status = constant.EvalStatusCancelled
					blockedEval.StatusDescription = fmt.Sprintf("evaluation %q successful", exist.Id)
					putEval(as.lg, tx, blockedEval)
				}
			}
		}
		putEval(as.lg, tx, exist)
	}
	as.commitRevision(tx)
	as.lg.Debug("upsert many allocations", zap.Int("allocation-size", len(allocsToUpsert)))
	return &pb.ScheduleAllocationAddResponse{}, nil
}

func (as *scheduleStore) AllocationList(r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	allocations := getAllAllocs(as.lg, tx, &SchedulerFilter{
		Id:        r.Id,
		Name:      r.Name,
		JobId:     r.JobId,
		Status:    r.Status,
		PlanId:    r.PlanId,
		EvalId:    r.EvalId,
		Namespace: r.Namespace,
		NodeId:    r.NodeId,
		Ids:       r.Ids,
		Start:     r.StartTime,
		End:       r.EndTime,
	})
	tx.Unlock()

	// 分页操作
	pageResponse := auth.GetPage(allocations, auth.PageRequest{
		PageNumber: r.PageNumber,
		PageSize:   r.PageSize,
	})

	resp := &pb.ScheduleAllocationListResponse{
		Data:          pageResponse.Data.([]*schedulepb.Allocation),
		TotalElements: pageResponse.TotalElements,
		TotalPages:    pageResponse.TotalPages,
	}
	return resp, nil
}

func (as *scheduleStore) AllocationUpdate(r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	allocIds := r.Ids
	status := r.Status
	description := r.Description
	var jobId string
	needUpdateJob := true
	// 是否有任意alloc更改
	hasUpdateAlloc := false
	// update allocation by ids
	if r.Id != "" {
		allocIds = append(allocIds, r.Id)
	}
	now := timestamppb.TimestampNow()
	var allocUpdateArray []*schedulepb.Allocation
	for _, id := range allocIds {
		allocation := getAlloc(as.lg, tx, id)
		if allocation == nil {
			return nil, domain.NewErr1101None("allocation", r.Id)
		}
		if jobId == "" {
			jobId = allocation.JobId
			if allocation.DesiredStatus != "" {
				needUpdateJob = false
			}
		}
		statusChanged := false
		updateAllocation := allocation
		if r.Status != "" && r.Status != updateAllocation.ClientStatus {
			updateAllocation.ClientStatus = r.Status
			statusChanged = true
		}
		if r.Description != "" && r.Description != updateAllocation.ClientDescription {
			updateAllocation.ClientDescription = r.Description
			statusChanged = true
		}
		if statusChanged {
			// 没有任何改变就不需要更新操作
			updateAllocation.UpdateTime = now
			putAlloc(as.lg, tx, updateAllocation)
			hasUpdateAlloc = true
			allocUpdateArray = append(allocUpdateArray, updateAllocation)
		}
	}
	if !hasUpdateAlloc {
		return &pb.ScheduleAllocationStatusUpdateResponse{
			Data: allocUpdateArray,
		}, nil
	}
	// update job
	if constant.AllocClientStatusUnknown == status {
		status = constant.JobStatusRunning
	}
	if jobId != "" && needUpdateJob {
		job := getJob(as.lg, tx, jobId)
		if job != nil {
			job.Status = status
			if constant.JobStatusComplete == status {
				job.Info = ""
			} else {
				job.Info = description
			}
			job.UpdateTime = now
			putJob(as.lg, tx, job)
		}
	}
	// update other allocations
	if constant.AllocClientStatusFailed == r.Status {
		// 将运行中的其他allocation改为failed
		runningAllocs := getAllAllocs(as.lg, tx, &SchedulerFilter{
			JobId:  jobId,
			Status: constant.AllocClientStatusRunning,
		})
		for _, toBeUpdate := range runningAllocs {
			// 上面有，则表示已经修改过，就不用再更新了
			flagExist := false
			for _, idUpdated := range allocIds {
				if toBeUpdate.Id == idUpdated {
					flagExist = true
					break
				}
			}
			if flagExist {
				continue
			}
			toBeUpdate.ClientStatus = constant.AllocClientStatusFailed
			toBeUpdate.UpdateTime = now
			allocUpdateArray = append(allocUpdateArray, toBeUpdate)
			putAlloc(as.lg, tx, toBeUpdate)
		}
	}
	as.commitRevision(tx)
	as.lg.Debug(
		"changed a info of a allocation",
		zap.Strings("allocation-id", allocIds),
		zap.Reflect("request", r),
	)
	return &pb.ScheduleAllocationStatusUpdateResponse{
		Data: allocUpdateArray,
	}, nil
}

func (as *scheduleStore) AllocationDelete(r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	allocation := getAlloc(as.lg, tx, r.Id)
	if allocation == nil {
		return nil, domain.NewErr1101None("allocation", r.Id)
	}
	delAlloc(tx, r.Id)
	as.commitRevision(tx)
	as.lg.Debug(
		"deleted a allocation",
		zap.String("allocation-id", r.Id),
	)
	return &pb.ScheduleAllocationDeleteResponse{}, nil
}

func (as *scheduleStore) AllocationDetail(r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	allocation := getAlloc(as.lg, tx, r.Id)
	tx.Unlock()
	if allocation == nil {
		return nil, domain.NewErr1101None("allocation", r.Id)
	}
	var resp pb.ScheduleAllocationDetailResponse
	resp.Data = allocation
	return &resp, nil
}

func (as *scheduleStore) SimpleAllocationList(r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error) {
	resp, err := as.AllocationList(r)
	if err != nil {
		return nil, err
	}

	//遍历pageResponse.Data将其转换为SimpleAllocation
	data := make([]*schedulepb.SimpleAllocation, reflect.ValueOf(resp.Data).Len())
	//参数转换
	for i := 0; i < reflect.ValueOf(resp.Data).Len(); i++ {
		data[i] = ConvertSimpleAllocation(resp.Data[i])
	}

	result := &pb.ScheduleSimpleAllocationListResponse{
		Data:          data,
		TotalElements: resp.TotalElements,
		TotalPages:    resp.TotalPages,
	}
	return result, nil
}

func ConvertSimpleAllocation(allocation *schedulepb.Allocation) *schedulepb.SimpleAllocation {
	return &schedulepb.SimpleAllocation{
		Id: allocation.Id,
	}
}

func (as *scheduleStore) RunningAllocsByNamespace(namespace string) ([]*domain.Allocation, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	allocations := getAllAllocs(as.lg, tx, &SchedulerFilter{
		Namespace: namespace,
	})
	tx.Unlock()
	var result []*domain.Allocation
	for i := range allocations {
		item := allocations[i]
		if constant.AllocClientStatusRunning != item.ClientStatus && constant.AllocClientStatusPending != item.ClientStatus {
			continue
		}
		alloc := domain.ConvertAllocation(item)
		// get plan and set plan
		plan, _ := as.PlanByID(namespace, alloc.PlanID)
		if plan != nil {
			alloc.Plan = plan
		}
		result = append(result, alloc)
	}
	return result, nil
}

// UnhealthAllocsByPlan 不健康的分配任务
func (as *scheduleStore) UnhealthAllocsByPlan(namespace string, planId string, b bool) ([]*domain.Allocation, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	allocations := getAllAllocs(as.lg, tx, &SchedulerFilter{
		Namespace: namespace,
		PlanId:    planId,
		Status:    fmt.Sprintf("%s,%s", constant.AllocClientStatusPending, constant.AllocClientStatusRunning),
	})
	tx.Unlock()
	var result []*domain.Allocation
	var normalNodes []string
	nodesResp, _ := as.NodeList(&pb.NodeListRequest{
		Status: constant.NodeStatusReady,
	})
	if nodesResp != nil && nodesResp.Data != nil {
		for _, node := range nodesResp.Data {
			normalNodes = append(normalNodes, node.ID)
		}
	}
	for _, item := range allocations {
		if constant.AllocDesiredStatusStop == item.DesiredStatus || constant.AllocDesiredStatusEvict == item.DesiredStatus {
			// 且alloc的node不正常了
			matchNode := false
			for _, nodeId := range normalNodes {
				if item.NodeId == nodeId {
					matchNode = true
					break
				}
			}
			if !matchNode {
				result = append(result, domain.ConvertAllocation(item))
			}
		}
	}
	return result, nil
}

func getAlloc(lg *zap.Logger, tx backend.ReadTx, id string) *schedulepb.Allocation {
	_, vs := tx.UnsafeRange(buckets.ScheduleAllocations, []byte(id), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	allocation := &schedulepb.Allocation{}
	err := allocation.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'schedulepb.Allocation'",
			zap.String("Job-id", id),
			zap.Error(err),
		)
	}
	return allocation
}

type SchedulerFilter struct {
	Id        string
	Name      string
	Namespace string
	PlanId    string
	JobId     string
	EvalId    string
	Status    string
	NodeId    string
	Ids       []string
	Start     *timestamppb.Timestamp
	End       *timestamppb.Timestamp
}

func getAllAllocs(lg *zap.Logger, tx backend.ReadTx, filter *SchedulerFilter) []*schedulepb.Allocation {
	var vs [][]byte
	err := tx.UnsafeForEach(buckets.ScheduleAllocations, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		lg.Panic("failed to get allocations",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return nil
	}

	var allocations AllocationByCreateDesc
	for i := range vs {
		allocation := &schedulepb.Allocation{}
		err := allocation.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'schedulepb.Allocation'", zap.Error(err))
		}
		if filter != nil {
			if filter.Id != "" && filter.Id != allocation.Id {
				continue
			}
			if filter.Ids != nil && len(filter.Ids) > 0 {
				flagExist := false
				for _, id := range filter.Ids {
					if id == allocation.Id {
						flagExist = true
						break
					}
				}
				if !flagExist {
					continue
				}
			}
			if filter.Namespace != "" && allocation.Namespace != "" && filter.Namespace != allocation.Namespace {
				continue
			}
			if filter.JobId != "" && allocation.JobId != "" && filter.JobId != allocation.JobId {
				continue
			}
			if filter.PlanId != "" && allocation.PlanId != "" && filter.PlanId != allocation.PlanId {
				continue
			}
			if filter.EvalId != "" && (allocation.EvalId == "" || (allocation.EvalId != "" && filter.EvalId != allocation.EvalId)) {
				continue
			}
			if filter.Status != "" && allocation.ClientStatus != "" && !strings.Contains(filter.Status, allocation.ClientStatus) {
				continue
			}
			if filter.End != nil && allocation.CreateTime != nil && allocation.CreateTime.Compare(filter.End) > 0 {
				continue
			}
			if filter.Start != nil && allocation.CreateTime != nil && allocation.CreateTime.Compare(filter.Start) <= 0 {
				continue
			}
			if filter.NodeId != "" && allocation.NodeId != "" && filter.NodeId != allocation.NodeId {
				continue
			}
			if filter.Name != "" && !strings.Contains(allocation.Name, filter.Name) {
				continue
			}
		}
		allocations = append(allocations, allocation)
	}
	sort.Sort(allocations)
	return []*schedulepb.Allocation(allocations)
}

func putAlloc(lg *zap.Logger, tx backend.BatchTx, allocation *schedulepb.Allocation) {
	b, err := allocation.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'schedulepb.Allocation'", zap.Error(err))
	}
	tx.UnsafePut(buckets.ScheduleAllocations, []byte(allocation.Id), b)
}

func delAlloc(tx backend.BatchTx, allocationId string) {
	tx.UnsafeDelete(buckets.ScheduleAllocations, []byte(allocationId))
}

func (as *scheduleStore) PlanByID(namespace string, id string) (*domain.Plan, error) {
	r, err := as.PlanDetail(&pb.SchedulePlanDetailRequest{
		Id:        id,
		Namespace: namespace,
	})
	if err != nil {
		return nil, err
	}
	if r != nil && r.Data != nil {
		pr := r.Data
		plan := &domain.Plan{
			ID:          pr.Id,
			Name:        pr.Name,
			Namespace:   pr.Namespace,
			Type:        pr.Type,
			Priority:    pr.Priority,
			Stop:        pr.Stop,
			Status:      pr.Status,
			Description: pr.Description,
			Synchronous: pr.Synchronous,
		}
		if pr.Parameters != "" {
			plan.Parameters = []byte(pr.Parameters)
		}
		if pr.Periodic != nil {
			periodic := &domain.PeriodicConfig{
				Enabled: pr.Periodic.Enabled,
				Spec:    pr.Periodic.Spec,
			}
			if pr.Periodic.StartTime != nil {
				periodic.StartTime = xtime.NewFormatTime(time.Unix(pr.Periodic.StartTime.Seconds, int64(pr.Periodic.StartTime.Nanos)))
			}
			if pr.Periodic.EndTime != nil {
				periodic.EndTime = xtime.NewFormatTime(time.Unix(pr.Periodic.EndTime.Seconds, int64(pr.Periodic.EndTime.Nanos)))
			}
			plan.Periodic = periodic
		}
		return plan, nil
	} else {
		return nil, domain.NewErr1101None("plan", id)
	}
}

func (as *scheduleStore) JobByID(id string) (*domain.Job, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	job := getJob(as.lg, tx, id)
	tx.Unlock()
	if job == nil {
		return nil, domain.NewErr1101None("job", id)
	}
	return domain.ConvertJob(job), nil
}

func (as *scheduleStore) NodeByID(id string) (*domain.Node, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	node := getNode(as.lg, tx, id)
	tx.Unlock()
	if node == nil {
		return nil, domain.NewErr1101None("node", id)
	}
	return node, nil
}

func (as *scheduleStore) Nodes() ([]*domain.Node, error) {
	resp, err := as.NodeList(&pb.NodeListRequest{})
	if err != nil {
		return nil, err
	}
	nodes := make([]*domain.Node, len(resp.Data))
	for _, item := range resp.Data {
		nodes = append(nodes, domain.ConvertNodeFromPb(item))
	}
	return nodes, nil
}

func (as *scheduleStore) NodeAdd(r *pb.NodeAddRequest, ttl time.Duration) (*pb.NodeAddResponse, error) {
	if len(r.ID) == 0 {
		return nil, domain.NewErr1001Blank("id")
	}
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()

	if r.ID == "" {
		return nil, fmt.Errorf("missing node ID for client registration")
	}
	if r.DataCenter == "" {
		return nil, fmt.Errorf("missing datacenter for client registration")
	}
	if r.Name == "" {
		return nil, fmt.Errorf("missing node name for client registration")
	}

	if !domain.ValidNodeStatus(r.Status) {
		return nil, fmt.Errorf("invalid status for node")
	}
	newNode := domain.ConvertNodeFromRequest(r)
	if r.Status == "" {
		newNode.Status = constant.NodeStatusInit
	}
	if newNode.SchedulingEligibility == "" {
		newNode.SchedulingEligibility = constant.NodeSchedulingEligible
	}
	newNode.StatusUpdatedAt = time.Now().Unix()

	// fetch exist node in db
	nd := getNode(as.lg, tx, r.ID)
	existNode := false
	if nd != nil {
		existNode = true
		if r.SecretId != "" && r.SecretId != nd.SecretID {
			return nil, domain.NewErr1100Exist("node", r.ID)
		}
	}

	putNode(as.lg, tx, newNode)
	as.commitRevision(tx)
	if existNode {
		as.lg.Debug("update a node", zap.String("node-id", r.ID))
	} else {
		as.lg.Debug("register a node", zap.String("node-id", r.ID))
	}

	var ttlMil int64
	if !newNode.TerminalStatus() {
		// TODO  reset heart beat timer
		ttlMil = ttl.Milliseconds()
	}

	return &pb.NodeAddResponse{
		Data:         domain.ConvertNode(newNode),
		HeartbeatTtl: ttlMil,
	}, nil
}

func (as *scheduleStore) NodeList(r *pb.NodeListRequest) (*pb.NodeListResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	nodes := getAllNodes(as.lg, tx, &SchedulerFilter{
		Id:     r.Id,
		Name:   r.Name,
		Status: r.Status,
	})
	tx.Unlock()

	// 分页操作
	pageResponse := auth.GetPage(nodes, auth.PageRequest{
		PageNumber: r.PageNumber,
		PageSize:   r.PageSize,
	})
	data := make([]*pb.Node, reflect.ValueOf(pageResponse.Data).Len())
	//参数转换
	for i := 0; i < reflect.ValueOf(pageResponse.Data).Len(); i++ {
		data[i] = domain.ConvertNode(pageResponse.Data.([]*domain.Node)[i])
	}
	resp := &pb.NodeListResponse{
		Data:          data,
		TotalElements: pageResponse.TotalElements,
		TotalPages:    pageResponse.TotalPages,
	}
	return resp, nil
}

func (as *scheduleStore) NodeUpdate(r *pb.NodeUpdateRequest, ttl time.Duration) (*pb.NodeUpdateResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()

	if r.ID == "" {
		return nil, fmt.Errorf("missing node ID for client status update")
	}
	if !domain.ValidNodeStatus(r.Status) {
		return nil, fmt.Errorf("invalid status for node")
	}

	var updatedNode *domain.Node
	node := getNode(as.lg, tx, r.ID)
	now := time.Now()
	if node == nil {
		// 重启过或者清空过，新建一个node
		updatedNode = &domain.Node{
			ID:                r.ID,
			Name:              r.Name,
			Status:            r.Status,
			StatusDescription: r.StatusDescription,
			SecretID:          r.SecretId,
			StartedAt:         now.UnixMicro(),
			StatusUpdatedAt:   now.UnixMicro(),
		}
		if r.Status == "" {
			updatedNode.Status = constant.NodeStatusInit
		}
		if updatedNode.SchedulingEligibility == "" {
			updatedNode.SchedulingEligibility = constant.NodeSchedulingEligible
		}
	} else {
		updatedNode = node
	}
	if r.Status != "" && r.Status != updatedNode.Status {
		updatedNode.Status = r.Status
	}
	if r.StatusDescription != "" && r.StatusDescription != updatedNode.StatusDescription {
		updatedNode.StatusDescription = r.StatusDescription
	}
	updatedNode.StatusUpdatedAt = now.UnixMicro()
	putNode(as.lg, tx, updatedNode)
	as.commitRevision(tx)
	tx.Unlock()

	as.lg.Debug(
		"changed info of a node",
		zap.String("node-id", r.ID),
		zap.String("status", r.Status),
	)
	var ttlMil int64
	if !updatedNode.TerminalStatus() {
		ttlMil = ttl.Milliseconds()
	}
	return &pb.NodeUpdateResponse{
		HeartbeatTtl: ttlMil,
		Data:         domain.ConvertNode(updatedNode),
	}, nil
}

func (as *scheduleStore) NodeDelete(r *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	defer tx.Unlock()
	eval := getNode(as.lg, tx, r.Id)
	if eval == nil {
		return nil, domain.NewErr1101None("job", r.Id)
	}
	delNode(tx, r.Id)
	as.commitRevision(tx)
	as.lg.Debug(
		"deleted a node",
		zap.String("node-id", r.Id),
	)
	return &pb.NodeDeleteResponse{}, nil
}

func (as *scheduleStore) NodeDetail(r *pb.NodeDetailRequest) (*pb.NodeDetailResponse, error) {
	tx := as.be.BatchTx()
	tx.LockInsideApply()
	node := getNode(as.lg, tx, r.Id)
	tx.Unlock()
	if node == nil {
		return nil, domain.NewErr1101None("node", r.Id)
	}
	var resp pb.NodeDetailResponse
	resp.Data = domain.ConvertNode(node)
	return &resp, nil
}

func getNode(lg *zap.Logger, tx backend.ReadTx, id string) *domain.Node {
	_, vs := tx.UnsafeRange(buckets.Node, []byte(id), nil, 0)
	if len(vs) == 0 {
		return nil
	}

	node := &domain.Node{}
	err := node.Unmarshal(vs[0])
	if err != nil {
		lg.Panic(
			"failed to unmarshal 'domain.Node'",
			zap.String("node-id", id),
			zap.Error(err),
		)
	}
	return node
}

func getAllNodes(lg *zap.Logger, tx backend.ReadTx, filter *SchedulerFilter) []*domain.Node {
	var vs [][]byte
	err := tx.UnsafeForEach(buckets.Node, func(k []byte, v []byte) error {
		vs = append(vs, v)
		return nil
	})
	if err != nil {
		lg.Panic("failed to get nodes",
			zap.Error(err))
	}
	if len(vs) == 0 {
		return nil
	}

	var nodes []*domain.Node
	for i := range vs {
		node := &domain.Node{}
		err := node.Unmarshal(vs[i])
		if err != nil {
			lg.Panic("failed to unmarshal 'pb.Node'", zap.Error(err))
		}
		if filter.Id != "" && filter.Id != node.ID {
			continue
		}
		if filter.Status != "" && filter.Status != node.Status {
			continue
		}
		if filter.Name != "" && !strings.Contains(node.Name, filter.Name) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func putNode(lg *zap.Logger, tx backend.BatchTx, node *domain.Node) {
	b, err := node.Marshal()
	if err != nil {
		lg.Panic("failed to unmarshal 'pb.Node'", zap.Error(err))
	}
	tx.UnsafePut(buckets.Node, []byte(node.ID), b)
}

func delNode(tx backend.BatchTx, nodeId string) {
	tx.UnsafeDelete(buckets.Node, []byte(nodeId))
}
