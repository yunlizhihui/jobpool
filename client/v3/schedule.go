package clientv3

import (
	"context"
	"google.golang.org/grpc"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/api/v2/schedulepb"
)

type Schedule interface {
	NamespaceAdd(ctx context.Context, name string) (*pb.ScheduleNameSpaceAddResponse, error)

	NamespaceList(ctx context.Context, request *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error)

	NamespaceDelete(ctx context.Context, name string) (*pb.ScheduleNameSpaceDeleteResponse, error)

	PlanAdd(ctx context.Context, name, tp, namespace, spec, timeZone string, priority int64, enable bool) (*pb.SchedulePlanAddResponse, error)

	PlanList(ctx context.Context, request *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error)

	PlanDetail(ctx context.Context, request *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error)

	JobList(ctx context.Context, request *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error)

	JobStatusUpdate(ctx context.Context, request *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error)

	JobDetail(ctx context.Context, request *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error)

	EvalList(ctx context.Context, request *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error)

	EvalDetail(ctx context.Context, request *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error)

	EvalUpdate(ctx context.Context, request *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error)

	EvalCreate(ctx context.Context, request *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error)

	AllocationList(ctx context.Context, request *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error)

	AllocationDetail(ctx context.Context, request *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error)

	SimpleAllocationList(ctx context.Context, request *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error)

	AllocationUpdate(ctx context.Context, request *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error)

	EvalDequeue(ctx context.Context, timeout uint64, schedulers []string) (*pb.ScheduleEvalDequeueResponse, error)

	EvalAck(ctx context.Context, evalId string, jobId string, token string, namespace string) (*pb.ScheduleEvalAckResponse, error)

	EvalNack(ctx context.Context, evalId string, jobId string, token string, namespace string) (*pb.ScheduleEvalNackResponse, error)

	PlanAllocationEnqueue(ctx context.Context, request *pb.PlanAllocationEnqueueRequest) (*pb.PlanAllocationEnqueueResponse, error)

	NodeAdd(ctx context.Context, request *pb.NodeAddRequest) (*pb.NodeAddResponse, error)

	NodeUpdate(ctx context.Context, request *pb.NodeUpdateRequest) (*pb.NodeUpdateResponse, error)
}

type scheduleClient struct {
	remote   pb.ScheduleClient
	callOpts []grpc.CallOption
}

func NewSchedule(c *Client) Schedule {
	api := &scheduleClient{remote: RetryScheduleClient(c)}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func NewScheduleFromScheduleClient(remote pb.ScheduleClient, c *Client) Schedule {
	api := &scheduleClient{remote: remote}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func (sch *scheduleClient) NamespaceAdd(ctx context.Context, name string) (*pb.ScheduleNameSpaceAddResponse, error) {
	resp, err := sch.remote.NameSpaceAdd(ctx, &pb.ScheduleNameSpaceAddRequest{
		Name: name,
	}, sch.callOpts...)
	return (*pb.ScheduleNameSpaceAddResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) NamespaceList(ctx context.Context, r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error) {
	resp, err := sch.remote.NameSpaceList(ctx, r, sch.callOpts...)
	return (*pb.ScheduleNameSpaceListResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) NamespaceDelete(ctx context.Context, name string) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	resp, err := sch.remote.NameSpaceDelete(ctx, &pb.ScheduleNameSpaceDeleteRequest{
		Name: name,
	})
	return resp, err
}

func (sch *scheduleClient) PlanAdd(ctx context.Context, name, tp, namespace, spec, timeZone string, priority int64, enable bool) (*pb.SchedulePlanAddResponse, error) {
	resp, err := sch.remote.PlanAdd(ctx, &pb.SchedulePlanAddRequest{
		Name:      name,
		Type:      tp,
		Namespace: namespace,
		Periodic: &schedulepb.PeriodicConfig{
			Enabled: enable,
			//Spec:     spec,
			//TimeZone: timeZone,
		},
	}, sch.callOpts...)
	return (*pb.SchedulePlanAddResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) PlanList(ctx context.Context, r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error) {
	resp, err := sch.remote.PlanList(ctx, r, sch.callOpts...)
	return (*pb.SchedulePlanListResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) PlanDetail(ctx context.Context, request *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := sch.remote.PlanDetail(ctx, request, sch.callOpts...)
	return (*pb.SchedulePlanDetailResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) JobList(ctx context.Context, r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error) {
	resp, err := sch.remote.JobList(ctx, r, sch.callOpts...)
	return (*pb.ScheduleJobListResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) JobDetail(ctx context.Context, r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error) {
	resp, err := sch.remote.JobDetail(ctx, r, sch.callOpts...)
	return (*pb.ScheduleJobDetailResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) JobStatusUpdate(ctx context.Context, r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error) {
	resp, err := sch.remote.JobUpdate(ctx, r, sch.callOpts...)
	return resp, toErr(ctx, err)
}

func (sch *scheduleClient) EvalList(ctx context.Context, r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error) {
	resp, err := sch.remote.EvalList(ctx, r, sch.callOpts...)
	return (*pb.ScheduleEvalListResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) EvalDetail(ctx context.Context, r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error) {
	resp, err := sch.remote.EvalDetail(ctx, r, sch.callOpts...)
	return resp, toErr(ctx, err)
}

func (sch *scheduleClient) EvalDequeue(ctx context.Context, timeout uint64, schedulers []string) (*pb.ScheduleEvalDequeueResponse, error) {
	resp, err := sch.remote.EvalDequeue(ctx, &pb.ScheduleEvalDequeueRequest{
		Timeout:    timeout,
		Schedulers: schedulers,
	}, sch.callOpts...)
	return (*pb.ScheduleEvalDequeueResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) EvalAck(ctx context.Context, evalId string, jobId string, token string, namespace string) (*pb.ScheduleEvalAckResponse, error) {
	resp, err := sch.remote.EvalAck(ctx, &pb.ScheduleEvalAckRequest{
		Id:        evalId,
		JobId:     jobId,
		Token:     token,
		Namespace: namespace,
	}, sch.callOpts...)
	return (*pb.ScheduleEvalAckResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) EvalNack(ctx context.Context, evalId string, jobId string, token string, namespace string) (*pb.ScheduleEvalNackResponse, error) {
	resp, err := sch.remote.EvalNack(ctx, &pb.ScheduleEvalNackRequest{
		Id:        evalId,
		JobId:     jobId,
		Token:     token,
		Namespace: namespace,
	}, sch.callOpts...)
	return (*pb.ScheduleEvalNackResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) EvalUpdate(ctx context.Context, request *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	resp, err := sch.remote.EvalUpdate(ctx, request, sch.callOpts...)
	return (*pb.ScheduleEvalStatusUpdateResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) EvalCreate(ctx context.Context, request *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error) {
	resp, err := sch.remote.EvalAdd(ctx, request, sch.callOpts...)
	return (*pb.ScheduleEvalAddResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) AllocationList(ctx context.Context, request *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error) {
	resp, err := sch.remote.AllocationList(ctx, request, sch.callOpts...)
	return (*pb.ScheduleAllocationListResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) AllocationDetail(ctx context.Context, request *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error) {
	resp, err := sch.remote.AllocationDetail(ctx, request, sch.callOpts...)
	return (resp), toErr(ctx, err)
}

func (sch *scheduleClient) SimpleAllocationList(ctx context.Context, request *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error) {
	resp, err := sch.remote.SimpleAllocationList(ctx, request, sch.callOpts...)
	return (*pb.ScheduleSimpleAllocationListResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) AllocationUpdate(ctx context.Context, request *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	resp, err := sch.remote.AllocationUpdate(ctx, request, sch.callOpts...)
	return (*pb.ScheduleAllocationStatusUpdateResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) PlanAllocationEnqueue(ctx context.Context, request *pb.PlanAllocationEnqueueRequest) (*pb.PlanAllocationEnqueueResponse, error) {
	resp, err := sch.remote.PlanAllocationEnqueue(ctx, request, sch.callOpts...)
	return (*pb.PlanAllocationEnqueueResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) NodeAdd(ctx context.Context, request *pb.NodeAddRequest) (*pb.NodeAddResponse, error) {
	resp, err := sch.remote.NodeAdd(ctx, request, sch.callOpts...)
	return (*pb.NodeAddResponse)(resp), toErr(ctx, err)
}

func (sch *scheduleClient) NodeUpdate(ctx context.Context, request *pb.NodeUpdateRequest) (*pb.NodeUpdateResponse, error) {
	resp, err := sch.remote.NodeUpdate(ctx, request, sch.callOpts...)
	return (*pb.NodeUpdateResponse)(resp), toErr(ctx, err)
}
