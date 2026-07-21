package adapter

import (
	"context"
	"google.golang.org/grpc"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
)

type scs2scc struct{ as pb.ScheduleServer }

func ScheduleServerToScheduleClient(as pb.ScheduleServer) pb.ScheduleClient {
	return &scs2scc{as}
}

func (s *scs2scc) PlanAdd(ctx context.Context, in *pb.SchedulePlanAddRequest, opts ...grpc.CallOption) (*pb.SchedulePlanAddResponse, error) {
	return s.as.PlanAdd(ctx, in)
}

func (s *scs2scc) PlanList(ctx context.Context, in *pb.SchedulePlanListRequest, opts ...grpc.CallOption) (*pb.SchedulePlanListResponse, error) {
	return s.as.PlanList(ctx, in)
}

func (s *scs2scc) PlanUpdate(ctx context.Context, in *pb.SchedulePlanUpdateRequest, opts ...grpc.CallOption) (*pb.SchedulePlanUpdateResponse, error) {
	return s.as.PlanUpdate(ctx, in)
}

func (s *scs2scc) PlanDelete(ctx context.Context, in *pb.SchedulePlanDeleteRequest, opts ...grpc.CallOption) (*pb.SchedulePlanDeleteResponse, error) {
	return s.as.PlanDelete(ctx, in)
}

func (s *scs2scc) PlanDetail(ctx context.Context, in *pb.SchedulePlanDetailRequest, opts ...grpc.CallOption) (*pb.SchedulePlanDetailResponse, error) {
	return s.as.PlanDetail(ctx, in)
}

func (s *scs2scc) PlanOnline(ctx context.Context, in *pb.SchedulePlanOnlineRequest, opts ...grpc.CallOption) (*pb.SchedulePlanDetailResponse, error) {
	return s.as.PlanOnline(ctx, in)
}

func (s *scs2scc) PlanOffline(ctx context.Context, in *pb.SchedulePlanOfflineRequest, opts ...grpc.CallOption) (*pb.SchedulePlanDetailResponse, error) {
	return s.as.PlanOffline(ctx, in)
}

func (s *scs2scc) NameSpaceAdd(ctx context.Context, in *pb.ScheduleNameSpaceAddRequest, opts ...grpc.CallOption) (*pb.ScheduleNameSpaceAddResponse, error) {
	return s.as.NameSpaceAdd(ctx, in)
}
func (s *scs2scc) NameSpaceList(ctx context.Context, in *pb.ScheduleNameSpaceListRequest, opts ...grpc.CallOption) (*pb.ScheduleNameSpaceListResponse, error) {
	return s.as.NameSpaceList(ctx, in)
}

func (s *scs2scc) NameSpaceDelete(ctx context.Context, in *pb.ScheduleNameSpaceDeleteRequest, opts ...grpc.CallOption) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	return s.as.NameSpaceDelete(ctx, in)
}

func (s *scs2scc) NameSpaceDetail(ctx context.Context, in *pb.ScheduleNameSpaceDetailRequest, opts ...grpc.CallOption) (*pb.ScheduleNameSpaceDetailResponse, error) {
	return s.as.NameSpaceDetail(ctx, in)
}

func (s *scs2scc) NameSpaceUpdate(ctx context.Context, in *pb.ScheduleNameSpaceUpdateRequest, opts ...grpc.CallOption) (*pb.ScheduleNameSpaceUpdateResponse, error) {
	return s.as.NameSpaceUpdate(ctx, in)
}

func (s *scs2scc) JobAdd(ctx context.Context, in *pb.ScheduleJobAddRequest, opts ...grpc.CallOption) (*pb.ScheduleJobAddResponse, error) {
	return s.as.JobAdd(ctx, in)
}

func (s *scs2scc) JobList(ctx context.Context, in *pb.ScheduleJobListRequest, opts ...grpc.CallOption) (*pb.ScheduleJobListResponse, error) {
	return s.as.JobList(ctx, in)
}

func (s *scs2scc) JobExist(ctx context.Context, in *pb.ScheduleJobExistRequest, opts ...grpc.CallOption) (*pb.ScheduleJobExistResponse, error) {
	return s.as.JobExist(ctx, in)
}

func (s *scs2scc) JobUpdate(ctx context.Context, in *pb.ScheduleJobStatusUpdateRequest, opts ...grpc.CallOption) (*pb.ScheduleJobStatusUpdateResponse, error) {
	return s.as.JobUpdate(ctx, in)
}

func (s *scs2scc) JobDelete(ctx context.Context, in *pb.ScheduleJobDeleteRequest, opts ...grpc.CallOption) (*pb.ScheduleJobDeleteResponse, error) {
	return s.as.JobDelete(ctx, in)
}

func (s *scs2scc) JobDetail(ctx context.Context, in *pb.ScheduleJobDetailRequest, opts ...grpc.CallOption) (*pb.ScheduleJobDetailResponse, error) {
	return s.as.JobDetail(ctx, in)
}

func (s *scs2scc) EvalAdd(ctx context.Context, in *pb.ScheduleEvalAddRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalAddResponse, error) {
	return s.as.EvalAdd(ctx, in)
}

func (s *scs2scc) EvalList(ctx context.Context, in *pb.ScheduleEvalListRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalListResponse, error) {
	return s.as.EvalList(ctx, in)
}

func (s *scs2scc) EvalUpdate(ctx context.Context, in *pb.ScheduleEvalStatusUpdateRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	return s.as.EvalUpdate(ctx, in)
}

func (s *scs2scc) EvalDelete(ctx context.Context, in *pb.ScheduleEvalDeleteRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalDeleteResponse, error) {
	return s.as.EvalDelete(ctx, in)
}

func (s *scs2scc) EvalDetail(ctx context.Context, in *pb.ScheduleEvalDetailRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalDetailResponse, error) {
	return s.as.EvalDetail(ctx, in)
}

func (s *scs2scc) AllocationAdd(ctx context.Context, in *pb.ScheduleAllocationAddRequest, opts ...grpc.CallOption) (*pb.ScheduleAllocationAddResponse, error) {
	return s.as.AllocationAdd(ctx, in)
}

func (s *scs2scc) AllocationList(ctx context.Context, in *pb.ScheduleAllocationListRequest, opts ...grpc.CallOption) (*pb.ScheduleAllocationListResponse, error) {
	return s.as.AllocationList(ctx, in)
}

func (s *scs2scc) AllocationUpdate(ctx context.Context, in *pb.ScheduleAllocationStatusUpdateRequest, opts ...grpc.CallOption) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	return s.as.AllocationUpdate(ctx, in)
}

func (s *scs2scc) AllocationDelete(ctx context.Context, in *pb.ScheduleAllocationDeleteRequest, opts ...grpc.CallOption) (*pb.ScheduleAllocationDeleteResponse, error) {
	return s.as.AllocationDelete(ctx, in)
}

func (s *scs2scc) AllocationDetail(ctx context.Context, in *pb.ScheduleAllocationDetailRequest, opts ...grpc.CallOption) (*pb.ScheduleAllocationDetailResponse, error) {
	return s.as.AllocationDetail(ctx, in)
}

func (s *scs2scc) SimpleAllocationList(ctx context.Context, in *pb.ScheduleAllocationListRequest, opts ...grpc.CallOption) (*pb.ScheduleSimpleAllocationListResponse, error) {
	return s.as.SimpleAllocationList(ctx, in)
}

func (s *scs2scc) EvalDequeue(ctx context.Context, in *pb.ScheduleEvalDequeueRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalDequeueResponse, error) {
	return s.as.EvalDequeue(ctx, in)
}

func (s *scs2scc) EvalAck(ctx context.Context, in *pb.ScheduleEvalAckRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalAckResponse, error) {
	return s.as.EvalAck(ctx, in)
}

func (s *scs2scc) EvalNack(ctx context.Context, in *pb.ScheduleEvalNackRequest, opts ...grpc.CallOption) (*pb.ScheduleEvalNackResponse, error) {
	return s.as.EvalNack(ctx, in)
}

func (s *scs2scc) NodeAdd(ctx context.Context, in *pb.NodeAddRequest, opts ...grpc.CallOption) (*pb.NodeAddResponse, error) {
	return s.as.NodeAdd(ctx, in)
}

func (s *scs2scc) NodeList(ctx context.Context, in *pb.NodeListRequest, opts ...grpc.CallOption) (*pb.NodeListResponse, error) {
	return s.as.NodeList(ctx, in)
}

func (s *scs2scc) NodeUpdate(ctx context.Context, in *pb.NodeUpdateRequest, opts ...grpc.CallOption) (*pb.NodeUpdateResponse, error) {
	return s.as.NodeUpdate(ctx, in)
}

func (s *scs2scc) NodeDelete(ctx context.Context, in *pb.NodeDeleteRequest, opts ...grpc.CallOption) (*pb.NodeDeleteResponse, error) {
	return s.as.NodeDelete(ctx, in)
}

func (s *scs2scc) NodeDetail(ctx context.Context, in *pb.NodeDetailRequest, opts ...grpc.CallOption) (*pb.NodeDetailResponse, error) {
	return s.as.NodeDetail(ctx, in)
}

func (s *scs2scc) PlanAllocationEnqueue(ctx context.Context, in *pb.PlanAllocationEnqueueRequest, opts ...grpc.CallOption) (*pb.PlanAllocationEnqueueResponse, error) {
	return s.as.PlanAllocationEnqueue(ctx, in)
}

func (s *scs2scc) QueueDetail(ctx context.Context, in *pb.QueueDetailRequest, opts ...grpc.CallOption) (*pb.QueueDetailResponse, error) {
	return s.as.QueueDetail(ctx, in)
}

func (s *scs2scc) QueueJobViewDetail(ctx context.Context, in *pb.QueueJobViewRequest, opts ...grpc.CallOption) (*pb.QueueJobViewResponse, error) {
	return s.as.QueueJobViewDetail(ctx, in)
}

func (s *scs2scc) QueueForbiddenUpdate(ctx context.Context, in *pb.QueueForbiddenRequest, opts ...grpc.CallOption) (*pb.QueueForbiddenResponse, error) {
	return s.as.QueueForbiddenUpdate(ctx, in)
}

func (s *scs2scc) QueueStatusDetail(ctx context.Context, in *pb.QueueStatusRequest, opts ...grpc.CallOption) (*pb.QueueStatusResponse, error) {
	return s.as.QueueStatusDetail(ctx, in)
}
