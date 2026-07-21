package grpcproxy

import (
	"context"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	clientv3 "yunli.com/jobpool/client/v2"
)

type ScheduleProxy struct {
	client *clientv3.Client
}

func NewScheduleProxy(c *clientv3.Client) pb.ScheduleServer {
	return &ScheduleProxy{client: c}
}

func (s *ScheduleProxy) PlanAdd(ctx context.Context, r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanAdd(ctx, r)
}

func (s *ScheduleProxy) PlanList(ctx context.Context, r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanList(ctx, r)
}

func (s *ScheduleProxy) PlanUpdate(ctx context.Context, r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanUpdate(ctx, r)
}

func (s *ScheduleProxy) PlanDelete(ctx context.Context, r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanDelete(ctx, r)
}

func (s *ScheduleProxy) PlanDetail(ctx context.Context, r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanDetail(ctx, r)
}

func (s *ScheduleProxy) PlanOnline(ctx context.Context, r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanOnline(ctx, r)
}

func (s *ScheduleProxy) PlanOffline(ctx context.Context, r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanOffline(ctx, r)
}

func (s *ScheduleProxy) NameSpaceList(ctx context.Context, r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NameSpaceList(ctx, r)
}

func (s *ScheduleProxy) NameSpaceAdd(ctx context.Context, r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NameSpaceAdd(ctx, r)
}

func (s *ScheduleProxy) NameSpaceDelete(ctx context.Context, r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NameSpaceDelete(ctx, r)
}

func (s *ScheduleProxy) NameSpaceDetail(ctx context.Context, r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NameSpaceDetail(ctx, r)
}

func (s *ScheduleProxy) NameSpaceUpdate(ctx context.Context, r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NameSpaceUpdate(ctx, r)
}

func (s *ScheduleProxy) JobAdd(ctx context.Context, r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).JobAdd(ctx, r)
}

func (s *ScheduleProxy) JobList(ctx context.Context, r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).JobList(ctx, r)
}

func (s *ScheduleProxy) JobExist(ctx context.Context, r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).JobExist(ctx, r)
}

func (s *ScheduleProxy) JobUpdate(ctx context.Context, r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).JobUpdate(ctx, r)
}

func (s *ScheduleProxy) JobDelete(ctx context.Context, r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).JobDelete(ctx, r)
}

func (s *ScheduleProxy) JobDetail(ctx context.Context, r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).JobDetail(ctx, r)
}

func (s *ScheduleProxy) EvalAdd(ctx context.Context, r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalAdd(ctx, r)
}

func (s *ScheduleProxy) EvalList(ctx context.Context, r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalList(ctx, r)
}

func (s *ScheduleProxy) EvalUpdate(ctx context.Context, r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalUpdate(ctx, r)
}

func (s *ScheduleProxy) EvalDelete(ctx context.Context, r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalDelete(ctx, r)
}

func (s *ScheduleProxy) EvalDetail(ctx context.Context, r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalDetail(ctx, r)
}

func (s *ScheduleProxy) AllocationAdd(ctx context.Context, r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).AllocationAdd(ctx, r)
}

func (s *ScheduleProxy) AllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).AllocationList(ctx, r)
}

func (s *ScheduleProxy) AllocationUpdate(ctx context.Context, r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).AllocationUpdate(ctx, r)
}

func (s *ScheduleProxy) AllocationDelete(ctx context.Context, r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).AllocationDelete(ctx, r)
}

func (s *ScheduleProxy) AllocationDetail(ctx context.Context, r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).AllocationDetail(ctx, r)
}

func (s *ScheduleProxy) SimpleAllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).SimpleAllocationList(ctx, r)
}

func (s *ScheduleProxy) EvalDequeue(ctx context.Context, r *pb.ScheduleEvalDequeueRequest) (*pb.ScheduleEvalDequeueResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalDequeue(ctx, r)
}

func (s *ScheduleProxy) EvalAck(ctx context.Context, r *pb.ScheduleEvalAckRequest) (*pb.ScheduleEvalAckResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalAck(ctx, r)
}

func (s *ScheduleProxy) EvalNack(ctx context.Context, r *pb.ScheduleEvalNackRequest) (*pb.ScheduleEvalNackResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).EvalNack(ctx, r)
}

func (s *ScheduleProxy) NodeAdd(ctx context.Context, r *pb.NodeAddRequest) (*pb.NodeAddResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NodeAdd(ctx, r)
}

func (s *ScheduleProxy) NodeList(ctx context.Context, r *pb.NodeListRequest) (*pb.NodeListResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NodeList(ctx, r)
}

func (s *ScheduleProxy) NodeUpdate(ctx context.Context, r *pb.NodeUpdateRequest) (*pb.NodeUpdateResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NodeUpdate(ctx, r)
}

func (s *ScheduleProxy) NodeDelete(ctx context.Context, r *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NodeDelete(ctx, r)
}

func (s *ScheduleProxy) NodeDetail(ctx context.Context, r *pb.NodeDetailRequest) (*pb.NodeDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).NodeDetail(ctx, r)
}

func (s *ScheduleProxy) PlanAllocationEnqueue(ctx context.Context, r *pb.PlanAllocationEnqueueRequest) (*pb.PlanAllocationEnqueueResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).PlanAllocationEnqueue(ctx, r)
}

func (s *ScheduleProxy) QueueDetail(ctx context.Context, r *pb.QueueDetailRequest) (*pb.QueueDetailResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).QueueDetail(ctx, r)
}

func (s *ScheduleProxy) QueueJobViewDetail(ctx context.Context, r *pb.QueueJobViewRequest) (*pb.QueueJobViewResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).QueueJobViewDetail(ctx, r)
}

func (s *ScheduleProxy) QueueForbiddenUpdate(ctx context.Context, r *pb.QueueForbiddenRequest) (*pb.QueueForbiddenResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).QueueForbiddenUpdate(ctx, r)
}

func (s *ScheduleProxy) QueueStatusDetail(ctx context.Context, r *pb.QueueStatusRequest) (*pb.QueueStatusResponse, error) {
	conn := s.client.ActiveConnection()
	return pb.NewScheduleClient(conn).QueueStatusDetail(ctx, r)
}
