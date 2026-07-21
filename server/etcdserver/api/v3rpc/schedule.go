package v3rpc

import (
	"context"
	"yunli.com/jobpool/api/v2/domain"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/server/v2/etcdserver"
)

type ScheduleServer struct {
	scheduleService etcdserver.ScheduleService
}

func NewScheduleServer(s *etcdserver.EtcdServer) *ScheduleServer {
	return &ScheduleServer{scheduleService: s}
}

func (as *ScheduleServer) PlanAdd(ctx context.Context, r *pb.SchedulePlanAddRequest) (*pb.SchedulePlanAddResponse, error) {
	resp, err := as.scheduleService.PlanAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanList(ctx context.Context, r *pb.SchedulePlanListRequest) (*pb.SchedulePlanListResponse, error) {
	resp, err := as.scheduleService.PlanList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanUpdate(ctx context.Context, r *pb.SchedulePlanUpdateRequest) (*pb.SchedulePlanUpdateResponse, error) {
	resp, err := as.scheduleService.PlanUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanDelete(ctx context.Context, r *pb.SchedulePlanDeleteRequest) (*pb.SchedulePlanDeleteResponse, error) {
	if r.Namespace == "" {
		return nil, domain.NewErr1001Blank("参数namespace")
	}
	if r.Id == "" {
		return nil, domain.NewErr1001Blank("id")
	}
	resp, err := as.scheduleService.PlanDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanDetail(ctx context.Context, r *pb.SchedulePlanDetailRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := as.scheduleService.PlanDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanOnline(ctx context.Context, r *pb.SchedulePlanOnlineRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := as.scheduleService.PlanOnline(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanOffline(ctx context.Context, r *pb.SchedulePlanOfflineRequest) (*pb.SchedulePlanDetailResponse, error) {
	resp, err := as.scheduleService.PlanOffline(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NameSpaceDelete(ctx context.Context, r *pb.ScheduleNameSpaceDeleteRequest) (*pb.ScheduleNameSpaceDeleteResponse, error) {
	if r.Name == "" {
		return nil, domain.NewErr1001Blank("name")
	}
	resp, err := as.scheduleService.NameSpaceDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NameSpaceDetail(ctx context.Context, r *pb.ScheduleNameSpaceDetailRequest) (*pb.ScheduleNameSpaceDetailResponse, error) {
	resp, err := as.scheduleService.NameSpaceDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NameSpaceUpdate(ctx context.Context, r *pb.ScheduleNameSpaceUpdateRequest) (*pb.ScheduleNameSpaceUpdateResponse, error) {
	resp, err := as.scheduleService.NameSpaceUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NameSpaceAdd(ctx context.Context, r *pb.ScheduleNameSpaceAddRequest) (*pb.ScheduleNameSpaceAddResponse, error) {
	resp, err := as.scheduleService.NameSpaceAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NameSpaceList(ctx context.Context, r *pb.ScheduleNameSpaceListRequest) (*pb.ScheduleNameSpaceListResponse, error) {
	resp, err := as.scheduleService.NameSpaceList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) JobAdd(ctx context.Context, r *pb.ScheduleJobAddRequest) (*pb.ScheduleJobAddResponse, error) {
	resp, err := as.scheduleService.JobAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) JobList(ctx context.Context, r *pb.ScheduleJobListRequest) (*pb.ScheduleJobListResponse, error) {
	resp, err := as.scheduleService.JobList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) JobUpdate(ctx context.Context, r *pb.ScheduleJobStatusUpdateRequest) (*pb.ScheduleJobStatusUpdateResponse, error) {
	resp, err := as.scheduleService.JobUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) JobDelete(ctx context.Context, r *pb.ScheduleJobDeleteRequest) (*pb.ScheduleJobDeleteResponse, error) {
	resp, err := as.scheduleService.JobDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) JobDetail(ctx context.Context, r *pb.ScheduleJobDetailRequest) (*pb.ScheduleJobDetailResponse, error) {
	resp, err := as.scheduleService.JobDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) JobExist(ctx context.Context, r *pb.ScheduleJobExistRequest) (*pb.ScheduleJobExistResponse, error) {
	resp, err := as.scheduleService.JobExist(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalAdd(ctx context.Context, r *pb.ScheduleEvalAddRequest) (*pb.ScheduleEvalAddResponse, error) {
	resp, err := as.scheduleService.EvalAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalList(ctx context.Context, r *pb.ScheduleEvalListRequest) (*pb.ScheduleEvalListResponse, error) {
	resp, err := as.scheduleService.EvalList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalUpdate(ctx context.Context, r *pb.ScheduleEvalStatusUpdateRequest) (*pb.ScheduleEvalStatusUpdateResponse, error) {
	resp, err := as.scheduleService.EvalUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalDelete(ctx context.Context, r *pb.ScheduleEvalDeleteRequest) (*pb.ScheduleEvalDeleteResponse, error) {
	resp, err := as.scheduleService.EvalDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalDetail(ctx context.Context, r *pb.ScheduleEvalDetailRequest) (*pb.ScheduleEvalDetailResponse, error) {
	resp, err := as.scheduleService.EvalDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) AllocationAdd(ctx context.Context, r *pb.ScheduleAllocationAddRequest) (*pb.ScheduleAllocationAddResponse, error) {
	resp, err := as.scheduleService.AllocationAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) AllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleAllocationListResponse, error) {
	resp, err := as.scheduleService.AllocationList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) AllocationUpdate(ctx context.Context, r *pb.ScheduleAllocationStatusUpdateRequest) (*pb.ScheduleAllocationStatusUpdateResponse, error) {
	resp, err := as.scheduleService.AllocationUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) AllocationDelete(ctx context.Context, r *pb.ScheduleAllocationDeleteRequest) (*pb.ScheduleAllocationDeleteResponse, error) {
	resp, err := as.scheduleService.AllocationDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) AllocationDetail(ctx context.Context, r *pb.ScheduleAllocationDetailRequest) (*pb.ScheduleAllocationDetailResponse, error) {
	resp, err := as.scheduleService.AllocationDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) SimpleAllocationList(ctx context.Context, r *pb.ScheduleAllocationListRequest) (*pb.ScheduleSimpleAllocationListResponse, error) {
	resp, err := as.scheduleService.SimpleAllocationList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalDequeue(ctx context.Context, r *pb.ScheduleEvalDequeueRequest) (*pb.ScheduleEvalDequeueResponse, error) {
	resp, err := as.scheduleService.EvalDequeue(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalAck(ctx context.Context, r *pb.ScheduleEvalAckRequest) (*pb.ScheduleEvalAckResponse, error) {
	resp, err := as.scheduleService.EvalAck(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) EvalNack(ctx context.Context, r *pb.ScheduleEvalNackRequest) (*pb.ScheduleEvalNackResponse, error) {
	resp, err := as.scheduleService.EvalNack(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NodeAdd(ctx context.Context, r *pb.NodeAddRequest) (*pb.NodeAddResponse, error) {
	resp, err := as.scheduleService.NodeAdd(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NodeList(ctx context.Context, r *pb.NodeListRequest) (*pb.NodeListResponse, error) {
	resp, err := as.scheduleService.NodeList(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NodeUpdate(ctx context.Context, r *pb.NodeUpdateRequest) (*pb.NodeUpdateResponse, error) {
	resp, err := as.scheduleService.NodeUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NodeDelete(ctx context.Context, r *pb.NodeDeleteRequest) (*pb.NodeDeleteResponse, error) {
	resp, err := as.scheduleService.NodeDelete(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) NodeDetail(ctx context.Context, r *pb.NodeDetailRequest) (*pb.NodeDetailResponse, error) {
	resp, err := as.scheduleService.NodeDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) PlanAllocationEnqueue(ctx context.Context, r *pb.PlanAllocationEnqueueRequest) (*pb.PlanAllocationEnqueueResponse, error) {
	resp, err := as.scheduleService.PlanAllocationEnqueue(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) QueueDetail(ctx context.Context, r *pb.QueueDetailRequest) (*pb.QueueDetailResponse, error) {
	resp, err := as.scheduleService.QueueDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) QueueJobViewDetail(ctx context.Context, r *pb.QueueJobViewRequest) (*pb.QueueJobViewResponse, error) {
	resp, err := as.scheduleService.QueueJobViewDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) QueueForbiddenUpdate(ctx context.Context, r *pb.QueueForbiddenRequest) (*pb.QueueForbiddenResponse, error) {
	resp, err := as.scheduleService.QueueForbiddenUpdate(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}

func (as *ScheduleServer) QueueStatusDetail(ctx context.Context, r *pb.QueueStatusRequest) (*pb.QueueStatusResponse, error) {
	resp, err := as.scheduleService.QueueStatusDetail(ctx, r)
	if err != nil {
		return nil, togRPCError(err)
	}
	return resp, nil
}
