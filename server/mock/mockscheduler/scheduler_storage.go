package mockscheduler

import (
	"fmt"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	xtime "yunli.com/jobpool/api/v2/helper/time"
	"yunli.com/jobpool/api/v2/schedulepb"
	"yunli.com/jobpool/pkg/v2/ulid"
)

func EvaluationMock() *domain.Evaluation {
	return &domain.Evaluation{
		ID:          ulid.Generate(),
		Namespace:   "default",
		Priority:    200,
		Type:        constant.PlanTypeService,
		TriggeredBy: constant.EvalTriggerScheduled,
		JobID:       ulid.Generate(),
		PlanID:      ulid.Generate(),
		Status:      constant.EvalStatusPending,
		CreateTime:  xtime.NewFormatTime(time.Now()),
		UpdateTime:  xtime.NewFormatTime(time.Now()),
	}
}

func PlanAddMock() *etcdserverpb.SchedulePlanAddRequest {
	planId := ulid.Generate()
	return &etcdserverpb.SchedulePlanAddRequest{
		Id:          planId,
		Name:        fmt.Sprintf("plan-name%s", planId),
		Type:        constant.PlanTypeService,
		Namespace:   "default",
		Priority:    100,
		Stop:        false,
		Synchronous: false,
		Status:      constant.PlanStatusRunning,
		Description: "test",
		Parameters:  "",
		Periodic: &schedulepb.PeriodicConfig{
			Enabled:         true,
			Spec:            "*/1 * * * *",
			ProhibitOverlap: false,
		},
	}
}
