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
	"context"
	"fmt"
	"github.com/spf13/cobra"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/pkg/v2/cobrautl"
)

// NewUserCommand returns the cobra command for "user".
func NewPlanCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "plan <subcommand>",
		Short: "PlanAllocation related commands",
	}
	ac.AddCommand(newPlanListCommand())
	ac.AddCommand(newPlanDetailCommand())
	return ac
}

func newPlanListCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "list status page-number page-size [options]",
		Short: "Lists page plans",
		Run:   planListCommandFunc,
	}
	cmd.Flags().StringVar(&planStatus, "status", "", "Show status of plans")
	cmd.Flags().IntVar(&pageNumber, "number", 1, "Show page number of plans")
	cmd.Flags().IntVar(&pageSize, "size", 10, "Show page size of plans")
	cmd.Flags().StringVar(&namespace, "ns", "", "Show namespace of plans")
	return &cmd
}

func newPlanDetailCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "detail <PlanId> <namespace>",
		Short: "",
		Run:   planDetailCommandFunc,
	}
}

// userListCommandFunc executes the "user list" command.
func planListCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("plan list command requires no arguments"))
	}

	resp, err := mustClientFromCmd(cmd).Schedule.PlanList(context.TODO(), &pb.SchedulePlanListRequest{
		Status:     planStatus,
		PageNumber: int32(pageNumber),
		PageSize:   int32(pageSize),
		Namespace:  namespace,
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.PlanList(*resp)
}

func planDetailCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("plan detail command requires planId and namespace arguments"))
	}
	resp, err := mustClientFromCmd(cmd).Schedule.PlanDetail(context.TODO(), &pb.SchedulePlanDetailRequest{
		Id:        args[0],
		Namespace: args[1],
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.PlanDetail(*resp)
}
