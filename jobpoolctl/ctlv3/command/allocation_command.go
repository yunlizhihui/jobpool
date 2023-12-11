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

var (
	allocationStatus string
)

// NewUserCommand returns the cobra command for "user".
func NewAllocationCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "allocation <subcommand>",
		Short: "AllocationAllocation related commands",
	}
	ac.AddCommand(newAllocationListCommand())
	ac.AddCommand(newAllocationDetailCommand())
	return ac
}

func newAllocationListCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "list status page-number page-size [options]",
		Short: "Lists page allocations",
		Run:   allocationListCommandFunc,
	}
	cmd.Flags().StringVar(&allocationStatus, "status", "", "Show status of allocations")
	cmd.Flags().StringVar(&planId, "plan-id", "", "Show the allocations of the plan")
	cmd.Flags().StringVar(&jobId, "job-id", "", "Show the allocations of the job")
	cmd.Flags().StringVar(&evalId, "eval-id", "", "Show the allocations of the eval")
	cmd.Flags().IntVar(&pageNumber, "number", 1, "Show page number of allocations")
	cmd.Flags().IntVar(&pageSize, "size", 10, "Show page size of allocations")
	return &cmd
}

// userListCommandFunc executes the "user list" command.
func allocationListCommandFunc(cmd *cobra.Command, args []string) {
	resp, err := mustClientFromCmd(cmd).Schedule.AllocationList(context.TODO(), &pb.ScheduleAllocationListRequest{
		Status:     allocationStatus,
		PageNumber: int32(pageNumber),
		PageSize:   int32(pageSize),
		PlanId:     planId,
		JobId:      jobId,
		EvalId:     evalId,
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.AllocationList(*resp)
}

func newAllocationDetailCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "detail <allocationId>",
		Short: "",
		Run:   allocationDetailCommandFunc,
	}
	return &cmd
}

func allocationDetailCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("allocation detail command requires allocationId argument"))
	}
	resp, err := mustClientFromCmd(cmd).Schedule.AllocationDetail(context.TODO(), &pb.ScheduleAllocationDetailRequest{
		Id: args[0],
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.AllocationDetail(*resp)
}
