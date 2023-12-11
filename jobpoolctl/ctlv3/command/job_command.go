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
func NewJobCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "job <subcommand>",
		Short: "JobAllocation related commands",
	}
	ac.AddCommand(newJobListCommand())
	ac.AddCommand(newJobDetailCommand())
	ac.AddCommand(newJobStatusUpdateCommand())
	return ac
}

func newJobListCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "list status page-number page-size [options]",
		Short: "Lists page jobs",
		Run:   jobListCommandFunc,
	}
	cmd.Flags().StringVar(&jobStatus, "status", "", "Show status of jobs")
	cmd.Flags().StringVar(&planId, "plan-id", "", "Show the jobs of the plan")
	cmd.Flags().IntVar(&pageNumber, "number", 1, "Show page number of jobs")
	cmd.Flags().IntVar(&pageSize, "size", 10, "Show page size of jobs")
	return &cmd
}

func newJobDetailCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "detail <jobId>",
		Short: "",
		Run:   JobDetailCommandFunc,
	}
	return &cmd
}

func newJobStatusUpdateCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "update <jobId> <status>",
		Short: "",
		Run:   JobStatusUpdateCommandFunc,
	}
	return &cmd
}

// userListCommandFunc executes the "user list" command.
func jobListCommandFunc(cmd *cobra.Command, args []string) {
	resp, err := mustClientFromCmd(cmd).Schedule.JobList(context.TODO(), &pb.ScheduleJobListRequest{
		Status:     jobStatus,
		PlanId:     planId,
		PageNumber: int32(pageNumber),
		PageSize:   int32(pageSize),
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.JobList(*resp)
}

func JobDetailCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("job detail command requires jobId argument"))
	}
	resp, err := mustClientFromCmd(cmd).Schedule.JobDetail(context.TODO(), &pb.ScheduleJobDetailRequest{
		Id: args[0],
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.JobDetail(*resp)
}

func JobStatusUpdateCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("job update status command requires jobId and jobStatus argument"))
	}
	resp, err := mustClientFromCmd(cmd).Schedule.JobStatusUpdate(context.TODO(), &pb.ScheduleJobStatusUpdateRequest{
		Id:          args[0],
		Status:      args[1],
		Description: "change by command line tools",
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.JobStatusUpdate(*resp)
}
