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
	evalStatus string
)

// NewUserCommand returns the cobra command for "user".
func NewEvalCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "eval <subcommand>",
		Short: "Eval related commands",
	}
	ac.AddCommand(newEvalListCommand())
	ac.AddCommand(newEvalDetailCommand())
	return ac
}

func newEvalListCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "list status page-number page-size [options]",
		Short: "Lists page evals",
		Run:   evalListCommandFunc,
	}
	cmd.Flags().StringVar(&evalStatus, "status", "", "Show status of evals")
	cmd.Flags().StringVar(&planId, "plan-id", "", "Show the evals of the plan")
	cmd.Flags().StringVar(&jobId, "job-id", "", "Show the evals of the job")
	cmd.Flags().IntVar(&pageNumber, "number", 1, "Show page number of evals")
	cmd.Flags().IntVar(&pageSize, "size", 10, "Show page size of evals")
	return &cmd
}

func newEvalDetailCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "detail <evalId>",
		Short: "",
		Run:   evalDetailCommandFunc,
	}
	return &cmd
}

// userListCommandFunc executes the "user list" command.
func evalListCommandFunc(cmd *cobra.Command, args []string) {
	resp, err := mustClientFromCmd(cmd).Schedule.EvalList(context.TODO(), &pb.ScheduleEvalListRequest{
		Status:     evalStatus,
		PageNumber: int32(pageNumber),
		PageSize:   int32(pageSize),
		PlanId:     planId,
		JobId:      jobId,
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.EvalList(*resp)
}

func evalDetailCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("eval detail command requires evalId argument"))
	}
	resp, err := mustClientFromCmd(cmd).Schedule.EvalDetail(context.TODO(), &pb.ScheduleEvalDetailRequest{
		Id: args[0],
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.EvalDetail(*resp)
}
