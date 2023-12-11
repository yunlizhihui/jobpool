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
func NewNamespaceCommand() *cobra.Command {
	ac := &cobra.Command{
		Use:   "namespace <subcommand>",
		Short: "Namespace related commands",
	}
	ac.AddCommand(newNamespaceAddCommand())
	ac.AddCommand(newNamespaceListCommand())
	ac.AddCommand(newNamespaceDeleteCommand())
	return ac
}

func newNamespaceAddCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "add <namespace name>",
		Short: "Adds a new namespace",
		Run:   namespaceAddCommandFunc,
	}
}

func newNamespaceListCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "list  [options]",
		Short: "Lists namespaces",
		Run:   namespaceListCommandFunc,
	}
	cmd.Flags().IntVar(&pageNumber, "number", 1, "Show page number of namespace")
	cmd.Flags().IntVar(&pageSize, "size", 10, "Show page size of namespace")
	return &cmd
}

func newNamespaceDeleteCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "delete <namespace name>",
		Short: "Delete namespace by name",
		Run:   namespaceDeleteCommandFunc,
	}
	return &cmd
}

func namespaceAddCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("namespace add command requires role name as its argument"))
	}

	resp, err := mustClientFromCmd(cmd).Schedule.NamespaceAdd(context.TODO(), args[0])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}

	display.NamespaceAdd(args[0], *resp)
}

// userListCommandFunc executes the "user list" command.
func namespaceListCommandFunc(cmd *cobra.Command, args []string) {
	resp, err := mustClientFromCmd(cmd).Schedule.NamespaceList(context.TODO(), &pb.ScheduleNameSpaceListRequest{
		PageNumber: int32(pageNumber),
		PageSize:   int32(pageSize),
	})
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.NamespaceList(*resp)
}

func namespaceDeleteCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cobrautl.ExitWithError(cobrautl.ExitBadArgs, fmt.Errorf("namespace delete command requires name as its argument"))
	}
	resp, err := mustClientFromCmd(cmd).Schedule.NamespaceDelete(context.TODO(), args[0])
	if err != nil {
		cobrautl.ExitWithError(cobrautl.ExitError, err)
	}
	display.NamespaceDelete(args[0], *resp)
}
