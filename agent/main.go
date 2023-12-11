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

package main

import (
	"bytes"
	"fmt"
	"github.com/mitchellh/cli"
	"os"
	"strings"
	"text/tabwriter"
	"yunli.com/jobpool/agent/v2/command"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/v2/version"
)

var (
	internalCommonds = []string{
		"syslog",
	}

	commonCommands = []string{
		"agent",
	}
)

func main() {
	os.Exit(Run(os.Args[1:]))
}

func Run(args []string) int {
	if len(args) == 0 {
		args = append(args, "agent")
		args = append(args, "--dev")
		fmt.Println("no args load default arg: agent --dev")
	}

	metaPtr := new(command.Meta)
	agentUi := &cli.BasicUi{
		Reader:      os.Stdin,
		Writer:      os.Stdout,
		ErrorWriter: os.Stderr,
	}

	commands := command.Commands(metaPtr, agentUi)
	cli := &cli.CLI{
		Name:                       constant.ProductName,
		Version:                    version.Version,
		Args:                       args,
		Commands:                   commands,
		HiddenCommands:             internalCommonds,
		Autocomplete:               true,
		AutocompleteNoDefaultFlags: true,
		HelpFunc: groupedHelpFunc(
			cli.BasicHelpFunc(constant.ProductName),
		),
		HelpWriter: os.Stdout,
	}

	exitCode, err := cli.Run()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing CLI: %s\n", err.Error())
		return 1
	}
	return exitCode
}

func groupedHelpFunc(f cli.HelpFunc) cli.HelpFunc {
	return func(commands map[string]cli.CommandFactory) string {
		var b bytes.Buffer
		tw := tabwriter.NewWriter(&b, 0, 2, 6, ' ', 0)
		fmt.Fprintf(tw, "Usage: jobpool [-version] [-help] [-autocomplete-(un)install] <command> [args]\n\n")
		fmt.Fprintf(tw, "Common commands:\n")
		for _, v := range commonCommands {
			cmd, err := commands[v]()
			if err != nil {
				panic(fmt.Sprintf("failed to load %q command: %s", v, err))
			}
			fmt.Fprintf(tw, "    %s\t%s\n", v, cmd.Synopsis())
		}
		fmt.Fprintf(tw, "\n")
		tw.Flush()
		return strings.TrimSpace(b.String())
	}
}
