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
	"github.com/mattn/go-colorable"
	"github.com/mitchellh/cli"
	"os"
)

func Commands(metaInfo *Meta, agentUi cli.Ui) map[string]cli.CommandFactory {
	if metaInfo == nil {
		metaInfo = new(Meta)
	}
	meta := *metaInfo
	if meta.Ui == nil {
		meta.Ui = &cli.BasicUi{
			Reader:      os.Stdin,
			Writer:      colorable.NewColorableStdout(),
			ErrorWriter: colorable.NewColorableStderr(),
		}
	}
	all := map[string]cli.CommandFactory{
		"agent": func() (cli.Command, error) {
			return &Command{
				Ui:         agentUi,
				ShutdownCh: make(chan struct{}),
			}, nil
		},
	}
	return all
}
