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
	"fmt"
	"github.com/mitchellh/cli"
	"go.uber.org/zap"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
	"yunli.com/jobpool/agent/v2/config"
	"yunli.com/jobpool/agent/v2/dispatcher"
	"yunli.com/jobpool/client/pkg/v2/logutil"
	"yunli.com/jobpool/pkg/v2/winsvc"
)

const gracefulTimeout = 5 * time.Second

type Command struct {
	Ui         cli.Ui
	ShutdownCh <-chan struct{}

	args           []string
	dispatcher     *dispatcher.Dispatcher
	logOutput      io.Writer
	retryJoinErrCh chan struct{}
	logger         *zap.Logger
}

func (c *Command) Run(args []string) int {
	c.Ui = &cli.PrefixedUi{
		OutputPrefix: "==> ",
		InfoPrefix:   "    ",
		ErrorPrefix:  "==> ",
		Ui:           c.Ui,
	}
	c.args = args
	c.logger, _ = logutil.CreateDefaultZapLogger(zap.InfoLevel)
	// TODO read config
	config := c.readConfig()

	if err := c.setupDispatcher(config, c.logger); err != nil {
		return 1
	}
	defer func() {
		c.dispatcher.Shutdown()
	}()
	info := make(map[string]string)
	info["name"] = config.Name
	// Sort the keys for output
	infoKeys := make([]string, 0, len(info))
	for key := range info {
		infoKeys = append(infoKeys, key)
	}
	sort.Strings(infoKeys)
	padding := 18
	c.Ui.Output("jobpool agent configuration:\n")
	for _, k := range infoKeys {
		c.Ui.Info(fmt.Sprintf(
			"%s%s: %s",
			strings.Repeat(" ", padding-len(k)),
			strings.Title(k),
			info[k]))
	}
	c.Ui.Output("")
	c.Ui.Output("jobpool agent started! Log data will stream in below:\n")

	// Wait for exit
	return c.handleSignals()
}

func (c *Command) readConfig() *config.DispatcherConfig {
	cfg := newConfig(c.logger)
	if err := cfg.parse(c.args); err != nil {
		return nil
	}
	return cfg.ec.GenerateDispatcherConfig()
}

func (c *Command) handleSignals() int {
	signalCh := make(chan os.Signal, 4)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)

	// Wait for a signal
WAIT:
	var sig os.Signal
	select {
	case s := <-signalCh:
		sig = s
	case <-winsvc.ShutdownChannel():
		sig = os.Interrupt
	case <-c.ShutdownCh:
		sig = os.Interrupt
	case <-c.retryJoinErrCh:
		return 1
	}

	// Skip any SIGPIPE signal and don't try to log it (See issues #1798, #3554)
	if sig == syscall.SIGPIPE {
		goto WAIT
	}

	c.Ui.Output(fmt.Sprintf("Caught signal: %v", sig))

	// Check if this is a SIGHUP
	if sig == syscall.SIGHUP {
		// c.handleReload()
		goto WAIT
	}

	// Check if we should do a graceful leave
	graceful := false
	if sig == os.Interrupt && c.dispatcher.GetConfig().LeaveOnInt {
		graceful = true
	} else if sig == syscall.SIGTERM && c.dispatcher.GetConfig().LeaveOnTerm {
		graceful = true
	}

	// Bail fast if not doing a graceful leave
	if !graceful {
		return 1
	}

	// Attempt a graceful leave
	gracefulCh := make(chan struct{})
	c.Ui.Output("Gracefully shutting down agent...")
	go func() {
		if err := c.dispatcher.Leave(); err != nil {
			c.Ui.Error(fmt.Sprintf("Error: %s", err))
			return
		}
		close(gracefulCh)
	}()

	// Wait for leave or another signal
	select {
	case <-signalCh:
		return 1
	case <-time.After(gracefulTimeout):
		return 1
	case <-gracefulCh:
		return 0
	}
}
func (c *Command) Help() string {
	helpText := `
Usage: jobpool agent [options]
`
	return strings.TrimSpace(helpText)
}

func (c *Command) Synopsis() string {
	return "Runs a agent"
}

func (c *Command) setupDispatcher(config *config.DispatcherConfig, log *zap.Logger) error {
	c.Ui.Output("Starting dispatcher...")
	disp, err := dispatcher.NewDispatcher(config, log)
	if err != nil {
		// log the error as well, so it appears at the end
		c.logger.Error("error starting agent", zap.Error(err))
		c.Ui.Error(fmt.Sprintf("Error starting agent: %s", err))
		return err
	}
	c.dispatcher = disp
	return nil
}
