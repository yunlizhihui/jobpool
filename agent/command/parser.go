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
	"flag"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"runtime"
	"sigs.k8s.io/yaml"
	"yunli.com/jobpool/agent/v2/config"
	"yunli.com/jobpool/api/v2/version"
	"yunli.com/jobpool/pkg/v2/flags"
	flagsUtil "yunli.com/jobpool/pkg/v2/flags"
)

type cfg struct {
	ec           config.AgentConfig
	cf           configFlags
	configFile   string
	printVersion bool
	logger       *zap.Logger
}

type configFlags struct {
	flagSet *flag.FlagSet
}

func newConfig(logger *zap.Logger) *cfg {
	c := &cfg{
		ec:           *config.DefaultAgentConfig(),
		printVersion: false,
		logger:       logger,
	}
	c.cf = configFlags{
		flagSet: flag.NewFlagSet("agent", flag.ContinueOnError),
	}
	fs := c.cf.flagSet
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "the usage is jobpool agent")
	}
	fs.StringVar(&c.ec.Name, "name", c.ec.Name, "Human-readable name for this member.")
	fs.StringVar(&c.ec.StateDir, "data-dir", c.ec.StateDir, "")
	fs.StringVar(&c.ec.AdvertiseServerUrls, "advertise-server-urls", c.ec.AdvertiseServerUrls, "the url for link")
	fs.BoolVar(&c.printVersion, "version", false, "Print the version and exit.")
	fs.StringVar(&c.configFile, "config-file", c.configFile, "")
	return c
}

func (c *cfg) parse(arguments []string) error {
	perr := c.cf.flagSet.Parse(arguments)
	switch perr {
	case nil:
	case flag.ErrHelp:
		fmt.Println("the error of jobpool agent")
		os.Exit(0)
	default:
		os.Exit(2)
	}
	if len(c.cf.flagSet.Args()) != 0 {
		return fmt.Errorf("'%s' is not a valid flag", c.cf.flagSet.Arg(0))
	}

	if c.printVersion {
		fmt.Printf("jobpool Version: %s\n", version.Version)
		fmt.Printf("Git SHA: %s\n", version.GitSHA)
		fmt.Printf("Go Version: %s\n", runtime.Version())
		fmt.Printf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}
	var err error
	if c.configFile == "" {
		c.configFile = os.Getenv(flags.FlagToEnv("JOBPOOL", "config-file"))
	}

	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		c.logger.Info(
			"loaded server configuration, other configuration command line flags and environment variables will be ignored if provided",
			zap.String("path", c.configFile),
		)
	} else {
		err = c.configFromCmdLine()
	}
	return err
}

func (c *cfg) configFromFile(path string) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(b, &c.ec)
	if err != nil {
		return err
	}
	return nil
}

func (c *cfg) configFromCmdLine() error {
	err := flagsUtil.SetFlagsFromEnv(c.logger, "JOBPOOL", c.cf.flagSet)
	if err != nil {
		return err
	}
	return nil
}
