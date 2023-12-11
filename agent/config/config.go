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

package config

import (
	"fmt"
	"os"
	"strings"
	"time"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/client/pkg/v2/types"
)

const (
	RegisterRetryIntv = 15 * time.Second

	NoServerRetryIntv = time.Second

	DateTimeFormat = "2006-01-02 15:04:05"
)

type AgentConfig struct {
	Name                string `json:"name"`
	AdvertiseServerUrls string `json:"advertise-server-urls"`
	StateDir            string `json:"data-dir"`
	// security
	ClientSecurityJSON *securityConfig `json:"client-transport-security"`
	AuthConfig         *authCfg        `json:"auth-config"`
}

type DispatcherConfig struct {
	ID                  string
	Name                string
	AdvertiseServerUrls types.URLs
	// node information
	Node     *domain.Node
	StateDir string `json:"data-dir"`

	LeaveOnInt  bool `json:"leave_on_interrupt"`
	LeaveOnTerm bool `json:"leave_on_terminate"`
	// Security
	ClientSecurityJSON *securityConfig `json:"client-transport-security"`
	AuthConfig         *authCfg        `json:"auth-config"`
}

type securityConfig struct {
	CertFile       string `json:"cert-file"`
	KeyFile        string `json:"key-file"`
	ClientCertFile string `json:"client-cert-file"`
	ClientKeyFile  string `json:"client-key-file"`
	CertAuth       bool   `json:"client-cert-auth"`
	TrustedCAFile  string `json:"trusted-ca-file"`
	AutoTLS        bool   `json:"auto-tls"`
}

type authCfg struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func DefaultAgentConfig() *AgentConfig {
	return &AgentConfig{
		Name:                "agent-1",
		AdvertiseServerUrls: "http://127.0.0.1:2379",
		ClientSecurityJSON: &securityConfig{
			CertAuth: false,
			AutoTLS:  false,
		},
	}
}

func (c *AgentConfig) GenerateDispatcherConfig() *DispatcherConfig {
	config := &DispatcherConfig{
		Name:     c.Name,
		StateDir: c.StateDir,
	}
	if c.AdvertiseServerUrls != "" {
		u, err := types.NewURLs(strings.Split(c.AdvertiseServerUrls, ","))
		if err != nil {
			fmt.Fprintf(os.Stderr, "unexpected error setting up advertise-peer-urls: %v\n", err)
			os.Exit(1)
		}
		config.AdvertiseServerUrls = u
	}
	if c.ClientSecurityJSON != nil {
		config.ClientSecurityJSON = &securityConfig{
			CertFile:       c.ClientSecurityJSON.CertFile,
			KeyFile:        c.ClientSecurityJSON.KeyFile,
			ClientCertFile: c.ClientSecurityJSON.ClientCertFile,
			ClientKeyFile:  c.ClientSecurityJSON.ClientKeyFile,
			CertAuth:       c.ClientSecurityJSON.CertAuth,
			TrustedCAFile:  c.ClientSecurityJSON.TrustedCAFile,
			AutoTLS:        c.ClientSecurityJSON.AutoTLS,
		}
	}
	if c.AuthConfig != nil {
		config.AuthConfig = &authCfg{
			Username: c.AuthConfig.Username,
			Password: c.AuthConfig.Password,
		}
	}
	// Setup the node
	config.Node = new(domain.Node)
	config.Node.Datacenter = "default"
	config.Node.Name = c.Name
	// config.Node.Meta = agentConfig.Client.Meta
	//	conf.Node.NodeClass = agentConfig.Client.NodeClass
	//
	//	// Set up the HTTP advertise address
	config.Node.HTTPAddr = c.AdvertiseServerUrls
	//
	//	// Canonicalize Node struct
	config.Node.Canonicalize()

	return config
}

func (c *DispatcherConfig) Copy() *DispatcherConfig {
	dc := new(DispatcherConfig)
	*dc = *c
	if c.AdvertiseServerUrls.Len() > 0 {
		dc.AdvertiseServerUrls = c.AdvertiseServerUrls
	}
	return dc
}
