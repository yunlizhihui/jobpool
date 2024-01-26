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

package dispatcher

import (
	"errors"
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
	"yunli.com/jobpool/agent/v2/config"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/pkg/v2/helper"
	"yunli.com/jobpool/pkg/v2/ulid"
)

const (
	initialHeartbeatStagger = 10 * time.Second
)

func (d *Dispatcher) registerAndHeartbeat() {
	d.retryRegisterNode()

	go d.watchNodeUpdates()

	var heartbeat <-chan time.Time
	heartbeat = time.After(helper.RandomStagger(initialHeartbeatStagger))
	for {
		select {
		case <-d.rpcRetryWatcher():
		case <-heartbeat:
		case <-d.shutdownCh:
			return
		}
		if err := d.updateNodeStatus(); err != nil {
			// The servers have changed such that this node has not been
			// registered before
			if strings.Contains(err.Error(), "node not found") {
				// Re-register the node
				d.logger.Info("re-registering node")
				d.retryRegisterNode()
				heartbeat = time.After(helper.RandomStagger(initialHeartbeatStagger))
			} else {
				intv := d.getHeartbeatRetryIntv(err)
				d.logger.Error("error heartbeating. retrying", zap.Error(err))
				heartbeat = time.After(intv)
			}
		} else {
			d.heartbeatLock.Lock()
			heartbeat = time.After(d.heartbeatTTL)
			d.heartbeatLock.Unlock()
		}
	}
}

func (d *Dispatcher) retryRegisterNode() {
	for {
		err := d.registerNode()
		if err == nil {
			// Registered!
			d.markRegisteredWithLock()
			return
		} else {
			d.logger.Warn(fmt.Sprintf("register node failed, %s", err.Error()))
		}

		retryIntv := config.RegisterRetryIntv
		noServersErr := errors.New("no servers")
		if err == noServersErr {
			d.logger.Debug("registration waiting on servers")
			retryIntv = config.NoServerRetryIntv
		} else {
			d.logger.Error("error registering", zap.Error(err))
		}
		select {
		case <-d.rpcRetryWatcher():
		case <-time.After(d.retryIntv(retryIntv)):
		case <-d.shutdownCh:
			return
		}
	}
}

func (d *Dispatcher) registerNode() error {
	node := d.Node()
	now := time.Now()

	req := etcdserverpb.NodeAddRequest{
		ID:                node.ID,
		Name:              node.Name,
		DataCenter:        node.Datacenter,
		SecretId:          node.SecretID,
		Status:            node.Status,
		StatusDescription: node.StatusDescription,
		StatusUpdateAt:    now.UnixMicro(),
		StartedAt:         timestamppb.TimestampNow(),
	}
	var resp *etcdserverpb.NodeAddResponse
	resp, err := d.client.NodeAdd(d.ctx, &req)
	if err != nil {
		d.logger.Warn("Node.Register error", zap.Error(err))
		return err
	}

	// Update the node status to ready after we register.
	d.configLock.Lock()
	node.Status = constant.NodeStatusReady
	d.config.Node.Status = constant.NodeStatusReady
	d.configLock.Unlock()

	d.logger.Info("node registration complete")

	d.heartbeatLock.Lock()
	defer d.heartbeatLock.Unlock()
	d.heartbeatStop.setLastOk(time.Now())
	d.heartbeatTTL, _ = time.ParseDuration(fmt.Sprintf("%dms", resp.HeartbeatTtl))
	return nil
}

func (d *Dispatcher) rpcRetryWatcher() <-chan struct{} {
	d.rpcRetryLock.Lock()
	defer d.rpcRetryLock.Unlock()

	if d.rpcRetryCh == nil {
		d.rpcRetryCh = make(chan struct{})
	}
	return d.rpcRetryCh
}

// retryIntv calculates a retry interval value given the base
func (d *Dispatcher) retryIntv(base time.Duration) time.Duration {
	return base + helper.RandomStagger(base)
}

// Node returns the locally registered node
func (d *Dispatcher) Node() *domain.Node {
	d.configLock.RLock()
	defer d.configLock.RUnlock()
	return d.configCopy.Node
}

// NodeID returns the node ID for the given client
func (d *Dispatcher) NodeID() string {
	return d.config.Node.ID
}

func (d *Dispatcher) SecretID() string {
	return d.config.Node.SecretID
}

// 初始化node
func (d *Dispatcher) setupNode() error {
	node := d.config.Node
	if node == nil {
		node = &domain.Node{}
		d.config.Node = node
	}
	id, secretID, err := d.nodeID()
	if err != nil {
		return fmt.Errorf("node ID setup failed: %v", err)
	}
	node.ID = id
	node.SecretID = secretID
	if node.Attributes == nil {
		node.Attributes = make(map[string]string)
	}

	if node.Meta == nil {
		node.Meta = make(map[string]string)
	}
	if node.Datacenter == "" {
		node.Datacenter = "default"
	}
	if node.Name == "" {
		node.Name, _ = os.Hostname()
	}
	if node.Name == "" {
		node.Name = node.ID
	}
	node.Status = constant.NodeStatusInit
	return nil
}

// nodeID restores, or generates if necessary, a unique node ID and SecretID.
// The node ID is, if available, a persistent unique ID.  The secret ID is a
// high-entropy random ULID.
func (d *Dispatcher) nodeID() (id, secret string, err error) {
	var hostID string
	if hostID == "" {
		// Generate a random hostID if no constant ID is available on
		// this platform.
		hostID = ulid.Generate()
	}
	// Attempt to read existing ID
	idPath := filepath.Join(d.config.StateDir, "client-id")
	d.logger.Info("the state dir path is :", zap.String("path", idPath))
	idBuf, err := ioutil.ReadFile(idPath)
	if err != nil && !os.IsNotExist(err) {
		return "", "", err
	}

	secretPath := filepath.Join(d.config.StateDir, "secret-id")
	d.logger.Info("the secret dir path is :", zap.String("secret", secretPath))
	secretBuf, err := ioutil.ReadFile(secretPath)
	if err != nil && !os.IsNotExist(err) {
		return "", "", err
	}

	// Use existing ID if any
	if len(idBuf) != 0 {
		id = strings.ToLower(string(idBuf))
	} else {
		id = hostID
		// Persist the ID
		if err := ioutil.WriteFile(idPath, []byte(id), 0700); err != nil {
			return "", "", err
		}
	}
	if len(secretBuf) != 0 {
		secret = string(secretBuf)
	} else {
		// Generate new ID
		secret = ulid.Generate()
		// Persist the ID
		if err := ioutil.WriteFile(secretPath, []byte(secret), 0700); err != nil {
			return "", "", err
		}
	}
	return id, secret, nil
}

func (d *Dispatcher) markRegisteredWithLock() {
	d.registerLock.Lock()
	defer d.registerLock.Unlock()
	if d.hasRegistered {
		return
	}
	d.hasRegistered = true
	go func() {
		d.registeredCh <- struct{}{}
	}()
}
