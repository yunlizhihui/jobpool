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
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"time"
	"yunli.com/jobpool/agent/v2/config"
	"yunli.com/jobpool/agent/v2/helper/util"
	"yunli.com/jobpool/api/v2/constant"
	pb "yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/pkg/v2/helper"
)

// watchNodeUpdates 监控节点中的任务状态变化
func (d *Dispatcher) watchNodeUpdates() {
	var hasChanged bool

	timer := util.StoppedTimer()
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			d.logger.Info("state changed, updating node and re-registering")
			d.retryRegisterNode()
			hasChanged = false
		case <-d.triggerNodeUpdate:
			if hasChanged {
				continue
			}
			hasChanged = true
			timer.Reset(d.retryIntv(5 * time.Second))
		case <-d.shutdownCh:
			return
		}
	}
}

func (d *Dispatcher) updateNodeStatus() error {
	start := time.Now()
	request := &pb.NodeUpdateRequest{
		ID:                d.NodeID(),
		SecretId:          d.SecretID(),
		Status:            constant.NodeStatusReady,
		StatusDescription: "node is ready",
	}
	var resp *pb.NodeUpdateResponse
	resp, err := d.client.NodeUpdate(d.ctx, request)
	if err != nil {
		d.logger.Warn("Node.update error", zap.Error(err))
		return err
	}
	end := time.Now()
	d.heartbeatLock.Lock()
	last := d.lastHeartbeat()
	haveHeartbeated := d.haveHeartbeated
	d.heartbeatStop.setLastOk(time.Now())
	d.heartbeatTTL, err = time.ParseDuration(fmt.Sprintf("%dms", resp.HeartbeatTtl))
	if err != nil {
		d.logger.Warn("Node.update response error", zap.Error(err))
		return err
	}
	d.haveHeartbeated = true
	d.heartbeatLock.Unlock()
	d.logger.Debug("next heartbeat", zap.Duration("period", d.heartbeatTTL))

	oldTTL := d.heartbeatTTL
	if haveHeartbeated {
		d.logger.Debug("missed heartbeat",
			zap.Duration("req_latency", end.Sub(start)),
			zap.Duration("heartbeat_ttl", oldTTL),
			zap.Duration("since_last_heartbeat", time.Since(last)))
	}
	return nil
}

func (d *Dispatcher) getHeartbeatRetryIntv(err error) time.Duration {
	// Collect the useful heartbeat info
	d.heartbeatLock.Lock()
	haveHeartbeated := d.haveHeartbeated
	last := d.lastHeartbeat()
	ttl := d.heartbeatTTL
	d.heartbeatLock.Unlock()

	// TODO 没有leader则重试，error确认
	noLeaderErr := errors.New("No cluster leader")
	if !haveHeartbeated || err == noLeaderErr {
		return d.retryIntv(config.RegisterRetryIntv)
	}

	// Determine how much time we have left to heartbeat
	left := time.Until(last.Add(ttl))

	// 重试逻辑，限制在0-30s之间比较合理
	switch {
	case left < -30*time.Second:
		// Make left the absolute value so we delay and jitter properly.
		left *= -1
	case left < 0:
		return time.Second + helper.RandomStagger(time.Second)
	default:
	}

	stagger := helper.RandomStagger(left)
	switch {
	case stagger < time.Second:
		return time.Second + helper.RandomStagger(time.Second)
	case stagger > 30*time.Second:
		return 25*time.Second + helper.RandomStagger(5*time.Second)
	default:
		return stagger
	}
}

func (d *Dispatcher) lastHeartbeat() time.Time {
	return d.heartbeatStop.getLastOk()
}

func (d *Dispatcher) batchFirstFingerprints() {
	_, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	d.configLock.Lock()
	defer d.configLock.Unlock()

	// TODO only update the node if changes occurred
	d.updateNodeLocked()
}

func (d *Dispatcher) updateNodeLocked() {
	// Update the config copy.
	node := d.config.Node.Copy()
	d.configCopy.Node = node

	select {
	case d.triggerNodeUpdate <- struct{}{}:
		// Node update goroutine was released to execute
	default:
		// Node update goroutine was already running
	}
}
