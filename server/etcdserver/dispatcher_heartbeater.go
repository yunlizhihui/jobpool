package etcdserver

import (
	"go.uber.org/zap"
	"sync"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/api/v2/etcdserverpb"
	"yunli.com/jobpool/pkg/v2/helper"
)

type dispatcherHeartbeater struct {
	*EtcdServer
	enabled bool
	logger  *zap.Logger

	// heartbeatTimers track the expiration time of each heartbeat that has
	// a TTL. On expiration, the node status is updated to be 'down'.
	heartbeatTimers     map[string]*time.Timer
	heartbeatTimersLock sync.Mutex
	// lock for enabled
	l sync.RWMutex
}

func newDispatcherHeartbeater(s *EtcdServer) *dispatcherHeartbeater {
	return &dispatcherHeartbeater{
		EtcdServer: s,
		enabled:    false,
		logger:     s.logger.Named("heartBeater"),
	}
}

func (h *dispatcherHeartbeater) SetEnabled(enabled bool) {
	h.l.Lock()
	h.enabled = enabled
	h.l.Unlock()
}

// leader负责初始化dispatcher的心跳波动
func (h *dispatcherHeartbeater) initializeHeartbeatTimers() error {
	resp, err := h.EtcdServer.NodeList(h.ctx, &etcdserverpb.NodeListRequest{})
	if err != nil {
		return err
	}
	nodes := resp.Data
	for _, node := range nodes {
		if node == nil {
			continue
		}
		n := domain.ConvertNodeFromPb(node)
		if n.TerminalStatus() {
			continue
		}
		h.resetHeartbeatTimerLocked(n.ID, h.EtcdServer.Cfg.FailoverHeartbeatTTL)
	}
	return nil
}

func (h *dispatcherHeartbeater) resetHeartbeatTimerLocked(id string, ttl time.Duration) {
	// Ensure a timer map exists
	if h.heartbeatTimers == nil {
		h.heartbeatTimers = make(map[string]*time.Timer)
	}
	h.logger.Debug("start reset", zap.String("node", id), zap.Duration("duration", ttl))
	// Renew the heartbeat timer if it exists
	if timer, ok := h.heartbeatTimers[id]; ok {
		if !timer.Stop() {
			<-timer.C
		}
		isReset := timer.Reset(ttl)

		h.logger.Debug("reset finished",
			zap.Bool("reset", isReset),
			zap.Duration("duration", ttl))
		return
	} else {
		h.logger.Debug("reset failed", zap.String("node", id), zap.Duration("duration", ttl))
	}

	// Create a new timer to track expiration of this heartbeat
	timer := time.AfterFunc(ttl, func() {
		h.logger.Debug("timer after ttl", zap.Duration("ttl", ttl))
		h.invalidateHeartbeat(id)
	})
	h.logger.Debug("give time a new timer", zap.String("id", id))
	h.heartbeatTimers[id] = timer
}

func (h *dispatcherHeartbeater) invalidateHeartbeat(id string) {
	// Clear the heartbeat timer
	h.heartbeatTimersLock.Lock()
	if timer, ok := h.heartbeatTimers[id]; ok {
		timer.Stop()
		delete(h.heartbeatTimers, id)
	}
	h.heartbeatTimersLock.Unlock()

	// Do not invalidate the node since we are not the leader. This check avoids
	// the race in which leadership is lost but a timer is created on this
	// server since it was servicing an RPC during a leadership loss.
	if !h.EtcdServer.isLeader() {
		h.logger.Debug("ignoring node TTL since this server is not the leader",
			zap.String("node_id", id))
		return
	}

	h.logger.Warn("node TTL expired", zap.String("node_id", id))

	canDisconnect, hasPendingReconnects := h.disconnectState(id)

	// Make a request to update the node status
	req := &etcdserverpb.NodeUpdateRequest{
		ID:                id,
		Status:            constant.NodeStatusDown,
		StatusDescription: "no heartbeat received",
	}
	if canDisconnect && hasPendingReconnects {
		req.Status = constant.NodeStatusDisconnected
	}
	_, err := h.EtcdServer.NodeUpdate(h.ctx, req)
	if err != nil {
		h.logger.Error("update node status failed", zap.Error(err))
	}
}

func (h *dispatcherHeartbeater) disconnectState(id string) (bool, bool) {
	resp, err := h.EtcdServer.NodeDetail(h.ctx, &etcdserverpb.NodeDetailRequest{
		Id: id,
	})
	if err != nil {
		h.logger.Error("error retrieving node by id", zap.String("error", err.Error()))
		return false, false
	}
	node := resp.Data
	// Exit if the node is already down or just initializing.
	if node.Status == constant.NodeStatusDown || node.Status == constant.NodeStatusInit {
		return false, false
	}
	// TODO 该节点上所有的allocation超过10分钟还没跑，则返回true, true
	return true, false
}

func (h *dispatcherHeartbeater) resetHeartbeatTimer(id string) (time.Duration, error) {
	h.heartbeatTimersLock.Lock()
	defer h.heartbeatTimersLock.Unlock()

	// Do not create a timer for the node since we are not the leader. This
	// check avoids the race in which leadership is lost but a timer is created
	// on this server since it was servicing an RPC during a leadership loss.
	if !h.EtcdServer.isLeader() || !h.enabled {
		h.logger.Debug("ignoring resetting node TTL since this server is not the leader", zap.String("node_id", id))
		// return 0, fmt.Errorf("failed to reset heartbeat since server is not leader")
		return 0 * time.Second, nil
	}

	// Compute the target TTL value
	n := len(h.heartbeatTimers)
	ttl := helper.RateScaledInterval(h.EtcdServer.Cfg.MaxHeartbeatsPerSecond, h.EtcdServer.Cfg.MinHeartbeatTTL, n)
	ttl += helper.RandomStagger(ttl)

	// Reset the TTL
	h.resetHeartbeatTimerLocked(id, ttl+h.EtcdServer.Cfg.HeartbeatGrace)
	return ttl, nil
}

func (h *dispatcherHeartbeater) clearAllHeartbeatTimers() error {
	h.heartbeatTimersLock.Lock()
	defer h.heartbeatTimersLock.Unlock()

	for _, t := range h.heartbeatTimers {
		t.Stop()
	}
	h.heartbeatTimers = nil
	return nil
}

func (h *dispatcherHeartbeater) clearHeartbeatTimer(nodeId string) error {
	if !h.enabled {
		return nil
	}

	h.heartbeatTimersLock.Lock()
	defer h.heartbeatTimersLock.Unlock()

	if timer, ok := h.heartbeatTimers[nodeId]; ok {
		timer.Stop()
		delete(h.heartbeatTimers, nodeId)
	}
	return nil
}
