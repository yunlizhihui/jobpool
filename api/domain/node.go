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

package domain

import (
	"encoding/json"
	"fmt"
	timestamppb "github.com/gogo/protobuf/types"
	"net"
	"time"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/etcdserverpb"
)

// Node 用于调度的客户端节点
type Node struct {
	ID                string // uuid
	SecretID          string // 安全性
	Datacenter        string
	Name              string
	HTTPAddr          string            // is the address on which the Jobpool client is listening for http requests
	Attributes        map[string]string // 附加一些版本信息
	Meta              map[string]string // 元数据信息
	NodeClass         string            // for group
	ComputedClass     string            // 唯一ID标识公共属性，如rest
	Status            string
	StatusDescription string
	StartedAt         int64
	StatusUpdatedAt   int64
	LastDrain         *DrainMetadata // contains metadata about the most recent drain operation
	// DrainStrategy determines the node's draining behavior.
	// Will be non-nil only while draining.
	DrainStrategy *DrainStrategy

	// SchedulingEligibility determines whether this node will receive new
	// placements.
	SchedulingEligibility string
}

type DrainStatus string

const (
	DrainStatusDraining DrainStatus = "draining"
	DrainStatusComplete DrainStatus = "complete"
	DrainStatusCanceled DrainStatus = "canceled"
)

// Sanitize returns a copy of the Node omitting confidential fields
// It only returns a copy if the Node contains the confidential fields
func (n *Node) Sanitize() *Node {
	if n == nil {
		return nil
	}
	if n.SecretID == "" {
		return n
	}
	clean := n.Copy()
	clean.SecretID = ""
	return clean
}

func (n *Node) Copy() *Node {
	if n == nil {
		return nil
	}
	nn := new(Node)
	*nn = *n
	nn.Attributes = CopyMapStringString(nn.Attributes)
	nn.Meta = CopyMapStringString(nn.Meta)
	nn.DrainStrategy = nn.DrainStrategy.Copy()
	nn.LastDrain = nn.LastDrain.Copy()
	return nn
}

// DrainMetadata contains information about the most recent drain operation for a given Node.
type DrainMetadata struct {
	// StartedAt is the time that the drain operation started. This is equal to Node.DrainStrategy.StartedAt,
	// if it exists
	StartedAt time.Time

	// UpdatedAt is the time that that this struct was most recently updated, either via API action
	// or drain completion
	UpdatedAt time.Time

	// Status reflects the status of the drain operation.
	Status DrainStatus

	// AccessorID is the accessor ID of the ACL token used in the most recent API operation against this drain
	AccessorID string

	// Meta includes the operator-submitted metadata about this drain operation
	Meta map[string]string
}

func (m *DrainMetadata) Copy() *DrainMetadata {
	if m == nil {
		return nil
	}
	c := new(DrainMetadata)
	*c = *m
	c.Meta = CopyMapStringString(m.Meta)
	return c
}

func (n *Node) Canonicalize() {
	if n == nil {
		return
	}
	if n.DrainStrategy != nil {
		n.SchedulingEligibility = constant.NodeSchedulingIneligible
	} else if n.SchedulingEligibility == "" {
		n.SchedulingEligibility = constant.NodeSchedulingEligible
	}
}

// DrainUpdate is used to update the drain of a node
type DrainUpdate struct {
	// DrainStrategy is the new strategy for the node
	DrainStrategy *DrainStrategy

	// MarkEligible marks the node as eligible if removing the drain strategy.
	MarkEligible bool
}

func (n *Node) TerminalStatus() bool {
	switch n.Status {
	case constant.NodeStatusDown:
		return true
	default:
		return false
	}
}

// ValidNodeStatus is used to check if a node status is valid
func ValidNodeStatus(status string) bool {
	switch status {
	case constant.NodeStatusInit, constant.NodeStatusReady, constant.NodeStatusDown, constant.NodeStatusDisconnected:
		return true
	default:
		return false
	}
}

// ShouldDrainNode 是否可以运行调度逻辑
func ShouldDrainNode(status string) bool {
	switch status {
	case constant.NodeStatusInit, constant.NodeStatusReady, constant.NodeStatusDisconnected:
		return false
	case constant.NodeStatusDown:
		return true
	default:
		panic(fmt.Sprintf("unhandled node status %s", status))
	}
}

// NodeStubFields defines which fields are included in the NodeListStub.
type NodeStubFields struct {
	Resources bool
	OS        bool
}

// Stub returns a summarized version of the node
func (n *Node) Stub(fields *NodeStubFields) *NodeListStub {
	addr, _, _ := net.SplitHostPort(n.HTTPAddr)
	s := &NodeListStub{
		Address:               addr,
		ID:                    n.ID,
		Datacenter:            n.Datacenter,
		Name:                  n.Name,
		NodeClass:             n.NodeClass,
		Version:               n.Attributes["jobpool.version"],
		Drain:                 n.DrainStrategy != nil,
		SchedulingEligibility: n.SchedulingEligibility,
		Status:                n.Status,
		StatusDescription:     n.StatusDescription,
		LastDrain:             n.LastDrain,
	}

	if fields != nil {
		if fields.OS {
			m := make(map[string]string)
			m["os.name"] = n.Attributes["os.name"]
			s.Attributes = m
		}
	}

	return s
}

// NodeListStub is used to return a subset of plan information
// for the plan list
type NodeListStub struct {
	Address               string
	ID                    string
	Attributes            map[string]string `json:",omitempty"`
	Datacenter            string
	Name                  string
	NodeClass             string
	Version               string
	Drain                 bool
	SchedulingEligibility string
	Status                string
	StatusDescription     string
	LastDrain             *DrainMetadata
	CreateIndex           uint64
	ModifyIndex           uint64
}

func (n *Node) Ready() bool {
	return n.Status == constant.NodeStatusReady && n.DrainStrategy == nil && n.SchedulingEligibility == constant.NodeSchedulingEligible
}

// DrainSpec describes a Node's desired drain behavior.
type DrainSpec struct {
	Deadline         time.Duration
	IgnoreSystemJobs bool
}

// DrainStrategy describes a Node's drain behavior.
type DrainStrategy struct {
	// DrainSpec is the user declared drain specification
	DrainSpec

	// ForceDeadline is the deadline time for the drain after which drains will
	// be forced
	ForceDeadline time.Time

	// StartedAt is the time the drain process started
	StartedAt time.Time
}

func (d *DrainStrategy) Copy() *DrainStrategy {
	if d == nil {
		return nil
	}

	nd := new(DrainStrategy)
	*nd = *d
	return nd
}

// DeadlineTime returns a boolean whether the drain strategy allows an infinite
// duration or otherwise the deadline time. The force drain is captured by the
// deadline time being in the past.
func (d *DrainStrategy) DeadlineTime() (infinite bool, deadline time.Time) {
	// Treat the nil case as a force drain so during an upgrade where a node may
	// not have a drain strategy but has Drain set to true, it is treated as a
	// force to mimick old behavior.
	if d == nil {
		return false, time.Time{}
	}

	ns := d.Deadline.Nanoseconds()
	switch {
	case ns < 0: // Force
		return false, time.Time{}
	case ns == 0: // Infinite
		return true, time.Time{}
	default:
		return false, d.ForceDeadline
	}
}

func (d *DrainStrategy) Equal(o *DrainStrategy) bool {
	if d == nil && o == nil {
		return true
	} else if o != nil && d == nil {
		return false
	} else if d != nil && o == nil {
		return false
	}

	// Compare values
	if d.ForceDeadline != o.ForceDeadline {
		return false
	} else if d.Deadline != o.Deadline {
		return false
	} else if d.IgnoreSystemJobs != o.IgnoreSystemJobs {
		return false
	}

	return true
}

func CopyMapStringString(m map[string]string) map[string]string {
	l := len(m)
	if l == 0 {
		return nil
	}

	c := make(map[string]string, l)
	for k, v := range m {
		c[k] = v
	}
	return c
}

func (n *Node) Marshal() ([]byte, error) {
	return json.Marshal(n)
}

func (n *Node) Unmarshal(content []byte) error {
	return json.Unmarshal(content, n)
}

func ConvertNode(n *Node) *etcdserverpb.Node {
	node := &etcdserverpb.Node{
		ID:                n.ID,
		Name:              n.Name,
		Status:            n.Status,
		StatusDescription: n.StatusDescription,
		StatusUpdateAt:    n.StatusUpdatedAt,
		NodeClass:         n.NodeClass,
		SecretId:          n.SecretID,
	}
	if n.StartedAt != 0 {
		node.StartedAt, _ = timestamppb.TimestampProto(time.UnixMicro(n.StartedAt))
	}
	if n.StatusUpdatedAt != 0 {
		node.UpdatedAt, _ = timestamppb.TimestampProto(time.UnixMicro(n.StatusUpdatedAt))
	}
	return node
}

func ConvertNodeFromPb(n *etcdserverpb.Node) *Node {
	node := &Node{
		ID:                n.ID,
		Name:              n.Name,
		Status:            n.Status,
		StatusDescription: n.StatusDescription,
		StatusUpdatedAt:   n.StatusUpdateAt,
		NodeClass:         n.NodeClass,
		SecretID:          n.SecretId,
	}
	if n.StartedAt != nil {
		date := time.Unix(n.StartedAt.GetSeconds(), int64(n.StartedAt.GetNanos()))
		node.StartedAt = date.UnixMicro()
	}
	if n.UpdatedAt != nil {
		date := time.Unix(n.UpdatedAt.GetSeconds(), int64(n.UpdatedAt.GetNanos()))
		node.StatusUpdatedAt = date.UnixMicro()
	}
	return node
}

func ConvertNodeFromRequest(n *etcdserverpb.NodeAddRequest) *Node {
	node := &Node{
		ID:                n.ID,
		Name:              n.Name,
		Status:            n.Status,
		StatusDescription: n.StatusDescription,
		StatusUpdatedAt:   n.StatusUpdateAt,
		SecretID:          n.SecretId,
	}
	if n.StartedAt != nil {
		date := time.Unix(n.StartedAt.GetSeconds(), int64(n.StartedAt.GetNanos()))
		node.StartedAt = date.UnixMicro()
	}
	if n.UpdatedAt != nil {
		date := time.Unix(n.UpdatedAt.GetSeconds(), int64(n.UpdatedAt.GetNanos()))
		node.StatusUpdatedAt = date.UnixMicro()
	}
	return node
}
