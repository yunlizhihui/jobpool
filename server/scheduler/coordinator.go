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

package scheduler

import (
	"go.uber.org/zap"
	"math/rand"
	"yunli.com/jobpool/api/v2/constant"
	"yunli.com/jobpool/api/v2/domain"
	"yunli.com/jobpool/pkg/v2/helper"
)

type slotRunningType func(namespace string, hasAlloc int) bool

// allocCoordinator 任务协调器
type allocCoordinator struct {
	logger        *zap.Logger
	planAlloc     *domain.PlanAlloc
	slotRunningFn slotRunningType
}

// NewNodeCoordinator 新建一个任务协调器
func NewNodeCoordinator(logger *zap.Logger,
	slotRunningFn slotRunningType,
	planAlloc *domain.PlanAlloc) *allocCoordinator {
	return &allocCoordinator{
		logger:        logger,
		planAlloc:     planAlloc,
		slotRunningFn: slotRunningFn,
	}
}

// Compute 计算各个节点的分数，将任务分配给分数最低的节点
// TODO 接口化（多种实现方案）
func (r *allocCoordinator) Compute(repository ScheduleRepository) (*domain.Node, error) {
	iter, err := repository.Nodes()
	if err != nil {
		return nil, err
	}
	scoresMap := make(map[string]*int, 64)
	scoresVal := make(map[string]int, 64)
	var nodes []*domain.Node

	for _, node := range iter {
		// node信息中有down状态和其他状态的不适合用于运行任务
		if node == nil || node.Status != constant.NodeStatusReady {
			continue
		}
		nodes = append(nodes, node)
		scoresMap[node.ID] = helper.IntToPtr(0)
		scoresVal[node.ID] = 0
	}
	namespace := r.planAlloc.Plan.Namespace
	// 获取全部运行中alloc
	allocs, err := repository.RunningAllocsByNamespace(namespace)
	if err != nil {
		return nil, err
	}
	if !r.slotRunningFn(namespace, len(allocs)) {
		r.logger.Warn("no slot for running job, retry later",
			zap.String("namespace", namespace),
			zap.Int("used", len(allocs)))
		return nil, domain.NewErr1109NotExist("允许范围内的并发任务节点")
	}
	// 根据alloc计算最轻松的节点
	for _, alloc := range allocs {
		if alloc.NodeID == "" {
			continue
		}
		if scoresMap[alloc.NodeID] != nil {
			scoresVal[alloc.NodeID] = scoresVal[alloc.NodeID] + 1
		}
	}
	// 返回分数最低的节点
	minScore := -1
	var selectNodeId string
	for k, v := range scoresVal {
		if minScore == -1 {
			minScore = v
			selectNodeId = k
			continue
		}
		if minScore > v {
			minScore = v
			selectNodeId = k
		}
	}
	for _, node := range nodes {
		if node.ID == selectNodeId {
			return node, nil
		}
	}
	// 无则随机
	if len(nodes) > 1 {
		// random
		num := rand.Intn(len(nodes) - 1)
		if num < len(nodes) && nodes[num] != nil {
			return nodes[num], nil
		}
	}
	return nil, domain.NewErr1004Invalid("节点", "满足调度规则要求")
}
