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

package delayheap

import (
	"container/heap"
	"fmt"
	"time"
	"yunli.com/jobpool/api/v2/domain"
)

// DelayHeap 提供方法：Push/Pop.
// 内部的heap类型按照时间排序
type DelayHeap struct {
	index map[domain.NamespacedID]*delayHeapNode
	heap  delayedHeapInfo
}

// HeapNode 接口，内容存储在 DelayHeap 中
type HeapNode interface {
	Data() interface{}
	ID() string
	Namespace() string
}

type delayHeapNode struct {
	Node      HeapNode
	WaitUntil time.Time
	index     int
}

// NewDelayHeap 初始化一个延迟队列
func NewDelayHeap() *DelayHeap {
	return &DelayHeap{
		index: make(map[domain.NamespacedID]*delayHeapNode),
		heap:  make(delayedHeapInfo, 0),
	}
}

// Push 加入数据
func (p *DelayHeap) Push(dataNode HeapNode, next time.Time) error {
	tuple := domain.NamespacedID{
		ID:        dataNode.ID(),
		Namespace: dataNode.Namespace(),
	}
	if _, ok := p.index[tuple]; ok {
		return fmt.Errorf("node %q (%s) already exists", dataNode.ID(), dataNode.Namespace())
	}

	delayHeapNode := &delayHeapNode{dataNode, next, 0}
	p.index[tuple] = delayHeapNode
	heap.Push(&p.heap, delayHeapNode)
	return nil
}

func (p *DelayHeap) Pop() *delayHeapNode {
	if len(p.heap) == 0 {
		return nil
	}

	delayHeapNode := heap.Pop(&p.heap).(*delayHeapNode)
	tuple := domain.NamespacedID{
		ID:        delayHeapNode.Node.ID(),
		Namespace: delayHeapNode.Node.Namespace(),
	}
	delete(p.index, tuple)
	return delayHeapNode
}

// Peek 顺序获取（时间原则）
func (p *DelayHeap) Peek() *delayHeapNode {
	if len(p.heap) == 0 {
		return nil
	}

	return p.heap[0]
}

func (p *DelayHeap) Contains(heapNode HeapNode) bool {
	tuple := domain.NamespacedID{
		ID:        heapNode.ID(),
		Namespace: heapNode.Namespace(),
	}
	_, ok := p.index[tuple]
	return ok
}

func (p *DelayHeap) Update(heapNode HeapNode, waitUntil time.Time) error {
	tuple := domain.NamespacedID{
		ID:        heapNode.ID(),
		Namespace: heapNode.Namespace(),
	}
	if existingHeapNode, ok := p.index[tuple]; ok {
		// Need to update the job as well because its spec can change.
		existingHeapNode.Node = heapNode
		existingHeapNode.WaitUntil = waitUntil
		heap.Fix(&p.heap, existingHeapNode.index)
		return nil
	}

	return fmt.Errorf("heap doesn't contain object with ID %q (%s)", heapNode.ID(), heapNode.Namespace())
}

func (p *DelayHeap) Remove(heapNode HeapNode) error {
	tuple := domain.NamespacedID{
		ID:        heapNode.ID(),
		Namespace: heapNode.Namespace(),
	}
	if node, ok := p.index[tuple]; ok {
		heap.Remove(&p.heap, node.index)
		delete(p.index, tuple)
		return nil
	}

	return fmt.Errorf("heap doesn't contain object with ID %q (%s)", heapNode.ID(), heapNode.Namespace())
}

func (p *DelayHeap) Length() int {
	return len(p.heap)
}

// delayedHeapInfo 用于按照时间排序
type delayedHeapInfo []*delayHeapNode

func (h delayedHeapInfo) Len() int {
	return len(h)
}

func (h delayedHeapInfo) Less(i, j int) bool {
	if h[i].WaitUntil.IsZero() {
		// 0,? => ?,0
		return false
	}

	if h[j].WaitUntil.IsZero() {
		// ?,0 => ?,0
		return true
	}

	return h[i].WaitUntil.Before(h[j].WaitUntil)
}

func (h delayedHeapInfo) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *delayedHeapInfo) Push(x interface{}) {
	node := x.(*delayHeapNode)
	n := len(*h)
	node.index = n
	*h = append(*h, node)
}

func (h *delayedHeapInfo) Pop() interface{} {
	old := *h
	n := len(old)
	node := old[n-1]
	node.index = -1 // for safety
	*h = old[0 : n-1]
	return node
}
