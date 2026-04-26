package main

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
)

const (
	virtualNodeCount = 256
	replicaCount     = 3
)

// ConsistentHashRing implements consistent hashing for distributed data partitioning
type ConsistentHashRing struct {
	mu           sync.RWMutex
	virtualNodes []uint64
	nodeMap      map[uint64]string
	nodes        map[string]bool
}

func NewConsistentHashRing() *ConsistentHashRing {
	return &ConsistentHashRing{
		nodeMap: make(map[uint64]string),
		nodes:   make(map[string]bool),
	}
}

func (c *ConsistentHashRing) AddNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nodes[nodeID] = true
	for i := 0; i < virtualNodeCount; i++ {
		virtualKey := fmt.Sprintf("%s-vn-%d", nodeID, i)
		hash := hashKey(virtualKey)
		c.virtualNodes = append(c.virtualNodes, hash)
		c.nodeMap[hash] = nodeID
	}
	sort.Slice(c.virtualNodes, func(i, j int) bool {
		return c.virtualNodes[i] < c.virtualNodes[j]
	})
}

func (c *ConsistentHashRing) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.nodes, nodeID)
	newVirtualNodes := make([]uint64, 0, len(c.virtualNodes))
	for _, hash := range c.virtualNodes {
		if c.nodeMap[hash] != nodeID {
			newVirtualNodes = append(newVirtualNodes, hash)
		} else {
			delete(c.nodeMap, hash)
		}
	}
	c.virtualNodes = newVirtualNodes
}

func (c *ConsistentHashRing) GetNode(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.virtualNodes) == 0 {
		return ""
	}

	hash := hashKey(key)
	idx := sort.Search(len(c.virtualNodes), func(i int) bool {
		return c.virtualNodes[i] >= hash
	})

	if idx == len(c.virtualNodes) {
		idx = 0
	}

	return c.nodeMap[c.virtualNodes[idx]]
}

func (c *ConsistentHashRing) getAllNodeIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeIDs := make([]string, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs
}

func hashKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return uint64(h[0])<<56 | uint64(h[1])<<48 | uint64(h[2])<<40 | uint64(h[3])<<32 |
		uint64(h[4])<<24 | uint64(h[5])<<16 | uint64(h[6])<<8 | uint64(h[7])
}
