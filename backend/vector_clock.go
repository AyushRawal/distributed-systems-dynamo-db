package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

type VectorClock struct {
	Clock map[string]int
}

func NewVectorClock() *VectorClock {
	return &VectorClock{Clock: make(map[string]int)}
}

func (vc *VectorClock) Increment(nodeID string) {
	if vc == nil {
		return
	}
	if vc.Clock == nil {
		vc.Clock = make(map[string]int)
	}
	vc.Clock[nodeID] = vc.Clock[nodeID] + 1
}

func (vc *VectorClock) Merge(other *VectorClock) {
	if vc == nil || other == nil {
		return
	}
	if vc.Clock == nil {
		vc.Clock = make(map[string]int)
	}
	for node, count := range other.Clock {
		if current, exists := vc.Clock[node]; !exists || count > current {
			vc.Clock[node] = count
		}
	}
}

func (vc *VectorClock) Clone() *VectorClock {
	if vc == nil {
		return NewVectorClock()
	}
	clone := NewVectorClock()
	if vc.Clock != nil {
		for node, count := range vc.Clock {
			clone.Clock[node] = count
		}
	}
	return clone
}

func (vc *VectorClock) Compare(other *VectorClock) int {
	if vc == nil && other == nil {
		return 0
	}
	if vc == nil {
		return -1
	}
	if other == nil {
		return 1
	}
	vcDominates := false
	otherDominates := false
	for node, count := range vc.Clock {
		if otherCount, exists := other.Clock[node]; exists {
			if count < otherCount {
				otherDominates = true
			} else if count > otherCount {
				vcDominates = true
			}
		} else {
			vcDominates = true
		}
	}
	for node := range other.Clock {
		if _, exists := vc.Clock[node]; !exists {
			otherDominates = true
		}
	}
	if vcDominates && !otherDominates {
		return 1
	} else if !vcDominates && otherDominates {
		return -1
	} else {
		return 0
	}
}

func (vc *VectorClock) String() string {
	if vc == nil || vc.Clock == nil {
		return "{}"
	}
	bytes, err := json.Marshal(vc.Clock)
	if err != nil {
		return fmt.Sprintf("Error marshaling vector clock: %v", err)
	}
	return string(bytes)
}

func (vc *VectorClock) IsEmpty() bool {
	if vc == nil || vc.Clock == nil {
		return true
	}
	return len(vc.Clock) == 0
}

func (vc *VectorClock) Equals(other *VectorClock) bool {
	if vc == nil && other == nil {
		return true
	}
	if vc == nil || other == nil {
		return false
	}
	if len(vc.Clock) != len(other.Clock) {
		return false
	}
	for node, count := range vc.Clock {
		if otherCount, exists := other.Clock[node]; !exists || otherCount != count {
			return false
		}
	}
	return true
}

func (vc *VectorClock) MergeAndIncrement(other *VectorClock, nodeID string) {
	if vc == nil {
		vc = NewVectorClock()
	}
	vc.Merge(other)
	vc.Increment(nodeID)
}

func (vc *VectorClock) Debug() string {
	if vc == nil {
		return "VectorClock<nil>"
	}
	if vc.Clock == nil {
		return "VectorClock{empty}"
	}
	var parts []string
	for node, count := range vc.Clock {
		parts = append(parts, fmt.Sprintf("%s:%d", node, count))
	}
	return fmt.Sprintf("VC{%s}", strings.Join(parts, ", "))
}
