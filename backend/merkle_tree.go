package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

// MerkleTree represents a hash tree for efficient data synchronization
type MerkleTree struct {
	Leaves  []string          // SHA-256 hashes of the leaf nodes
	Levels  [][]string        // All levels of the tree, with root at the last level
	KeyMap  map[string]string // Maps leaf hashes back to keys for lookup
	Version int               // Tree version for change tracking
}

func NewMerkleTree(data map[string]interface{}) *MerkleTree {
	mt := &MerkleTree{
		KeyMap: make(map[string]string),
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := data[k]
		leafHash := hashLeaf(k, v)
		mt.Leaves = append(mt.Leaves, leafHash)
		mt.KeyMap[leafHash] = k
	}

	mt.buildTree()
	return mt
}

func hashLeaf(key string, value interface{}) string {
	data := fmt.Sprintf("%s:%v", key, value)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func (mt *MerkleTree) buildTree() {
	mt.Levels = [][]string{mt.Leaves}
	current := mt.Leaves

	for len(current) > 1 {
		next := make([]string, 0, (len(current)+1)/2)
		for i := 0; i < len(current); i += 2 {
			a := current[i]
			b := a
			if i+1 < len(current) {
				b = current[i+1]
			}
			combined := a + b
			h := sha256.Sum256([]byte(combined))
			next = append(next, hex.EncodeToString(h[:]))
		}
		mt.Levels = append(mt.Levels, next)
		current = next
	}

	mt.Version++
}

func (mt *MerkleTree) Root() string {
	n := len(mt.Levels)
	if n == 0 || len(mt.Levels[n-1]) == 0 {
		return ""
	}
	return mt.Levels[n-1][0]
}

func (mt *MerkleTree) CompareTrees(other *MerkleTree) []string {
	if mt.Root() == other.Root() {
		return nil
	}
	return mt.findDifferingKeys(other)
}

func (mt *MerkleTree) findDifferingKeys(other *MerkleTree) []string {
	diffs := map[string]struct{}{}
	otherLeaves := make(map[string]bool, len(other.Leaves))
	for _, leaf := range other.Leaves {
		otherLeaves[leaf] = true
	}

	for _, leaf := range mt.Leaves {
		if !otherLeaves[leaf] {
			if key, ok := mt.KeyMap[leaf]; ok {
				diffs[key] = struct{}{}
			}
		}
	}

	for _, leaf := range other.Leaves {
		if _, ok := mt.KeyMap[leaf]; !ok {
			if key, ok := other.KeyMap[leaf]; ok {
				diffs[key] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(diffs))
	for k := range diffs {
		result = append(result, k)
	}
	return result
}

func (mt *MerkleTree) SerializeToMap() map[string]interface{} {
	km := make(map[string]string, len(mt.KeyMap))
	for k, v := range mt.KeyMap {
		km[k] = v
	}

	return map[string]interface{}{
		"leaves":  mt.Leaves,
		"levels":  mt.Levels,
		"key_map": km,
		"version": mt.Version,
		"root":    mt.Root(),
	}
}

func DeserializeFromMap(data map[string]interface{}) (*MerkleTree, error) {
	mt := &MerkleTree{KeyMap: make(map[string]string)}

	if _, ok := data["leaves"]; !ok {
		return nil, fmt.Errorf("missing field: leaves")
	}
	if _, ok := data["levels"]; !ok {
		return nil, fmt.Errorf("missing field: levels")
	}
	if _, ok := data["key_map"]; !ok {
		return nil, fmt.Errorf("missing field: key_map")
	}
	if _, ok := data["version"]; !ok {
		return nil, fmt.Errorf("missing field: version")
	}

	if leaves, ok := data["leaves"].([]interface{}); ok {
		for _, l := range leaves {
			if s, ok := l.(string); ok {
				mt.Leaves = append(mt.Leaves, s)
			}
		}
	}

	if levels, ok := data["levels"].([]interface{}); ok {
		for _, lvl := range levels {
			if arr, ok := lvl.([]interface{}); ok {
				strArr := make([]string, 0, len(arr))
				for _, h := range arr {
					if hs, ok := h.(string); ok {
						strArr = append(strArr, hs)
					}
				}
				mt.Levels = append(mt.Levels, strArr)
			}
		}
	}

	if km, ok := data["key_map"].(map[string]interface{}); ok {
		for k, v := range km {
			if s, ok := v.(string); ok {
				mt.KeyMap[k] = s
			}
		}
	}

	if ver, ok := data["version"].(float64); ok {
		mt.Version = int(ver)
	}

	return mt, nil
}

func (mt *MerkleTree) GetDifficultyLevel(other *MerkleTree) int {
	if mt.Root() == other.Root() {
		return 0
	}

	level := 0
	ml := len(mt.Levels)
	ol := len(other.Levels)
	minL := ml
	if ol < minL {
		minL = ol
	}

	for i := 0; i < minL; i++ {
		if mt.Levels[i][0] != other.Levels[i][0] {
			level++
		}
	}
	return level + abs(ml-ol)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
