package main

import "testing"

func TestConsistentHashRingGetNodeEmptyRing(t *testing.T) {
	ring := NewConsistentHashRing()

	if got := ring.GetNode("any-key"); got != "" {
		t.Fatalf("expected empty node for empty ring, got %q", got)
	}
}

func TestConsistentHashRingAddNodeCreatesVirtualNodes(t *testing.T) {
	ring := NewConsistentHashRing()
	ring.AddNode("nodeA")

	if got := len(ring.virtualNodes); got != virtualNodeCount {
		t.Fatalf("expected %d virtual nodes, got %d", virtualNodeCount, got)
	}

	if got := len(ring.nodeMap); got != virtualNodeCount {
		t.Fatalf("expected %d node map entries, got %d", virtualNodeCount, got)
	}

	nodes := ring.getAllNodeIDs()
	if len(nodes) != 1 || nodes[0] != "nodeA" {
		t.Fatalf("expected only nodeA in ring, got %v", nodes)
	}
}

func TestConsistentHashRingGetNodeDeterministic(t *testing.T) {
	ring := NewConsistentHashRing()
	ring.AddNode("nodeA")
	ring.AddNode("nodeB")
	ring.AddNode("nodeC")

	key := "user:42"
	first := ring.GetNode(key)
	if first == "" {
		t.Fatal("expected a node for non-empty ring")
	}

	for i := 0; i < 100; i++ {
		if got := ring.GetNode(key); got != first {
			t.Fatalf("expected deterministic mapping for key %q, got %q then %q", key, first, got)
		}
	}

	if !ring.nodes[first] {
		t.Fatalf("selected node %q is not present in ring node set", first)
	}
}

func TestConsistentHashRingRemoveNodePurgesEntries(t *testing.T) {
	ring := NewConsistentHashRing()
	ring.AddNode("nodeA")
	ring.AddNode("nodeB")

	ring.RemoveNode("nodeB")

	if ring.nodes["nodeB"] {
		t.Fatal("nodeB should be removed from ring nodes")
	}

	for _, hash := range ring.virtualNodes {
		if ring.nodeMap[hash] == "nodeB" {
			t.Fatalf("found virtual node hash still pointing to removed nodeB: %d", hash)
		}
	}

	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		if got := ring.GetNode(key); got != "nodeA" {
			t.Fatalf("expected nodeA after removing nodeB, key=%q got=%q", key, got)
		}
	}
}
