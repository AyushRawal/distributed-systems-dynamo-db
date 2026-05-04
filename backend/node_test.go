package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestCoordinator(t *testing.T, nodeID string, replication int) *Coordinator {
	t.Helper()

	ring := NewConsistentHashRing()
	ring.AddNode(nodeID)

	c := &Coordinator{
		Node: &Node{
			NodeID:      nodeID,
			Ring:        ring,
			Replication: replication,
			DataStore:   make(map[string]storedValue),
			Hints:       make(map[string][]HintedWrite),
		},
		ReadQuorum:  1,
		WriteQuorum: 1,
	}

	t.Cleanup(func() {
		_ = os.Remove(filepath.Join(dataDir, nodeID+".json"))
		_ = os.Remove(filepath.Join("logs", nodeID+".txt"))
	})

	return c
}

func TestLocalPutStoresValue(t *testing.T) {
	c := newTestCoordinator(t, "testLocalPutStore", 1)
	vc := &VectorClock{Clock: map[string]int{"testLocalPutStore": 1}}

	ok := c.localPut("k1", "v1", vc)
	if !ok {
		t.Fatal("expected localPut to return true")
	}

	stored := c.localGet("k1")
	if stored.Value != "v1" {
		t.Fatalf("expected stored value v1, got %v", stored.Value)
	}

	if stored.VectorClock.Clock["testLocalPutStore"] != 1 {
		t.Fatalf("expected vector clock count 1, got %d", stored.VectorClock.Clock["testLocalPutStore"])
	}
}

func TestLocalPutIgnoresOlderWrite(t *testing.T) {
	c := newTestCoordinator(t, "testLocalPutOlder", 1)
	c.DataStore["k1"] = storedValue{
		Value:       "current",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2}},
		Timestamp:   time.Now(),
	}

	ok := c.localPut("k1", "older", &VectorClock{Clock: map[string]int{"nodeA": 1}})
	if !ok {
		t.Fatal("expected localPut to return true for older write")
	}

	stored := c.localGet("k1")
	if stored.Value != "current" {
		t.Fatalf("expected existing value to remain, got %v", stored.Value)
	}
}

func TestLocalPutConcurrentWriteCreatesConflict(t *testing.T) {
	c := newTestCoordinator(t, "testLocalPutConcurrent", 1)
	c.DataStore["k1"] = storedValue{
		Value:       "vA",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}},
		Timestamp:   time.Now(),
	}

	ok := c.localPut("k1", "vB", &VectorClock{Clock: map[string]int{"nodeB": 1}})
	if !ok {
		t.Fatal("expected localPut to return true for concurrent write")
	}

	stored := c.localGet("k1")
	if stored.Value != "vB" {
		t.Fatalf("expected latest stored value vB, got %v", stored.Value)
	}

	if len(stored.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict entry, got %d", len(stored.Conflicts))
	}

	if stored.Conflicts[0].Value != "vA" {
		t.Fatalf("expected conflict to preserve prior value vA, got %v", stored.Conflicts[0].Value)
	}

	if stored.VectorClock.Clock["nodeA"] != 1 || stored.VectorClock.Clock["nodeB"] != 1 {
		t.Fatalf("expected merged vector clock with nodeA=1 and nodeB=1, got %v", stored.VectorClock.Clock)
	}

	if c.Stats.ConflictsDetected != 1 {
		t.Fatalf("expected conflict counter to increment to 1, got %d", c.Stats.ConflictsDetected)
	}
}

func TestResolveConflictsNewerValueWins(t *testing.T) {
	c := newTestCoordinator(t, "testResolveNewer", 1)

	responses := map[string]storedValue{
		"nodeA": {
			Value:       "old",
			VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}},
		},
		"nodeB": {
			Value:       "new",
			VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2}},
		},
	}

	result, conflicts := c.resolveConflicts(responses)
	if conflicts != 0 {
		t.Fatalf("expected 0 conflicts, got %d", conflicts)
	}

	if result.Value != "new" {
		t.Fatalf("expected newer value to win, got %v", result.Value)
	}
}

func TestResolveConflictsConcurrentValues(t *testing.T) {
	c := newTestCoordinator(t, "testResolveConcurrent", 1)

	responses := map[string]storedValue{
		"nodeA": {
			Value:       "vA",
			VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}},
		},
		"nodeB": {
			Value:       "vB",
			VectorClock: &VectorClock{Clock: map[string]int{"nodeB": 1}},
		},
	}

	result, conflicts := c.resolveConflicts(responses)
	if conflicts != 1 {
		t.Fatalf("expected 1 conflict, got %d", conflicts)
	}

	if len(result.Conflicts) != 1 {
		t.Fatalf("expected merged result to include 1 conflict entry, got %d", len(result.Conflicts))
	}
}

func TestFormatResultIncludesConflictsConditionally(t *testing.T) {
	c := newTestCoordinator(t, "testFormatResult", 1)

	value := storedValue{
		Value:       "latest",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2}},
		Conflicts: []storedValue{
			{Value: "oldA", VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}}},
			{Value: "oldB", VectorClock: &VectorClock{Clock: map[string]int{"nodeB": 1}}},
		},
	}

	withConflicts := c.formatResult(value, 2)
	if _, ok := withConflicts["conflicts"]; !ok {
		t.Fatal("expected conflicts field when conflict count > 0")
	}

	withoutConflicts := c.formatResult(value, 0)
	if _, ok := withoutConflicts["conflicts"]; ok {
		t.Fatal("did not expect conflicts field when conflict count is 0")
	}
}

func TestUpdateLocalVectorClock(t *testing.T) {
	c := newTestCoordinator(t, "nodeA", 1)

	first := c.updateLocalVectorClock("new-key")
	if first.Clock["nodeA"] != 1 {
		t.Fatalf("expected first increment to 1, got %d", first.Clock["nodeA"])
	}

	c.DataStore["existing"] = storedValue{
		Value:       "v",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 3, "nodeB": 2}},
		Timestamp:   time.Now(),
	}

	next := c.updateLocalVectorClock("existing")
	if next.Clock["nodeA"] != 4 {
		t.Fatalf("expected nodeA to increment from 3 to 4, got %d", next.Clock["nodeA"])
	}

	if next.Clock["nodeB"] != 2 {
		t.Fatalf("expected nodeB component to be preserved at 2, got %d", next.Clock["nodeB"])
	}
}

func TestParseStoredValue(t *testing.T) {
	timestamp := time.Now().UTC().Truncate(time.Second)

	parsed := parseStoredValue(map[string]interface{}{
		"value":        "hello",
		"vector_clock": map[string]interface{}{"nodeA": float64(2)},
		"timestamp":    timestamp.Format(time.RFC3339),
	})

	if parsed.Value != "hello" {
		t.Fatalf("expected parsed value hello, got %v", parsed.Value)
	}

	if parsed.VectorClock.Clock["nodeA"] != 2 {
		t.Fatalf("expected parsed vector clock nodeA=2, got %d", parsed.VectorClock.Clock["nodeA"])
	}

	if !parsed.Timestamp.Equal(timestamp) {
		t.Fatalf("expected parsed timestamp %v, got %v", timestamp, parsed.Timestamp)
	}

	withConflicts := parseStoredValue(map[string]interface{}{
		"value":        "latest",
		"vector_clock": map[string]interface{}{"nodeA": float64(3)},
		"timestamp":    timestamp.Format(time.RFC3339),
		"conflicts": []interface{}{
			map[string]interface{}{
				"value":        "older",
				"vector_clock": map[string]interface{}{"nodeB": float64(1)},
			},
		},
	})

	if len(withConflicts.Conflicts) != 1 {
		t.Fatalf("expected 1 parsed conflict, got %d", len(withConflicts.Conflicts))
	}
	if withConflicts.Conflicts[0].Value != "older" {
		t.Fatalf("expected parsed conflict value older, got %v", withConflicts.Conflicts[0].Value)
	}

	empty := parseStoredValue(map[string]interface{}{})
	if empty.Value != nil {
		t.Fatalf("expected empty parsed value for missing payload, got %v", empty.Value)
	}
}

func TestHelpers(t *testing.T) {
	if !contains([]string{"nodeA", "nodeB"}, "nodeB") {
		t.Fatal("expected contains to find nodeB")
	}

	if contains([]string{"nodeA", "nodeB"}, "nodeC") {
		t.Fatal("did not expect contains to find nodeC")
	}

	if got := getHost("backend-node-1"); got != "backend-node" {
		t.Fatalf("expected backend-node host, got %q", got)
	}

	if got := getHost("nodeA"); got != "localhost" {
		t.Fatalf("expected localhost fallback, got %q", got)
	}

	if got := backoffDelay(0); got != baseRetryDelay {
		t.Fatalf("expected base delay %v, got %v", baseRetryDelay, got)
	}

	if got := backoffDelay(3); got != 8*baseRetryDelay {
		t.Fatalf("expected exponential backoff 8x base, got %v", got)
	}
}

func TestIsNodeAvailableWithoutGossipDefaultsTrue(t *testing.T) {
	c := newTestCoordinator(t, "testNodeAvailability", 1)

	if !c.isNodeAvailable("nodeX") {
		t.Fatal("expected node to be treated available when gossip service is nil")
	}
}
