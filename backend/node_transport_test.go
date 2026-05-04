package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func startMockNodeServer(t *testing.T, nodeID string, handler http.HandlerFunc) {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", getPortForNode(nodeID))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on %s for %s: %v", addr, nodeID, err)
	}

	server := &http.Server{Handler: handler}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		err := <-serveDone
		if err != nil && err != http.ErrServerClosed {
			t.Fatalf("mock server for %s returned error: %v", nodeID, err)
		}
	})
}

func TestRepairNodeSendsPayloadAndUpdatesStats(t *testing.T) {
	c := newTestCoordinator(t, "testRepairNode", 1)
	targetNodeID := "node11000"
	fixedTime := time.Now().UTC().Truncate(time.Second)

	received := make(chan map[string]interface{}, 1)
	startMockNodeServer(t, targetNodeID, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT method, got %s", r.Method)
		}
		if want := "/internal/repair/repair-key"; r.URL.Path != want {
			t.Errorf("expected request path %s, got %s", want, r.URL.Path)
		}

		defer r.Body.Close()
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		received <- body
		w.WriteHeader(http.StatusOK)
	})

	value := storedValue{
		Value:       "latest",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 3}},
		Timestamp:   fixedTime,
		Conflicts: []storedValue{
			{Value: "older", VectorClock: &VectorClock{Clock: map[string]int{"nodeB": 1}}, Timestamp: fixedTime.Add(-time.Minute)},
		},
	}

	c.repairNode(targetNodeID, "repair-key", value)

	body := <-received
	if body["value"] != "latest" {
		t.Fatalf("expected value latest, got %v", body["value"])
	}
	if body["timestamp"] != fixedTime.Format(time.RFC3339) {
		t.Fatalf("expected timestamp %s, got %v", fixedTime.Format(time.RFC3339), body["timestamp"])
	}

	vc := body["vector_clock"].(map[string]interface{})
	if got := int(vc["nodeA"].(float64)); got != 3 {
		t.Fatalf("expected vector clock nodeA=3, got %d", got)
	}

	conflicts := body["conflicts"].([]interface{})
	if len(conflicts) != 1 {
		t.Fatalf("expected 1 conflict payload, got %d", len(conflicts))
	}

	conflict := conflicts[0].(map[string]interface{})
	if conflict["value"] != "older" {
		t.Fatalf("expected conflict value older, got %v", conflict["value"])
	}

	if c.Stats.ReadRepairCount != 1 {
		t.Fatalf("expected read repair count 1, got %d", c.Stats.ReadRepairCount)
	}
	if c.Stats.ConflictsResolved != 1 {
		t.Fatalf("expected conflicts resolved count 1, got %d", c.Stats.ConflictsResolved)
	}
}

func TestRepairNodeDoesNotUpdateStatsOnFailure(t *testing.T) {
	c := newTestCoordinator(t, "testRepairNodeFail", 1)
	targetNodeID := "node11001"

	startMockNodeServer(t, targetNodeID, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	c.repairNode(targetNodeID, "repair-key", storedValue{
		Value:       "value",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}},
		Timestamp:   time.Now().UTC(),
	})

	if c.Stats.ReadRepairCount != 0 {
		t.Fatalf("expected read repair count to remain 0, got %d", c.Stats.ReadRepairCount)
	}
	if c.Stats.ConflictsResolved != 0 {
		t.Fatalf("expected conflicts resolved count to remain 0, got %d", c.Stats.ConflictsResolved)
	}
}

func TestDeliverHintSendsHintPayloadAndUpdatesStats(t *testing.T) {
	c := newTestCoordinator(t, "originNode", 1)
	targetNodeID := "node11002"
	fixedTime := time.Now().UTC().Truncate(time.Second)

	received := make(chan map[string]interface{}, 1)
	startMockNodeServer(t, targetNodeID, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT method, got %s", r.Method)
		}
		if want := "/internal/kv/hint-key"; r.URL.Path != want {
			t.Errorf("expected request path %s, got %s", want, r.URL.Path)
		}

		defer r.Body.Close()
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		received <- body
		w.WriteHeader(http.StatusOK)
	})

	hint := HintedWrite{
		Key:         "hint-key",
		Value:       "hint-value",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2}},
		TargetNode:  targetNodeID,
		Timestamp:   fixedTime,
	}

	if ok := c.deliverHint(hint); !ok {
		t.Fatal("expected deliverHint to succeed")
	}

	body := <-received
	if body["value"] != "hint-value" {
		t.Fatalf("expected hint value, got %v", body["value"])
	}
	if body["origin_node"] != c.NodeID {
		t.Fatalf("expected origin node %s, got %v", c.NodeID, body["origin_node"])
	}
	if body["is_hint"] != true {
		t.Fatalf("expected is_hint=true, got %v", body["is_hint"])
	}
	if body["timestamp"] != fixedTime.Format(time.RFC3339) {
		t.Fatalf("expected timestamp %s, got %v", fixedTime.Format(time.RFC3339), body["timestamp"])
	}

	vc := body["vector_clock"].(map[string]interface{})
	if got := int(vc["nodeA"].(float64)); got != 2 {
		t.Fatalf("expected vector clock nodeA=2, got %d", got)
	}

	if c.Stats.HintDeliverCount != 1 {
		t.Fatalf("expected hint delivery count 1, got %d", c.Stats.HintDeliverCount)
	}
}

func TestProcessHintsRemovesDeliveredHints(t *testing.T) {
	c := newTestCoordinator(t, "originNodeProcessHints", 1)
	targetNodeID := "node11003"

	startMockNodeServer(t, targetNodeID, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	c.Hints[targetNodeID] = []HintedWrite{{
		Key:         "hint-key",
		Value:       "hint-value",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}},
		TargetNode:  targetNodeID,
		Timestamp:   time.Now().UTC(),
	}}

	c.processHints()

	if _, exists := c.Hints[targetNodeID]; exists {
		t.Fatalf("expected delivered hints for %s to be removed", targetNodeID)
	}
	if c.Stats.HintDeliverCount != 1 {
		t.Fatalf("expected hint delivery count 1 after processHints, got %d", c.Stats.HintDeliverCount)
	}
}
