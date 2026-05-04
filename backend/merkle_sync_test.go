package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestBuildMerkleTreeForBucketIncludesVectorClockState(t *testing.T) {
	c := newTestCoordinator(t, "testMerkleBucketState", 1)
	key := "stateful-key"

	c.DataStore[key] = storedValue{
		Value:       "same-value",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 1}},
		Timestamp:   time.Now().UTC(),
	}

	firstRoot := c.buildMerkleTreeForBucket(bucketForKey(key)).Root()
	if firstRoot == "" {
		t.Fatal("expected non-empty Merkle root")
	}

	c.DataStore[key] = storedValue{
		Value:       "same-value",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2}},
		Timestamp:   time.Now().UTC(),
	}

	secondRoot := c.buildMerkleTreeForBucket(bucketForKey(key)).Root()
	if firstRoot == secondRoot {
		t.Fatal("expected Merkle root to change when vector clock changes")
	}
}

func TestPerformMerkleSyncWithNodeRepairsOnlyDifferingKey(t *testing.T) {
	c := newTestCoordinator(t, "nodeA", 1)
	key := "sync-key"
	fixedTime := time.Now().UTC().Truncate(time.Second)

	c.DataStore[key] = storedValue{
		Value:       "local-value",
		VectorClock: &VectorClock{Clock: map[string]int{"nodeA": 2}},
		Timestamp:   fixedTime,
	}

	targetNodeID := "node11004"
	merkleHits := 0
	internalGetHits := 0
	repairHits := 0
	repairBody := make(chan map[string]interface{}, 1)

	startMockNodeServer(t, targetNodeID, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/internal/merkle/"):
			merkleHits++
			payload, err := json.Marshal(NewMerkleTree(map[string]interface{}{}).SerializeToMap())
			if err != nil {
				t.Fatalf("failed to marshal Merkle response: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(payload)
		case r.Method == http.MethodGet && r.URL.Path == "/internal/kv/"+key:
			internalGetHits++
			http.NotFound(w, r)
		case r.Method == http.MethodPut && r.URL.Path == "/internal/repair/"+key:
			repairHits++
			defer r.Body.Close()
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("failed to decode repair request: %v", err)
			}
			repairBody <- body
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})

	c.performMerkleSyncWithNode(targetNodeID)

	if merkleHits != merkleBucketCount {
		t.Fatalf("expected %d Merkle requests, got %d", merkleBucketCount, merkleHits)
	}
	if internalGetHits != maxRetryAttempts {
		t.Fatalf("expected %d internal GET retries for differing key, got %d", maxRetryAttempts, internalGetHits)
	}
	if repairHits != 1 {
		t.Fatalf("expected 1 repair PUT for missing remote key, got %d", repairHits)
	}

	body := <-repairBody
	if body["value"] != "local-value" {
		t.Fatalf("expected repaired value local-value, got %v", body["value"])
	}
	vc := body["vector_clock"].(map[string]interface{})
	if got := int(vc["nodeA"].(float64)); got != 2 {
		t.Fatalf("expected repaired vector clock nodeA=2, got %d", got)
	}
}
