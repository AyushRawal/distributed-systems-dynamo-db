package main

import (
	"context"
	"testing"
	"time"

	internalpb "dynamo-db/proto"
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
	repairBody := make(chan storedValue, 1)

	startMockNodeServer(t, targetNodeID, &mockInternalServiceServer{
		getMerkleBucket: func(ctx context.Context, req *internalpb.GetMerkleBucketRequest) (*internalpb.MerkleTreeResponse, error) {
			merkleHits++
			return merkleTreeToProto(NewMerkleTree(map[string]interface{}{})), nil
		},
		getLocal: func(ctx context.Context, req *internalpb.GetLocalRequest) (*internalpb.GetLocalResponse, error) {
			if req.Key != key {
				t.Fatalf("expected key %s, got %s", key, req.Key)
			}
			internalGetHits++
			return &internalpb.GetLocalResponse{Found: false}, nil
		},
		repairKey: func(ctx context.Context, req *internalpb.RepairKeyRequest) (*internalpb.OperationStatus, error) {
			if req.Key != key {
				t.Fatalf("expected repair key %s, got %s", key, req.Key)
			}
			repairHits++
			repairBody <- protoToStoredValue(req.Value)
			return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
		},
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
	if body.Value != "local-value" {
		t.Fatalf("expected repaired value local-value, got %v", body.Value)
	}
	if got := body.VectorClock.Clock["nodeA"]; got != 2 {
		t.Fatalf("expected repaired vector clock nodeA=2, got %d", got)
	}
}
