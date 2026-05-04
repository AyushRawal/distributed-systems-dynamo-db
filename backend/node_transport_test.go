package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	internalpb "dynamo-db/proto"
	"google.golang.org/grpc"
)

type mockInternalServiceServer struct {
	internalpb.UnimplementedInternalServiceServer
	getLocal        func(context.Context, *internalpb.GetLocalRequest) (*internalpb.GetLocalResponse, error)
	putLocal        func(context.Context, *internalpb.PutLocalRequest) (*internalpb.OperationStatus, error)
	repairKey       func(context.Context, *internalpb.RepairKeyRequest) (*internalpb.OperationStatus, error)
	getMerkleBucket func(context.Context, *internalpb.GetMerkleBucketRequest) (*internalpb.MerkleTreeResponse, error)
}

func (m *mockInternalServiceServer) GetLocal(ctx context.Context, req *internalpb.GetLocalRequest) (*internalpb.GetLocalResponse, error) {
	if m.getLocal != nil {
		return m.getLocal(ctx, req)
	}
	return &internalpb.GetLocalResponse{Found: false}, nil
}

func (m *mockInternalServiceServer) PutLocal(ctx context.Context, req *internalpb.PutLocalRequest) (*internalpb.OperationStatus, error) {
	if m.putLocal != nil {
		return m.putLocal(ctx, req)
	}
	return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
}

func (m *mockInternalServiceServer) RepairKey(ctx context.Context, req *internalpb.RepairKeyRequest) (*internalpb.OperationStatus, error) {
	if m.repairKey != nil {
		return m.repairKey(ctx, req)
	}
	return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
}

func (m *mockInternalServiceServer) GetMerkleBucket(ctx context.Context, req *internalpb.GetMerkleBucketRequest) (*internalpb.MerkleTreeResponse, error) {
	if m.getMerkleBucket != nil {
		return m.getMerkleBucket(ctx, req)
	}
	return merkleTreeToProto(NewMerkleTree(map[string]interface{}{})), nil
}

func startMockNodeServer(t *testing.T, nodeID string, serverImpl *mockInternalServiceServer) {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", getGRPCPortForNode(nodeID))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on %s for %s: %v", addr, nodeID, err)
	}

	server := grpc.NewServer()
	internalpb.RegisterInternalServiceServer(server, serverImpl)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.GracefulStop()
		err := <-serveDone
		if err != nil {
			t.Fatalf("mock gRPC server for %s returned error: %v", nodeID, err)
		}
	})
}

func TestRepairNodeSendsPayloadAndUpdatesStats(t *testing.T) {
	c := newTestCoordinator(t, "testRepairNode", 1)
	targetNodeID := "node11000"
	fixedTime := time.Now().UTC().Truncate(time.Second)

	received := make(chan storedValue, 1)
	startMockNodeServer(t, targetNodeID, &mockInternalServiceServer{
		repairKey: func(ctx context.Context, req *internalpb.RepairKeyRequest) (*internalpb.OperationStatus, error) {
			if req.Key != "repair-key" {
				t.Errorf("expected repair key, got %s", req.Key)
			}
			received <- protoToStoredValue(req.Value)
			return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
		},
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
	if body.Value != "latest" {
		t.Fatalf("expected value latest, got %v", body.Value)
	}
	if !body.Timestamp.Equal(fixedTime) {
		t.Fatalf("expected timestamp %s, got %v", fixedTime.Format(time.RFC3339), body.Timestamp)
	}
	if got := body.VectorClock.Clock["nodeA"]; got != 3 {
		t.Fatalf("expected vector clock nodeA=3, got %d", got)
	}
	if len(body.Conflicts) != 1 {
		t.Fatalf("expected 1 conflict payload, got %d", len(body.Conflicts))
	}
	if body.Conflicts[0].Value != "older" {
		t.Fatalf("expected conflict value older, got %v", body.Conflicts[0].Value)
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

	startMockNodeServer(t, targetNodeID, &mockInternalServiceServer{
		repairKey: func(ctx context.Context, req *internalpb.RepairKeyRequest) (*internalpb.OperationStatus, error) {
			return &internalpb.OperationStatus{Ok: false, Message: "fail"}, nil
		},
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

	received := make(chan *internalpb.PutLocalRequest, 1)
	startMockNodeServer(t, targetNodeID, &mockInternalServiceServer{
		putLocal: func(ctx context.Context, req *internalpb.PutLocalRequest) (*internalpb.OperationStatus, error) {
			received <- req
			return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
		},
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

	req := <-received
	body := protoToStoredValue(req.Value)
	if body.Value != "hint-value" {
		t.Fatalf("expected hint value, got %v", body.Value)
	}
	if req.OriginNode != c.NodeID {
		t.Fatalf("expected origin node %s, got %v", c.NodeID, req.OriginNode)
	}
	if !req.IsHint {
		t.Fatalf("expected is_hint=true, got %v", req.IsHint)
	}
	if !body.Timestamp.Equal(fixedTime) {
		t.Fatalf("expected timestamp %s, got %v", fixedTime.Format(time.RFC3339), body.Timestamp)
	}
	if got := body.VectorClock.Clock["nodeA"]; got != 2 {
		t.Fatalf("expected vector clock nodeA=2, got %d", got)
	}

	if c.Stats.HintDeliverCount != 1 {
		t.Fatalf("expected hint delivery count 1, got %d", c.Stats.HintDeliverCount)
	}
}

func TestProcessHintsRemovesDeliveredHints(t *testing.T) {
	c := newTestCoordinator(t, "originNodeProcessHints", 1)
	targetNodeID := "node11003"

	startMockNodeServer(t, targetNodeID, &mockInternalServiceServer{
		putLocal: func(ctx context.Context, req *internalpb.PutLocalRequest) (*internalpb.OperationStatus, error) {
			return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
		},
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
