package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	internalpb "dynamo-db/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var grpcClients = struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]internalpb.InternalServiceClient
}{
	conns:   make(map[string]*grpc.ClientConn),
	clients: make(map[string]internalpb.InternalServiceClient),
}

type internalServiceServer struct {
	internalpb.UnimplementedInternalServiceServer
	coordinator *Coordinator
}

func startGRPCServer(config *Config, c *Coordinator) {
	addr := fmt.Sprintf(":%d", config.GRPCPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen for gRPC on %s: %v", addr, err)
	}

	server := grpc.NewServer()
	internalpb.RegisterInternalServiceServer(server, &internalServiceServer{coordinator: c})

	go func() {
		log.Printf("Node %s starting internal gRPC server on port %d...", config.NodeID, config.GRPCPort)
		if err := server.Serve(listener); err != nil {
			log.Fatalf("internal gRPC server failed: %v", err)
		}
	}()
}

func getInternalServiceClient(nodeID string) (internalpb.InternalServiceClient, error) {
	grpcClients.mu.Lock()
	defer grpcClients.mu.Unlock()

	if client, ok := grpcClients.clients[nodeID]; ok {
		return client, nil
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("%s:%d", getHost(nodeID), getGRPCPortForNode(nodeID)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	client := internalpb.NewInternalServiceClient(conn)
	grpcClients.conns[nodeID] = conn
	grpcClients.clients[nodeID] = client
	return client, nil
}

func vectorClockToProto(vc *VectorClock) *internalpb.VectorClock {
	clock := make(map[string]int32)
	if vc != nil {
		for nodeID, count := range vc.Clock {
			clock[nodeID] = int32(count)
		}
	}
	return &internalpb.VectorClock{Clock: clock}
}

func protoToVectorClock(vc *internalpb.VectorClock) *VectorClock {
	clock := make(map[string]int)
	if vc != nil {
		for nodeID, count := range vc.Clock {
			clock[nodeID] = int(count)
		}
	}
	return &VectorClock{Clock: clock}
}

func storedValueToProto(value storedValue) (*internalpb.StoredValue, error) {
	protoValue, err := structpb.NewValue(value.Value)
	if err != nil {
		return nil, err
	}

	conflicts := make([]*internalpb.StoredValue, 0, len(value.Conflicts))
	for _, conflict := range value.Conflicts {
		protoConflict, err := storedValueToProto(conflict)
		if err != nil {
			return nil, err
		}
		conflicts = append(conflicts, protoConflict)
	}

	return &internalpb.StoredValue{
		Value:             protoValue,
		VectorClock:       vectorClockToProto(value.VectorClock),
		Conflicts:         conflicts,
		TimestampUnixNano: value.Timestamp.UnixNano(),
	}, nil
}

func protoToStoredValue(value *internalpb.StoredValue) storedValue {
	if value == nil {
		return storedValue{}
	}

	conflicts := make([]storedValue, 0, len(value.Conflicts))
	for _, conflict := range value.Conflicts {
		conflicts = append(conflicts, protoToStoredValue(conflict))
	}

	timestamp := time.Unix(0, value.TimestampUnixNano)
	if value.TimestampUnixNano == 0 {
		timestamp = time.Now()
	}

	stored := storedValue{
		VectorClock: protoToVectorClock(value.VectorClock),
		Conflicts:   conflicts,
		Timestamp:   timestamp,
	}
	if value.Value != nil {
		stored.Value = value.Value.AsInterface()
	}
	return stored
}

func merkleTreeToProto(tree *MerkleTree) *internalpb.MerkleTreeResponse {
	levels := make([]*internalpb.MerkleLevel, 0, len(tree.Levels))
	for _, level := range tree.Levels {
		levels = append(levels, &internalpb.MerkleLevel{Hashes: append([]string(nil), level...)})
	}

	keyMap := make(map[string]string, len(tree.KeyMap))
	for key, value := range tree.KeyMap {
		keyMap[key] = value
	}

	return &internalpb.MerkleTreeResponse{
		Leaves:  append([]string(nil), tree.Leaves...),
		Levels:  levels,
		KeyMap:  keyMap,
		Version: int32(tree.Version),
		Root:    tree.Root(),
	}
}

func protoToMerkleTree(resp *internalpb.MerkleTreeResponse) *MerkleTree {
	tree := &MerkleTree{
		Leaves:  append([]string(nil), resp.Leaves...),
		Levels:  make([][]string, 0, len(resp.Levels)),
		KeyMap:  make(map[string]string, len(resp.KeyMap)),
		Version: int(resp.Version),
	}

	for _, level := range resp.Levels {
		tree.Levels = append(tree.Levels, append([]string(nil), level.Hashes...))
	}
	for key, value := range resp.KeyMap {
		tree.KeyMap[key] = value
	}
	return tree
}

func memberToProto(member *Member) *internalpb.MemberState {
	if member == nil {
		return nil
	}

	return &internalpb.MemberState{
		NodeId:           member.NodeID,
		Host:             member.Host,
		Port:             int32(member.Port),
		GrpcPort:         int32(member.GRPCPort),
		Heartbeat:        member.Heartbeat,
		Status:           string(member.Status),
		LastSeenUnixNano: member.LastSeen.UnixNano(),
	}
}

func protoToMember(member *internalpb.MemberState) *Member {
	if member == nil {
		return nil
	}

	return &Member{
		NodeID:    member.NodeId,
		Host:      member.Host,
		Port:      int(member.Port),
		GRPCPort:  int(member.GrpcPort),
		Heartbeat: member.Heartbeat,
		Status:    NodeStatus(member.Status),
		LastSeen:  time.Unix(0, member.LastSeenUnixNano),
	}
}

func (s *internalServiceServer) GetLocal(ctx context.Context, req *internalpb.GetLocalRequest) (*internalpb.GetLocalResponse, error) {
	value := s.coordinator.localGet(req.Key)
	if value.Value == nil {
		return &internalpb.GetLocalResponse{Found: false}, nil
	}

	protoValue, err := storedValueToProto(value)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &internalpb.GetLocalResponse{Found: true, Value: protoValue}, nil
}

func (s *internalServiceServer) PutLocal(ctx context.Context, req *internalpb.PutLocalRequest) (*internalpb.OperationStatus, error) {
	incoming := protoToStoredValue(req.Value)
	s.coordinator.applyReplicaWrite(req.Key, incoming, replicaWriteOptions{
		ForceSync:  req.ForceSync,
		ForceKey:   req.ForceKey,
		IsHint:     req.IsHint,
		OriginNode: req.OriginNode,
	})
	return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
}

func (s *internalServiceServer) GetMerkleBucket(ctx context.Context, req *internalpb.GetMerkleBucketRequest) (*internalpb.MerkleTreeResponse, error) {
	return merkleTreeToProto(s.coordinator.buildMerkleTreeForBucket(int(req.Bucket))), nil
}

func (s *internalServiceServer) RepairKey(ctx context.Context, req *internalpb.RepairKeyRequest) (*internalpb.OperationStatus, error) {
	s.coordinator.applyRepair(req.Key, protoToStoredValue(req.Value))
	return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
}

func (s *internalServiceServer) StoreHint(ctx context.Context, req *internalpb.StoreHintRequest) (*internalpb.OperationStatus, error) {
	if req.Hint == nil {
		return &internalpb.OperationStatus{Ok: false, Message: "missing hint"}, nil
	}
	hintValue := protoToStoredValue(req.Hint.Value)
	s.coordinator.storeHint(req.Hint.TargetNode, req.Hint.Key, hintValue.Value, hintValue.VectorClock)
	return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
}

func (s *internalServiceServer) ExchangeGossip(ctx context.Context, req *internalpb.GossipRequest) (*internalpb.OperationStatus, error) {
	if s.coordinator.Gossip == nil {
		return &internalpb.OperationStatus{Ok: false, Message: "gossip unavailable"}, nil
	}

	members := make(map[string]*Member, len(req.Members))
	for nodeID, member := range req.Members {
		members[nodeID] = protoToMember(member)
	}

	s.coordinator.Gossip.processGossipPayload(gossipPayload{
		NodeID:    req.NodeId,
		Host:      req.Host,
		Port:      int(req.Port),
		GRPCPort:  int(req.GrpcPort),
		Heartbeat: req.Heartbeat,
		Members:   members,
	})

	return &internalpb.OperationStatus{Ok: true, Message: "OK"}, nil
}
