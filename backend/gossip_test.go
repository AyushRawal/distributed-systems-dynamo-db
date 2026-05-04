package main

import (
	"testing"
	"time"
)

func TestNewGossipServiceInitializesMembers(t *testing.T) {
	gs := NewGossipService("nodeA", []string{"nodeA", "nodeB", "nodeC"})

	if gs.Self == nil {
		t.Fatal("expected self member to be initialized")
	}

	if gs.Self.NodeID != "nodeA" {
		t.Fatalf("expected self nodeA, got %q", gs.Self.NodeID)
	}

	if gs.Self.Status != StatusAlive {
		t.Fatalf("expected self status alive, got %s", gs.Self.Status)
	}

	if _, exists := gs.Members.Load("nodeA"); exists {
		t.Fatal("self node should not be in members map")
	}

	for _, nodeID := range []string{"nodeB", "nodeC"} {
		value, ok := gs.Members.Load(nodeID)
		if !ok {
			t.Fatalf("expected member %s to exist", nodeID)
		}
		member := value.(*Member)
		if member.Status != StatusAlive {
			t.Fatalf("expected %s to be alive, got %s", nodeID, member.Status)
		}
	}
}

func TestSelectGossipTargetsSkipsDownMembersAndSelf(t *testing.T) {
	gs := NewGossipService("nodeA", []string{"nodeA", "nodeB", "nodeC", "nodeD"})

	value, _ := gs.Members.Load("nodeC")
	nodeC := value.(*Member)
	nodeC.Status = StatusDown
	gs.Members.Store("nodeC", nodeC)

	targets := gs.selectGossipTargets(10)
	if len(targets) == 0 {
		t.Fatal("expected at least one gossip target")
	}

	for _, target := range targets {
		if target.NodeID == "nodeA" {
			t.Fatal("self node must not be selected as gossip target")
		}
		if target.Status == StatusDown {
			t.Fatalf("down node selected as target: %s", target.NodeID)
		}
	}
}

func TestUpdateMemberBehavior(t *testing.T) {
	gs := NewGossipService("nodeA", []string{"nodeA", "nodeB"})

	t.Run("heartbeat increase updates status and heartbeat", func(t *testing.T) {
		gs.Members.Store("nodeB", &Member{
			NodeID:    "nodeB",
			Host:      "localhost",
			Port:      5001,
			Heartbeat: 1,
			Status:    StatusSuspected,
			LastSeen:  time.Now().Add(-5 * time.Second),
		})

		gs.updateMember(&Member{
			NodeID:    "nodeB",
			Host:      "localhost",
			Port:      5001,
			Heartbeat: 3,
			Status:    StatusAlive,
		})

		value, _ := gs.Members.Load("nodeB")
		member := value.(*Member)
		if member.Heartbeat != 3 {
			t.Fatalf("expected heartbeat 3, got %d", member.Heartbeat)
		}
		if member.Status != StatusAlive {
			t.Fatalf("expected status alive after heartbeat increase, got %s", member.Status)
		}
	})

	t.Run("down report marks node suspected", func(t *testing.T) {
		gs.Members.Store("nodeB", &Member{
			NodeID:    "nodeB",
			Host:      "localhost",
			Port:      5001,
			Heartbeat: 5,
			Status:    StatusAlive,
			LastSeen:  time.Now(),
		})

		gs.updateMember(&Member{
			NodeID:    "nodeB",
			Heartbeat: 5,
			Status:    StatusDown,
		})

		value, _ := gs.Members.Load("nodeB")
		member := value.(*Member)
		if member.Status != StatusSuspected {
			t.Fatalf("expected status suspected after down report, got %s", member.Status)
		}
	})

	t.Run("self update only refreshes last seen", func(t *testing.T) {
		before := gs.Self.LastSeen
		time.Sleep(1 * time.Millisecond)

		gs.updateMember(&Member{NodeID: "nodeA", Heartbeat: 999, Status: StatusDown})

		if !gs.Self.LastSeen.After(before) {
			t.Fatal("expected self last seen to be refreshed")
		}
		if gs.Self.Status != StatusAlive {
			t.Fatalf("self status should remain alive, got %s", gs.Self.Status)
		}
	})
}

func TestCheckMemberStatusesTransitions(t *testing.T) {
	gs := NewGossipService("nodeA", []string{"nodeA", "nodeB", "nodeC", "nodeD"})
	now := time.Now()

	gs.Members.Store("nodeB", &Member{NodeID: "nodeB", Host: "localhost", Port: 5001, Status: StatusAlive, LastSeen: now.Add(-2 * time.Second)})
	gs.Members.Store("nodeC", &Member{NodeID: "nodeC", Host: "localhost", Port: 5002, Status: StatusAlive, LastSeen: now.Add(-4 * time.Second)})
	gs.Members.Store("nodeD", &Member{NodeID: "nodeD", Host: "localhost", Port: 5003, Status: StatusAlive, LastSeen: now.Add(-7 * time.Second)})

	gs.checkMemberStatuses()

	assertMemberStatus(t, gs, "nodeB", StatusAlive)
	assertMemberStatus(t, gs, "nodeC", StatusSuspected)
	assertMemberStatus(t, gs, "nodeD", StatusDown)
}

func TestGetNodeStatusUnknownNodeBecomesSuspected(t *testing.T) {
	gs := NewGossipService("nodeA", []string{"nodeA"})

	status := gs.getNodeStatus("nodeX")
	if status != StatusSuspected {
		t.Fatalf("expected suspected for unknown node, got %s", status)
	}

	value, exists := gs.Members.Load("nodeX")
	if !exists {
		t.Fatal("expected unknown node to be inserted into members map")
	}
	if value.(*Member).Status != StatusSuspected {
		t.Fatalf("expected inserted unknown member to be suspected, got %s", value.(*Member).Status)
	}
}

func TestGetLiveMembersAndForceNodeDown(t *testing.T) {
	gs := NewGossipService("nodeA", []string{"nodeA", "nodeB", "nodeC"})

	gs.ForceNodeDown("nodeB")
	live := gs.GetLiveMembers()

	for _, m := range live {
		if m.NodeID == "nodeB" {
			t.Fatal("nodeB should not be considered live after ForceNodeDown")
		}
	}

	gs.ForceNodeDown("nodeZ")
	value, exists := gs.Members.Load("nodeZ")
	if !exists {
		t.Fatal("expected missing nodeZ to be added by ForceNodeDown")
	}
	if value.(*Member).Status != StatusDown {
		t.Fatalf("expected nodeZ to be down, got %s", value.(*Member).Status)
	}
}

func assertMemberStatus(t *testing.T, gs *GossipService, nodeID string, want NodeStatus) {
	t.Helper()
	value, ok := gs.Members.Load(nodeID)
	if !ok {
		t.Fatalf("member %s not found", nodeID)
	}
	got := value.(*Member).Status
	if got != want {
		t.Fatalf("member %s status mismatch: want %s got %s", nodeID, want, got)
	}
}
