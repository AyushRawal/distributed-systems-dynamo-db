package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type NodeStatus string

const (
	StatusAlive     NodeStatus = "alive"
	StatusSuspected NodeStatus = "suspected"
	StatusDown      NodeStatus = "down"
)

type Member struct {
	NodeID    string
	Host      string
	Port      int
	Heartbeat int64
	Status    NodeStatus
	LastSeen  time.Time
	Metadata  map[string]string // For future extensions
}

type GossipService struct {
	Self     *Member
	Members  *sync.Map // map[string]*Member
	stopChan chan struct{}
}

// NewGossipService creates a new gossip service with the given node ID and list of all nodes
func NewGossipService(nodeID string, allNodes []string) *GossipService {
	self := &Member{
		NodeID:    nodeID,
		Host:      "localhost", // Default to localhost
		Port:      getPortForNode(nodeID),
		Heartbeat: 0,
		Status:    StatusAlive,
		LastSeen:  time.Now(),
	}

	gs := &GossipService{
		Self:     self,
		Members:  &sync.Map{},
		stopChan: make(chan struct{}),
	}

	// Initialize member list
	for _, nid := range allNodes {
		if nid != nodeID {
			gs.Members.Store(nid, &Member{
				NodeID:    nid,
				Host:      "localhost", // Default to localhost
				Port:      getPortForNode(nid),
				Heartbeat: 0,
				Status:    StatusAlive,
				LastSeen:  time.Now(),
			})
		}
	}

	return gs
}

func (gs *GossipService) Start() {
	textLog(gs.Self.NodeID, "GOSSIP", "Starting gossip service")
	go gs.gossipLoop()
	go gs.failureDetectionLoop()
	textLog(gs.Self.NodeID, "GOSSIP", "Gossip service started successfully")

	// Log initial cluster state
	gs.logClusterState()
}

func (gs *GossipService) Stop() {
	close(gs.stopChan)
}

// Fix for gossipLoop to run more frequently
func (gs *GossipService) gossipLoop() {
	ticker := time.NewTicker(500 * time.Millisecond) // Run twice a second for tests
	defer ticker.Stop()

	textLog(gs.Self.NodeID, "GOSSIP", "Started gossip loop (2x per second)")

	for {
		select {
		case <-ticker.C:
			textLog(gs.Self.NodeID, "GOSSIP", "Sending gossip and incrementing heartbeat")
			gs.sendGossip()
			gs.incrementHeartbeat()
		case <-gs.stopChan:
			textLog(gs.Self.NodeID, "GOSSIP", "Stopping gossip loop")
			return
		}
	}
}

// Fix for failureDetectionLoop to run more frequently
func (gs *GossipService) failureDetectionLoop() {
	ticker := time.NewTicker(1 * time.Second) // Run every second for tests
	defer ticker.Stop()

	textLog(gs.Self.NodeID, "GOSSIP", "Started failure detection loop")

	for {
		select {
		case <-ticker.C:
			textLog(gs.Self.NodeID, "GOSSIP", "Checking member statuses")
			gs.checkMemberStatuses()
		case <-gs.stopChan:
			textLog(gs.Self.NodeID, "GOSSIP", "Stopping failure detection loop")
			return
		}
	}
}

func (gs *GossipService) incrementHeartbeat() {
	gs.Self.Heartbeat++
	gs.Self.LastSeen = time.Now()
}

func (gs *GossipService) sendGossip() {
	targets := gs.selectGossipTargets(2) // Default fanout of 2
	for _, target := range targets {
		go gs.sendGossipToNode(target)
	}
}

func (gs *GossipService) selectGossipTargets(fanout int) []*Member {
	var targets []*Member
	gs.Members.Range(func(key, value interface{}) bool {
		member := value.(*Member)
		if member.NodeID != gs.Self.NodeID && member.Status != StatusDown {
			targets = append(targets, member)
		}
		return true
	})

	// Shuffle and limit to fanout count
	if len(targets) > fanout {
		rand.Shuffle(len(targets), func(i, j int) {
			targets[i], targets[j] = targets[j], targets[i]
		})
		return targets[:fanout]
	}
	return targets
}

// Fix for sendGossipToNode to improve reliability
func (gs *GossipService) sendGossipToNode(target *Member) {
	if target == nil || target.NodeID == gs.Self.NodeID {
		return
	}

	client := &http.Client{
		Timeout: 1 * time.Second, // Reduced timeout for faster tests
	}

	url := fmt.Sprintf("http://%s:%d/internal/gossip", target.Host, target.Port)
	payload := gs.createGossipPayload()

	// Make 5 attempts with backoff (up from 3)
	for retries := 0; retries < 5; retries++ {
		resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if target.Status != StatusAlive {
					textLog(gs.Self.NodeID, "GOSSIP", "Successfully contacted %s, marking as ALIVE", target.NodeID)
					gs.Members.Store(target.NodeID, &Member{
						NodeID:    target.NodeID,
						Host:      target.Host,
						Port:      target.Port,
						Status:    StatusAlive,
						LastSeen:  time.Now(),
						Heartbeat: target.Heartbeat,
					})
				}
				return
			}
		}

		// Exponential backoff - but shorter for tests
		time.Sleep(time.Duration(100*(1<<uint(retries))) * time.Millisecond)
	}

	// Mark as suspected after retries fail
	textLog(gs.Self.NodeID, "GOSSIP", "Failed to contact %s after 5 retries, marking as SUSPECTED", target.NodeID)
	gs.Members.Store(target.NodeID, &Member{
		NodeID:    target.NodeID,
		Host:      target.Host,
		Port:      target.Port,
		Status:    StatusSuspected,
		LastSeen:  target.LastSeen,
		Heartbeat: target.Heartbeat,
	})
}

func (gs *GossipService) createGossipPayload() []byte {
	state := map[string]interface{}{
		"node_id":   gs.Self.NodeID,
		"host":      gs.Self.Host,
		"port":      gs.Self.Port,
		"heartbeat": gs.Self.Heartbeat,
		"members":   gs.collectMemberStates(),
	}

	payload, _ := json.Marshal(state)
	return payload
}

func (gs *GossipService) collectMemberStates() map[string]interface{} {
	members := make(map[string]interface{})
	gs.Members.Range(func(key, value interface{}) bool {
		member := value.(*Member)
		members[member.NodeID] = map[string]interface{}{
			"host":      member.Host,
			"port":      member.Port,
			"heartbeat": member.Heartbeat,
			"status":    member.Status,
			"last_seen": member.LastSeen.UnixNano(),
		}
		return true
	})
	return members
}

// Fix for HandleGossip to better handle failing nodes
func (gs *GossipService) HandleGossip(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		NodeID    string                 `json:"node_id"`
		Host      string                 `json:"host"`
		Port      int                    `json:"port"`
		Heartbeat int64                  `json:"heartbeat"`
		Members   map[string]interface{} `json:"members"`
	}

	defer r.Body.Close()

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		textLog(gs.Self.NodeID, "GOSSIP", "Received invalid gossip payload: %v", err)
		http.Error(w, "Invalid gossip payload", http.StatusBadRequest)
		return
	}

	// Always mark the sender as alive since we just heard from them
	gs.updateMember(&Member{
		NodeID:    payload.NodeID,
		Host:      payload.Host,
		Port:      payload.Port,
		Heartbeat: payload.Heartbeat,
		LastSeen:  time.Now(),
		Status:    StatusAlive,
	})

	// Process remote member states
	for nodeID, data := range payload.Members {
		memberData, ok := data.(map[string]interface{})
		if !ok {
			textLog(gs.Self.NodeID, "GOSSIP", "Invalid member data format for %s", nodeID)
			continue
		}

		// Extract required fields with safety checks
		host, _ := memberData["host"].(string)
		portFloat, _ := memberData["port"].(float64)
		port := int(portFloat)
		heartbeatFloat, _ := memberData["heartbeat"].(float64)
		heartbeat := int64(heartbeatFloat)
		statusStr, _ := memberData["status"].(string)
		lastSeenFloat, _ := memberData["last_seen"].(float64)

		// Create and update member
		gs.updateMember(&Member{
			NodeID:    nodeID,
			Host:      host,
			Port:      port,
			Heartbeat: heartbeat,
			Status:    NodeStatus(statusStr),
			LastSeen:  time.Unix(0, int64(lastSeenFloat)),
		})
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// Fix updateMember function to be more reliable
func (gs *GossipService) updateMember(newMember *Member) {
	if newMember == nil || newMember.NodeID == "" {
		return // Safety check
	}

	// Skip updates for self - only we can update our own status
	if newMember.NodeID == gs.Self.NodeID {
		// But do update timestamps to prevent false suspicions
		gs.Self.LastSeen = time.Now()
		return
	}

	existing, exists := gs.Members.Load(newMember.NodeID)

	if !exists {
		textLog(gs.Self.NodeID, "GOSSIP", "Added new node %s to member list", newMember.NodeID)
		gs.Members.Store(newMember.NodeID, newMember)
		return
	}

	current := existing.(*Member)

	// Detect if node was down and is now coming back online
	if (current.Status == StatusDown || current.Status == StatusSuspected) &&
		newMember.Heartbeat > current.Heartbeat {
		textLog(gs.Self.NodeID, "GOSSIP", "Node %s has come back ONLINE, reconnecting hints",
			newMember.NodeID)

		// Trigger hint reconnection for coordinator
		if coordinator != nil {
			go coordinator.forceReconnectHints(newMember.NodeID)
		}
	}

	// Always update last seen time if we're hearing about this node
	current.LastSeen = time.Now()

	// Update status based on heartbeat and status information
	if newMember.Heartbeat > current.Heartbeat {
		textLog(gs.Self.NodeID, "GOSSIP", "Node %s heartbeat increased: %d -> %d",
			newMember.NodeID, current.Heartbeat, newMember.Heartbeat)

		current.Heartbeat = newMember.Heartbeat

		// Only update status to alive when heartbeat increases
		if newMember.Status == StatusAlive {
			if current.Status != StatusAlive {
				textLog(gs.Self.NodeID, "GOSSIP", "Node %s status changed: %s -> %s",
					newMember.NodeID, current.Status, newMember.Status)
			}
			current.Status = StatusAlive
		}

		// Always update host/port when heartbeat increases
		current.Host = newMember.Host
		current.Port = newMember.Port
	}

	// Remember that someone else thinks this node is down
	if newMember.Status == StatusDown && current.Status != StatusDown {
		textLog(gs.Self.NodeID, "GOSSIP", "Received report that node %s is DOWN", newMember.NodeID)
		current.Status = StatusSuspected // Mark as suspected instead of down immediately
	}

	gs.Members.Store(newMember.NodeID, current)
}

// // Fix updateMember function to be more reliable
// func (gs *GossipService) updateMember(newMember *Member) {
// 	if newMember == nil || newMember.NodeID == "" {
// 		return // Safety check
// 	}

// 	// Skip updates for self - only we can update our own status
// 	if newMember.NodeID == gs.Self.NodeID {
// 		// But do update timestamps to prevent false suspicions
// 		gs.Self.LastSeen = time.Now()
// 		return
// 	}

// 	existing, exists := gs.Members.Load(newMember.NodeID)

// 	if !exists {
// 		textLog(gs.Self.NodeID, "GOSSIP", "Added new node %s to member list", newMember.NodeID)
// 		gs.Members.Store(newMember.NodeID, newMember)
// 		return
// 	}

// 	current := existing.(*Member)

// 	// Always update last seen time if we're hearing about this node
// 	current.LastSeen = time.Now()

// 	// Update status based on heartbeat and status information
// 	if newMember.Heartbeat > current.Heartbeat {
// 		textLog(gs.Self.NodeID, "GOSSIP", "Node %s heartbeat increased: %d -> %d",
// 			newMember.NodeID, current.Heartbeat, newMember.Heartbeat)

// 		current.Heartbeat = newMember.Heartbeat

// 		// Only update status to alive when heartbeat increases
// 		if newMember.Status == StatusAlive {
// 			if current.Status != StatusAlive {
// 				textLog(gs.Self.NodeID, "GOSSIP", "Node %s status changed: %s -> %s",
// 					newMember.NodeID, current.Status, newMember.Status)
// 			}
// 			current.Status = StatusAlive
// 		}

// 		// Always update host/port when heartbeat increases
// 		current.Host = newMember.Host
// 		current.Port = newMember.Port
// 	}

// 	// Remember that someone else thinks this node is down
// 	if newMember.Status == StatusDown && current.Status != StatusDown {
// 		textLog(gs.Self.NodeID, "GOSSIP", "Received report that node %s is DOWN", newMember.NodeID)
// 		current.Status = StatusSuspected // Mark as suspected instead of down immediately
// 	}

// 	gs.Members.Store(newMember.NodeID, current)
// }

// Fix for checkMemberStatuses to better detect and handle failures
func (gs *GossipService) checkMemberStatuses() {
	now := time.Now()
	suspicionTimeout := 3 * time.Second // Reduced timeout for tests
	failureTimeout := 6 * time.Second   // Reduced timeout for tests

	textLog(gs.Self.NodeID, "GOSSIP", "Checking member statuses. Current node: %s", gs.Self.NodeID)

	gs.Members.Range(func(key, value interface{}) bool {
		member := value.(*Member)
		if member.NodeID == gs.Self.NodeID {
			return true
		}

		timeSinceLastSeen := now.Sub(member.LastSeen)
		oldStatus := member.Status

		switch {
		case timeSinceLastSeen > failureTimeout:
			member.Status = StatusDown
			textLog(gs.Self.NodeID, "GOSSIP", "Node %s is DOWN (last seen %v ago)",
				member.NodeID, timeSinceLastSeen)
		case timeSinceLastSeen > suspicionTimeout:
			member.Status = StatusSuspected
			textLog(gs.Self.NodeID, "GOSSIP", "Node %s is SUSPECTED down (last seen %v ago)",
				member.NodeID, timeSinceLastSeen)
		default:
			// Keep nodes alive once they're marked alive until timeout
			if member.Status != StatusAlive {
				textLog(gs.Self.NodeID, "GOSSIP", "Node %s was revived (recent activity detected)",
					member.NodeID)
			}
			member.Status = StatusAlive
			textLog(gs.Self.NodeID, "GOSSIP", "Node %s is ALIVE (last seen %v ago)",
				member.NodeID, timeSinceLastSeen)
		}

		// Make sure status changes are logged clearly
		if oldStatus != member.Status {
			textLog(gs.Self.NodeID, "GOSSIP", "STATUS CHANGE: Node %s status changed from %s to %s",
				member.NodeID, oldStatus, member.Status)
		}

		gs.Members.Store(member.NodeID, member)
		return true
	})

	// Log complete cluster state after every check
	gs.logClusterState()
}

// Fix for getNodeStatus to be more reliable
func (gs *GossipService) getNodeStatus(nodeID string) NodeStatus {
	// Special case for self
	if nodeID == gs.Self.NodeID {
		return StatusAlive
	}

	if value, exists := gs.Members.Load(nodeID); exists {
		status := value.(*Member).Status
		// Log every status check to make detection more visible
		textLog(gs.Self.NodeID, "STATUS", "Check for node %s: %s", nodeID, status)
		return status
	}

	// If unknown node, add as suspected at first
	textLog(gs.Self.NodeID, "STATUS", "WARNING: No status information for unknown node %s, adding as SUSPECTED", nodeID)
	gs.Members.Store(nodeID, &Member{
		NodeID:    nodeID,
		Host:      "localhost",
		Port:      getPortForNode(nodeID),
		Heartbeat: 0,
		Status:    StatusSuspected,
		LastSeen:  time.Now().Add(-4 * time.Second), // Make it appear as suspected
	})

	return StatusSuspected
}

// getClusterState returns information about all nodes in the cluster
func (gs *GossipService) getClusterState() map[string]interface{} {
	state := make(map[string]interface{})

	// Add self information
	state[gs.Self.NodeID] = map[string]interface{}{
		"host":      gs.Self.Host,
		"port":      gs.Self.Port,
		"status":    string(StatusAlive),
		"heartbeat": gs.Self.Heartbeat,
		"last_seen": gs.Self.LastSeen.Format(time.RFC3339),
	}

	// Add other members
	gs.Members.Range(func(key, value interface{}) bool {
		member := value.(*Member)
		state[member.NodeID] = map[string]interface{}{
			"host":      member.Host,
			"port":      member.Port,
			"status":    string(member.Status),
			"heartbeat": member.Heartbeat,
			"last_seen": member.LastSeen.Format(time.RFC3339),
		}
		return true
	})

	return state
}

// GetLiveMembers returns all members that are considered alive
func (gs *GossipService) GetLiveMembers() []*Member {
	var live []*Member
	gs.Members.Range(func(key, value interface{}) bool {
		member := value.(*Member)
		if member.Status == StatusAlive {
			live = append(live, member)
		}
		return true
	})
	return live
}

// Enhanced logClusterState for better troubleshooting
func (gs *GossipService) logClusterState() {
	textLog(gs.Self.NodeID, "CLUSTER", "======= CLUSTER STATE REPORT FROM %s =======", gs.Self.NodeID)

	textLog(gs.Self.NodeID, "CLUSTER", "Self status: %s", StatusAlive)

	gs.Members.Range(func(key, value interface{}) bool {
		member := value.(*Member)
		textLog(gs.Self.NodeID, "CLUSTER", "Node %s: Status=%s, LastSeen=%v ago, Heartbeat=%d",
			member.NodeID,
			member.Status,
			time.Since(member.LastSeen),
			member.Heartbeat)
		return true
	})

	textLog(gs.Self.NodeID, "CLUSTER", "================================================")
}

// Fix for ForceNodeDown in GossipService to be more reliable
func (gs *GossipService) ForceNodeDown(nodeID string) {
	if nodeID == gs.Self.NodeID {
		textLog(gs.Self.NodeID, "GOSSIP", "WARNING: Cannot force self node down")
		return
	}

	if value, exists := gs.Members.Load(nodeID); exists {
		member := value.(*Member)
		oldStatus := member.Status
		member.Status = StatusDown
		member.LastSeen = time.Now().Add(-15 * time.Second) // Make it appear offline for longer
		textLog(gs.Self.NodeID, "GOSSIP", "FORCED STATUS CHANGE: Node %s status changed from %s to %s",
			nodeID, oldStatus, StatusDown)
		gs.Members.Store(nodeID, member)
	} else {
		// If node isn't in member list, add it as down
		gs.Members.Store(nodeID, &Member{
			NodeID:    nodeID,
			Host:      "localhost",
			Port:      getPortForNode(nodeID),
			Heartbeat: 0,
			Status:    StatusDown,
			LastSeen:  time.Now().Add(-15 * time.Second),
		})
		textLog(gs.Self.NodeID, "GOSSIP", "Added missing node %s as DOWN", nodeID)
	}
}
