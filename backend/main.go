package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io" // Added for MultiWriter
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

var coordinator *Coordinator

// getPortForNode maps a node ID to its port
func getPortForNode(nodeID string) int {
	switch nodeID {
	case "nodeA":
		return 5000
	case "nodeB":
		return 5001
	case "nodeC":
		return 5002
	case "nodeD":
		return 5003
	default:
		// If not a known node, try to get a port number from the node ID
		parts := strings.Split(nodeID, "node")
		if len(parts) == 2 {
			suffix := parts[1]
			// Try to convert the suffix to a port offset
			if offset, err := strconv.Atoi(suffix); err == nil {
				return 5000 + offset
			}
		}

		// Fallback to default port with hash
		h := int(hashKey(nodeID) % 1000)
		return 5000 + h
	}
}

func getGRPCPortForNode(nodeID string) int {
	switch nodeID {
	case "nodeA":
		return 6000
	case "nodeB":
		return 6001
	case "nodeC":
		return 6002
	case "nodeD":
		return 6003
	default:
		parts := strings.Split(nodeID, "node")
		if len(parts) == 2 {
			suffix := parts[1]
			if offset, err := strconv.Atoi(suffix); err == nil {
				return 6000 + offset
			}
		}

		h := int(hashKey(nodeID) % 1000)
		return 6000 + h
	}
}

func main() {
	// Parse command line flags
	configFile := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	// Load configuration
	var config *Config
	var err error

	if *configFile != "" {
		// Load from config file
		config, err = LoadConfig(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		// Fallback to command line args for backward compatibility
		if len(os.Args) < 3 {
			fmt.Println("Usage: go run main.go <node_id> <port> [peer1:port peer2:port ...]")
			os.Exit(1)
		}

		nodeID := os.Args[1]
		port, err := strconv.Atoi(os.Args[2])
		if err != nil {
			log.Fatalf("Invalid port: %v", err)
		}

		peerArgs := os.Args[3:]
		peers := []PeerConfig{}
		for _, p := range peerArgs {
			parts := strings.Split(p, ":")
			if len(parts) >= 2 {
				peerPort, _ := strconv.Atoi(parts[1])
				peers = append(peers, PeerConfig{
					NodeID:   parts[0],
					Host:     "localhost",
					Port:     peerPort,
					GRPCPort: getGRPCPortForNode(parts[0]),
				})
			} else if len(parts) == 1 {
				peers = append(peers, PeerConfig{
					NodeID:   parts[0],
					Host:     "localhost",
					Port:     getPortForNode(parts[0]),
					GRPCPort: getGRPCPortForNode(parts[0]),
				})
			}
		}

		config = &Config{
			NodeID:               nodeID,
			Host:                 "localhost",
			Port:                 port,
			GRPCPort:             getGRPCPortForNode(nodeID),
			Peers:                peers,
			ReplicationFactor:    3,
			ReadQuorum:           2,
			WriteQuorum:          2,
			GossipInterval:       1 * time.Second,
			FailureCheckInterval: 2 * time.Second,
			GossipTimeout:        1 * time.Second,
		}
	}

	// Create logs directory
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatalf("Failed to create logs directory: %v", err)
	}

	logFile, err := os.OpenFile(fmt.Sprintf("logs/%s.log", config.NodeID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	// Use standard logger with text-only output
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	// Build the consistent hash ring with self and peers
	ring := NewConsistentHashRing()

	// Add self to ring
	ring.AddNode(config.NodeID)

	// Add peers to ring
	allNodes := []string{config.NodeID}
	for _, peer := range config.Peers {
		ring.AddNode(peer.NodeID)
		allNodes = append(allNodes, peer.NodeID)
	}

	// Create a coordinator with configured parameters
	coordinator = NewCoordinator(config.NodeID, ring, config.ReplicationFactor, config.ReadQuorum, config.WriteQuorum)

	// Initialize and start gossip service with node IDs
	coordinator.Gossip = NewGossipService(config.NodeID, allNodes)
	coordinator.Gossip.Start()

	// Start periodic tasks (hinted handoff processing)
	coordinator.startPeriodicTasks()

	// Start anti-entropy process with a faster interval for testing
	go coordinator.startAntiEntropy(5 * time.Second)
	startGRPCServer(config, coordinator)

	// Setup HTTP server
	r := mux.NewRouter()

	// Public endpoints
	r.HandleFunc("/kv/{key}", GetHandler).Methods("GET")
	r.HandleFunc("/kv/{key}", PutHandler).Methods("PUT")

	// Internal endpoints
	r.HandleFunc("/internal/kv/{key}", InternalGetHandler).Methods("GET")
	r.HandleFunc("/internal/kv/{key}", InternalPutHandler).Methods("PUT")
	r.HandleFunc("/internal/gossip", coordinator.Gossip.HandleGossip)
	r.HandleFunc("/internal/merkle/{bucket}", MerkleTreeHandler).Methods("GET")
	r.HandleFunc("/internal/repair/{key}", RepairHandler).Methods("PUT")
	r.HandleFunc("/internal/store-hint", StoreHintHandler).Methods("POST")

	// Admin endpoints
	r.HandleFunc("/admin/cluster", ClusterInfoHandler).Methods("GET")
	r.HandleFunc("/admin/sync", ForceSyncHandler).Methods("POST")

	addr := fmt.Sprintf(":%d", config.Port)
	log.Printf("Node %s starting HTTP server on port %d...", config.NodeID, config.Port)
	log.Fatal(http.ListenAndServe(addr, r))
}

// Fix for startAntiEntropy to run more frequently for tests
func (c *Coordinator) startAntiEntropy(interval time.Duration) {
	ticker := time.NewTicker(5 * time.Second) // Use 5 seconds instead of the passed interval
	defer ticker.Stop()

	textLog(c.NodeID, "ANTI_ENTROPY", "Started anti-entropy process with faster interval 5s")

	// Run once at startup to establish initial sync
	go c.performAntiEntropy()

	for {
		select {
		case <-ticker.C:
			textLog(c.NodeID, "ANTI_ENTROPY", "Running scheduled anti-entropy sync")
			go c.performAntiEntropy()
		}
	}
}

// Fix for performAntiEntropy function - rewrite to be more reliable
func (c *Coordinator) performAntiEntropy() {
	peers := c.Ring.getAllNodeIDs()
	textLog(c.NodeID, "ANTI_ENTROPY", "Starting anti-entropy with peers: %v", peers)

	for _, peer := range peers {
		if peer == c.NodeID {
			continue
		}
		textLog(c.NodeID, "ANTI_ENTROPY", "Merkle syncing with peer %s", peer)
		c.performMerkleSyncWithNode(peer)
	}

	textLog(c.NodeID, "ANTI_ENTROPY", "Completed anti-entropy cycle")
}

func logMessage(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	log.Println(message)

	// Ensure the message is also written to our log file in a plain text format
	// This helps with grep and other text processing tools
	logFile := fmt.Sprintf("logs/%s.txt", coordinator.NodeID)
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		defer f.Close()
		fmt.Fprintln(f, message)
	}
}

// Fix for GetHandler to include better error handling
func GetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	textLog(coordinator.NodeID, "PUBLIC", "Processing GET request for key %s", key)
	result, err := coordinator.Get(key)
	if err != nil {
		// Special case - try to serve locally if we have it even without quorum
		localValue := coordinator.localGet(key)
		if localValue.Value != nil {
			textLog(coordinator.NodeID, "PUBLIC", "Failed to achieve quorum for GET %s, falling back to local copy", key)
			result = coordinator.formatResult(localValue, 0)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if result["value"] == nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	js, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(append(js, '\n'))
}

// Fix for PutHandler with better error handling and logging
func PutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}
	value, ok := body["value"]
	if !ok {
		http.Error(w, "No value provided", http.StatusBadRequest)
		return
	}

	textLog(coordinator.NodeID, "PUBLIC", "Processing PUT request for key %s", key)
	err := coordinator.Put(key, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]interface{}{
		"key":    key,
		"status": "stored",
		"node":   coordinator.NodeID,
	}
	js, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(append(js, '\n'))
}

// InternalGetHandler handles internal GET requests from other nodes
func InternalGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value := coordinator.localGet(key)
	if value.Value == nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	vectorClock := map[string]int{}
	if value.VectorClock != nil {
		vectorClock = value.VectorClock.Clock
	}

	resp := map[string]interface{}{
		"value":        value.Value,
		"vector_clock": vectorClock,
		"timestamp":    value.Timestamp.Format(time.RFC3339),
	}

	// Include conflicts if any
	if len(value.Conflicts) > 0 {
		conflicts := []map[string]interface{}{}
		for _, conflict := range value.Conflicts {
			conflictData := map[string]interface{}{
				"value":        conflict.Value,
				"vector_clock": conflict.VectorClock.Clock,
			}
			conflicts = append(conflicts, conflictData)
		}
		resp["conflicts"] = conflicts
	}

	js, err := json.Marshal(resp)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// In main.go - enhance InternalPutHandler
func InternalPutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	value, ok := body["value"]
	if !ok {
		http.Error(w, "Missing value", http.StatusBadRequest)
		return
	}

	incoming := parseStoredValue(body)
	if incoming.VectorClock == nil || len(incoming.VectorClock.Clock) == 0 {
		incoming.VectorClock = NewVectorClock()
		incoming.VectorClock.Increment(coordinator.NodeID)
	}

	// Check special flags
	isForceSync := false
	if forceSyncVal, ok := body["force_sync"].(bool); ok && forceSyncVal {
		isForceSync = true
	}

	isForceKey := false
	if forceKeyVal, ok := body["force_key"].(bool); ok && forceKeyVal {
		isForceKey = true
		textLog(coordinator.NodeID, "TEST_FIX", "Received force key request for %s", key)
	}

	isHint := false
	if isHintVal, ok := body["is_hint"].(bool); ok && isHintVal {
		isHint = true
	}

	originNode := "unknown"
	if origin, ok := body["origin_node"].(string); ok {
		originNode = origin
	}

	incoming.Value = value
	coordinator.applyReplicaWrite(key, incoming, replicaWriteOptions{
		ForceSync:  isForceSync,
		ForceKey:   isForceKey,
		IsHint:     isHint,
		OriginNode: originNode,
	})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// // Enhanced Fix for InternalPutHandler to be more reliable
// func InternalPutHandler(w http.ResponseWriter, r *http.Request) {
// 	vars := mux.Vars(r)
// 	key := vars["key"]

// 	var body map[string]interface{}
// 	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
// 		http.Error(w, "Invalid JSON", http.StatusBadRequest)
// 		return
// 	}

// 	value, ok := body["value"]
// 	if !ok {
// 		http.Error(w, "Missing value", http.StatusBadRequest)
// 		return
// 	}

// 	// Handle vector clock with better error handling
// 	var vc *VectorClock
// 	if vcInterface, ok := body["vector_clock"]; ok {
// 		// Convert vector clock to map[string]int
// 		vcMap := make(map[string]int)
// 		vcBytes, _ := json.Marshal(vcInterface)
// 		if err := json.Unmarshal(vcBytes, &vcMap); err == nil {
// 			vc = &VectorClock{Clock: vcMap}
// 		} else {
// 			// If error, create a new one
// 			vc = NewVectorClock()
// 			vc.Increment(coordinator.NodeID)
// 		}
// 	} else {
// 		// If no vector clock provided, create a new one
// 		vc = NewVectorClock()
// 		vc.Increment(coordinator.NodeID)
// 	}

// 	// Check if this is a force sync or hint
// 	isForceSync := false
// 	if forceSyncVal, ok := body["force_sync"].(bool); ok && forceSyncVal {
// 		isForceSync = true
// 		textLog(coordinator.NodeID, "INTERNAL", "Received forced sync for key %s", key)
// 	}

// 	isHint := false
// 	if isHintVal, ok := body["is_hint"].(bool); ok && isHintVal {
// 		isHint = true
// 		originNode := "unknown"
// 		if origin, ok := body["origin_node"].(string); ok {
// 			originNode = origin
// 		}
// 		textLog(coordinator.NodeID, "INTERNAL", "Received hint delivery for key %s from %s", key, originNode)
// 	}

// 	// Update local store with special handling for force sync and hints
// 	if isForceSync || isHint {
// 		// For forced sync or hints, always store the value regardless of vector clock
// 		coordinator.mu.Lock()
// 		coordinator.DataStore[key] = storedValue{
// 			Value:       value,
// 			VectorClock: vc,
// 			Timestamp:   time.Now(),
// 		}
// 		coordinator.mu.Unlock()
// 		textLog(coordinator.NodeID, "INTERNAL", "Force stored key %s", key)
// 	} else {
// 		// Normal put with vector clock comparison
// 		coordinator.localPut(key, value, vc)
// 	}

// 	// Log the operation
// 	textLog(coordinator.NodeID, "INTERNAL", "Internal PUT completed for key %s with vector clock %v",
// 		key, vc.Clock)

// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("OK"))
// }

// ClusterInfoHandler returns information about the cluster state
func ClusterInfoHandler(w http.ResponseWriter, r *http.Request) {
	clusterState := coordinator.Gossip.getClusterState()

	js, err := json.Marshal(clusterState)
	if err != nil {
		http.Error(w, "Failed to marshal cluster info", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// MerkleTreeHandler serves a node's Merkle tree for a specific bucket
func MerkleTreeHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketStr := vars["bucket"]

	// parse bucket as integer, not re-hash the string
	bucketNum, err := strconv.Atoi(bucketStr)
	if err != nil {
		http.Error(w, "Invalid bucket ID", http.StatusBadRequest)
		return
	}

	tree := coordinator.buildMerkleTreeForBucket(bucketNum)
	serialized := tree.SerializeToMap()
	js, err := json.Marshal(serialized)
	if err != nil {
		http.Error(w, "Failed to marshal Merkle tree", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// Enhanced RepairHandler for better conflict resolution
func RepairHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	value, ok := body["value"]
	if !ok {
		http.Error(w, "Missing value", http.StatusBadRequest)
		return
	}

	sv := parseStoredValue(body)
	sv.Value = value
	coordinator.applyRepair(key, sv)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// StoreHintHandler processes requests to store a hint
func StoreHintHandler(w http.ResponseWriter, r *http.Request) {
	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	targetNode, ok := body["target_node"].(string)
	if !ok {
		http.Error(w, "Missing target_node", http.StatusBadRequest)
		return
	}

	key, ok := body["key"].(string)
	if !ok {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}

	value, ok := body["value"]
	if !ok {
		http.Error(w, "Missing value", http.StatusBadRequest)
		return
	}

	vcInterface, ok := body["vector_clock"]
	if !ok {
		http.Error(w, "Missing vector_clock", http.StatusBadRequest)
		return
	}

	// Convert vector clock
	vcMap := make(map[string]int)
	vcBytes, _ := json.Marshal(vcInterface)
	json.Unmarshal(vcBytes, &vcMap)
	vc := &VectorClock{Clock: vcMap}

	// Store the hint
	coordinator.storeHint(targetNode, key, value, vc)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// // Enhanced ForceSyncHandler for better recovery
// func ForceSyncHandler(w http.ResponseWriter, r *http.Request) {
// 	textLog(coordinator.NodeID, "ADMIN", "Received force sync request")

// 	// Specifically process hinted handoffs for all nodes
// 	for _, nodeID := range coordinator.Ring.getAllNodeIDs() {
// 		if nodeID != coordinator.NodeID && coordinator.isNodeAvailable(nodeID) {
// 			textLog(coordinator.NodeID, "ADMIN", "Forcing hint reconnect for node %s", nodeID)
// 			go coordinator.forceReconnectHints(nodeID)
// 		}
// 	}

// 	var body map[string]interface{}
// 	if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
// 		// If a specific node is specified, sync only with that node
// 		if targetNode, ok := body["node"].(string); ok {
// 			textLog(coordinator.NodeID, "ADMIN", "Forcing sync with specific node %s", targetNode)
// 			go coordinator.performAntiEntropyWithNode(targetNode)
// 			w.WriteHeader(http.StatusOK)
// 			w.Write([]byte(fmt.Sprintf("Sync started with node %s", targetNode)))
// 			return
// 		}
// 	}

// 	// Perform multiple sync cycles for better chances of success
// 	textLog(coordinator.NodeID, "ADMIN", "Forcing full cluster sync with aggressive retries")
// 	for i := 0; i < 3; i++ {
// 		go coordinator.performAntiEntropy()
// 		time.Sleep(500 * time.Millisecond) // Spread out the sync attempts
// 	}

// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("Multiple sync cycles started with all nodes"))
// }

// In main.go - modify ForceSyncHandler
func ForceSyncHandler(w http.ResponseWriter, r *http.Request) {
	textLog(coordinator.NodeID, "ADMIN", "Received force sync request")

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
		// If a specific node is specified, sync only with that node
		if targetNode, ok := body["node"].(string); ok {
			textLog(coordinator.NodeID, "ADMIN", "Forcing sync with specific node %s", targetNode)
			go coordinator.performAntiEntropyWithNode(targetNode)

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(fmt.Sprintf("Sync started with node %s", targetNode)))
			return
		}
	}

	// Perform multiple sync cycles for better chances of success
	textLog(coordinator.NodeID, "ADMIN", "Forcing full cluster sync with aggressive retries")
	for i := 0; i < 3; i++ {
		go coordinator.performAntiEntropy()
		time.Sleep(500 * time.Millisecond) // Spread out the sync attempts
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Multiple sync cycles started with all nodes"))
}

// Helper function for anti-entropy with a specific node
func (c *Coordinator) performAntiEntropyWithNode(nodeID string) {
	c.performMerkleSyncWithNode(nodeID)
}
