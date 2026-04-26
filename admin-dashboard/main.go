package main

import (
	"bufio" // Added for reading initial lines
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hpcloud/tail"
)

// Data structure to pass to the HTML template
type PageData struct {
	Nodes  []NodeStatus
	Error  string
	Action string
}

type NodeStatus struct {
	ID        string
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Status    string `json:"status"`
	Heartbeat int64  `json:"heartbeat"`
	IsAlive   bool
}

var tpl *template.Template

// Define dynamic test keys at package level, initialized once
var (
	faultKey    string
	hintKey     string
	conflictKey string
)

func init() {
	// Parse the HTML template on startup
	tpl = template.Must(template.ParseFiles("templates/index.gohtml"))

	// Initialize dynamic test keys with a timestamp from app start
	now := time.Now().Unix()
	faultKey = fmt.Sprintf("fault-key-%d", now)
	hintKey = fmt.Sprintf("hint-key-%d", now+1)         // Slightly different to avoid collision if precise
	conflictKey = fmt.Sprintf("conflict-key-%d", now+2) // Slightly different
	log.Printf("Initialized dynamic test keys: Fault='%s', Hint='%s', Conflict='%s'", faultKey, hintKey, conflictKey)
}

// getClusterStatus fetches and processes status from your dynamoDB application
func getClusterStatus() PageData {
	var pageData PageData
	// Try port 5000 first, then 5001 as a fallback for cluster info
	resp, err := http.Get("http://localhost:5000/admin/cluster")
	if err != nil {
		resp, err = http.Get("http://localhost:5001/admin/cluster") // Fallback
		if err != nil {
			pageData.Error = "Cluster is unreachable. Please ensure nodes are running and accessible on ports 5000/5001."
			return pageData
		}
	}
	defer resp.Body.Close()

	var clusterInfo map[string]struct {
		Host      string `json:"host"`
		Port      int    `json:"port"`
		Status    string `json:"status"`
		Heartbeat int64  `json:"heartbeat"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&clusterInfo); err != nil {
		pageData.Error = fmt.Sprintf("Failed to parse cluster JSON: %v", err)
		return pageData
	}

	for id, status := range clusterInfo {
		pageData.Nodes = append(pageData.Nodes, NodeStatus{
			ID:        id,
			Host:      status.Host,
			Port:      status.Port,
			Status:    status.Status,
			Heartbeat: status.Heartbeat,
			IsAlive:   status.Status == "alive",
		})
	}
	return pageData
}

// mainHandler serves the main dashboard page
func mainHandler(w http.ResponseWriter, r *http.Request) {
	pageData := getClusterStatus()
	err := tpl.Execute(w, pageData)
	if err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
	}
}

// controlHandler handles form submissions for actions
func controlHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	r.ParseForm()
	action := r.FormValue("action")
	nodeID := r.FormValue("node_id")
	testKey := r.FormValue("test_key") // For specific test keys (used by GET)

	log.Printf("Control action received: '%s' for node: '%s', key: '%s'", action, nodeID, testKey)
	var cmd *exec.Cmd
	projectDir := "../backend/" // Relative path to your project folder

	switch action {
	case "stop-node":
		cmd = exec.Command("pkill", "-f", nodeID)
	case "force-sync":
		cmd = exec.Command("curl", "-s", "-X", "POST", "http://localhost:5000/admin/sync")
	case "restart-all":
		cmd = exec.Command(filepath.Join(projectDir, "run_cluster.sh"))
		cmd.Dir = projectDir
	case "put-value": // This is the auto-generated PUT
		key := fmt.Sprintf("test-key-%d", time.Now().Unix())
		value := fmt.Sprintf("test-value-%d", time.Now().Unix())
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"%s\"}", value), "http://localhost:5000/kv/"+key)
		log.Printf("Putting auto-generated key: %s, value: %s", key, value)

	case "put-custom-value": // This handles user-defined PUT
		customKey := r.FormValue("custom_key")
		customValue := r.FormValue("custom_value")
		if customKey == "" || customValue == "" {
			http.Error(w, "Custom PUT requires both a key and a value.", http.StatusBadRequest)
			return
		}
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"%s\"}", customValue), "http://localhost:5000/kv/"+customKey)
		log.Printf("Putting custom key: %s, value: %s", customKey, customValue)

	case "get-value": // This handles GET for any provided key
		if testKey == "" {
			log.Println("GET action requires a 'test_key' parameter. Cannot perform GET.")
			http.Error(w, "GET action requires a key", http.StatusBadRequest)
			return
		}
		cmd = exec.Command("curl", "-s", "http://localhost:5000/kv/"+testKey)
		log.Printf("Getting key: %s", testKey)

	case "stop-nodeC-nodeD":
		go func() {
			log.Println("Executing: pkill -f \"nodeC\" || true")
			exec.Command("pkill", "-f", "nodeC").Run()
			log.Println("Executing: pkill -f \"nodeD\" || true")
			exec.Command("pkill", "-f", "nodeD").Run()
			log.Println("Nodes C & D stop commands sent. Waiting 5s for gossip to update.")
			time.Sleep(5 * time.Second)
		}()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Stopping Node C & D. Check logs for status updates."))
		return

	case "put-fault-key-nodeA":
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"Fault tolerant\"}"), "http://localhost:5000/kv/"+faultKey)
		log.Printf("Putting fault key: %s on nodeA", faultKey)
	case "get-fault-key-nodeA":
		cmd = exec.Command("curl", "-s", "http://localhost:5000/kv/"+faultKey)
		log.Printf("Getting fault key: %s from nodeA", faultKey)
	case "put-hint-key-nodeA":
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"Hint test value\"}"), "http://localhost:5000/kv/"+hintKey)
		log.Printf("Putting hint key: %s on nodeA", hintKey)
	case "get-fault-key-nodeC":
		cmd = exec.Command("curl", "-s", "http://localhost:5002/kv/"+faultKey)
		log.Printf("Getting fault key: %s from nodeC", faultKey)
	case "force-replicate-fault-key-nodeC":
		cmd = exec.Command("curl", "-s", "-X", "POST", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"node\":\"nodeC\", \"key\":\"%s\"}", faultKey), "http://localhost:5000/admin/sync")
		log.Printf("Attempting to force replicate %s to nodeC via admin/sync", faultKey)
	case "initial-put-conflict-key-nodeA":
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"Version A\"}"), "http://localhost:5000/kv/"+conflictKey)
		log.Printf("Initial PUT for conflict key: %s on nodeA", conflictKey)
	case "put-conflict-key-nodeA":
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"Version A-updated\"}"), "http://localhost:5000/kv/"+conflictKey)
		log.Printf("PUT for conflict key: %s on nodeA (A-updated)", conflictKey)
	case "put-conflict-key-nodeB":
		cmd = exec.Command("curl", "-s", "-X", "PUT", "-H", "Content-Type: application/json",
			"-d", fmt.Sprintf("{\"value\":\"Version B-updated\"}"), "http://localhost:5001/kv/"+conflictKey)
		log.Printf("PUT for conflict key: %s on nodeB (B-updated)", conflictKey)
	case "get-conflict-key-all-nodes":
		log.Printf("Fetching conflict key '%s' from all nodes for verification...", conflictKey)
		go func() {
			nodes := []struct {
				ID   string
				Port int
			}{
				{"nodeA", 5000}, {"nodeB", 5001}, {"nodeC", 5002}, {"nodeD", 5003},
			}
			for _, n := range nodes {
				resp, err := http.Get(fmt.Sprintf("http://localhost:%d/kv/%s", n.Port, conflictKey))
				if err != nil {
					log.Printf("Error fetching %s from %s: %v", conflictKey, n.ID, err)
					continue
				}
				defer resp.Body.Close()
				var bodyContent map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&bodyContent); err != nil {
					log.Printf("Response for %s from %s: Status=%s, Body (non-JSON)=%s", conflictKey, n.ID, resp.Status, "Could not decode JSON")
				} else {
					bodyBytes, _ := json.Marshal(bodyContent)
					log.Printf("Response for %s from %s: Status=%s, Body=%s", conflictKey, n.ID, resp.Status, string(bodyBytes))
				}
			}
		}()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Fetching results from all nodes. Check logs."))
		return
	default:
		log.Println("Unknown action:", action)
		http.Error(w, "Unknown action", http.StatusBadRequest)
		return
	}

	if cmd != nil {
		if action == "restart-all" {
			err := cmd.Start()
			if err != nil {
				log.Printf("Command execution error: %v", err)
				http.Error(w, fmt.Sprintf("Failed to execute command: %v", err), http.StatusInternalServerError)
				return
			}
			log.Printf("Started command: %s", cmd.String())
			time.Sleep(1 * time.Second)
		} else {
			output, err := cmd.CombinedOutput()
			if err != nil {
				log.Printf("Command execution error: %v, Output: %s", err, string(output))
				if !(action == "stop-node" || action == "stop-nodeC-nodeD") {
					http.Error(w, fmt.Sprintf("Command failed: %v, Output: %s", err, string(output)), http.StatusInternalServerError)
				}
				return
			}
			log.Printf("Command output: %s", string(output))
		}
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// testKeysHandler returns the dynamically generated test keys
func testKeysHandler(w http.ResponseWriter, r *http.Request) {
	keys := map[string]string{
		"faultKey":    faultKey,
		"hintKey":     hintKey,
		"conflictKey": conflictKey,
	}
	js, err := json.Marshal(keys)
	if err != nil {
		http.Error(w, "Failed to marshal test keys", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// WebSocket upgrader
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// logsHandler streams log files
func logsHandler(w http.ResponseWriter, r *http.Request) {
	nodeID := r.URL.Query().Get("node")
	if nodeID == "" {
		http.Error(w, "Missing 'node' parameter", http.StatusBadRequest)
		return
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	logFile := filepath.Join("../backend/logs/", nodeID+".txt")

	// --- FIX START ---
	// Read initial lines using standard Go file I/O
	f, err := os.Open(logFile)
	if err != nil {
		log.Printf("Error opening log file %s for initial read: %v", logFile, err)
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: Could not open log file %s. Make sure node is running and logs are being written.", logFile)))
		return
	}
	defer f.Close() // Ensure the file is closed

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(scanner.Text())); err != nil {
			log.Printf("Error sending initial lines to websocket: %v", err)
			return // Client disconnected during initial send
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("Error reading initial lines from log file: %v", err)
	}
	// --- FIX END ---

	// Then, start tailing the file for new lines
	t, err := tail.TailFile(logFile, tail.Config{Follow: true, ReOpen: true, MustExist: true, Poll: true})
	if err != nil {
		log.Printf("Error tailing log file %s: %v", logFile, err)
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: Could not tail new log lines from file %s.", logFile)))
		return
	}
	defer t.Cleanup() // Ensure tail stops when done

	for line := range t.Lines {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(line.Text)); err != nil {
			log.Printf("Error writing to websocket (broken pipe likely, client disconnected): %v", err)
			break // Client disconnected or connection broke
		}
	}
	log.Printf("Log streaming for %s stopped.", nodeID)
}

func main() {
	http.HandleFunc("/", mainHandler)
	http.HandleFunc("/control", controlHandler)
	http.HandleFunc("/ws/logs", logsHandler)
	http.HandleFunc("/admin/test-keys", testKeysHandler) // Register the new handler

	log.Println("Starting Go-only UI on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
