// node_persist.go  – simple on‑disk snapshotting for DataStore
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

const dataDir = "data" // stored on local disk next to logs/

// --------------------------------------------------------------------
// helper returns  e.g.  data/nodeA.json
func (n *Node) dataFile() string {
	return filepath.Join(dataDir, n.NodeID+".json")
}

// --------------------------------------------------------------------
// loadData()  — call once at process start
func (n *Node) loadData() {
	_ = os.MkdirAll(dataDir, 0755)

	f, err := os.Open(n.dataFile())
	if err != nil { // first run: file doesn’t exist
		return
	}
	defer f.Close()

	tmp := map[string]storedValue{}
	if err := json.NewDecoder(f).Decode(&tmp); err == nil {
		n.DataStore = tmp
	}
}

// --------------------------------------------------------------------
// saveData()  — call after every mutation
func (n *Node) saveData() {
	_ = os.MkdirAll(dataDir, 0755)

	tmpFile := n.dataFile() + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return // don’t crash because of disk errors
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(n.DataStore); err == nil {
		f.Close()
		_ = os.Rename(tmpFile, n.dataFile()) // atomic replace
	} else {
		f.Close()
		_ = os.Remove(tmpFile)
	}
}
