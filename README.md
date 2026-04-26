# DistributedSystems-DynamoDB

A simplified implementation inspired by Amazon's [Dynamo](https://www.amazon.com), built in Go. This project demonstrates core principles of distributed systems:

* **Consistent hashing** for even key distribution
* **Data replication** with configurable read/write quorum (R/W)
* **Sloppy quorum** & **hinted handoff** for high availability
* **Vector clocks** for conflict detection and resolution
* **Merkle trees** and **anti-entropy** for replica synchronization
* **Gossip protocol** for decentralized membership and failure detection

---

## ✨ Features

* ⚡ **High availability**: serves reads/writes even when nodes fail
* 📈 **Scalability**: add or remove nodes without full rehash
* ⟳ **Conflict resolution**: causal ordering via vector clocks
* 🩺 **Self‑healing**: hinted handoff + periodic anti‑entropy
* 📊 **Monitoring**: built‑in stats & admin HTTP endpoints

---

## 🧱 Architecture Overview

```
Client ↔ Coordinator ↔ Consistent Hash Ring ↔ N Replicas
 ├─ Gossip Service (membership)
 ├─ Anti‑Entropy (Merkle trees)
 └─ Hint Store (sloppy writes)
```

---

## ⚙️ Prerequisites

* Go 1.18+
* bash, curl
* Python 3+ (for benchmarking)
* pip (for Python dependencies)

---

## 🚀 Getting Started

### Local Build & Run

Navigate to the backend directory:

```bash
cd distributed-dynamo-system/backend
```

Install Go dependencies and build the DynamoDB executable:

```bash
go mod tidy
go build -o dynamoDB .
```

### Configure Nodes

Edit `configs/*.json` files as needed (replication factor, quorum, peers).

### Start All Nodes

Start each node in a separate shell:

```bash
./dynamoDB -config configs/nodeA.json &
./dynamoDB -config configs/nodeB.json &
./dynamoDB -config configs/nodeC.json &
./dynamoDB -config configs/nodeD.json &
```

Or use the helper script:

```bash
./run_cluster.sh
```

---

## 🖥️ Running the Admin Dashboard UI

In a new terminal, navigate to the admin dashboard directory:

```bash
cd distributed-dynamo-system/admin-dashboard
```

Install Go dependencies and run the UI:

```bash
go mod tidy
go run main.go
```

Then open [http://localhost:8080](http://localhost:8080) in your browser.

---

## 🧺 Usage Examples

```bash
# Write data
echo '{"value":"hello"}' | curl -X PUT http://localhost:5000/kv/mykey -H 'Content-Type: application/json'

# Read data
curl http://localhost:5000/kv/mykey

# Delete data
curl -X DELETE http://localhost:5000/kv/mykey

# View node statistics
curl http://localhost:5000/stats
```

---

## ✅ Testing & Validation

From the `distributed-dynamo-system/backend` directory:

```bash
chmod +x test_dynamo.sh
./test_dynamo.sh
```

---

## 📊 Benchmarking

From the `distributed-dynamo-system/backend` directory:

```bash
pip install -r requirements.txt
python3 benchmark.py --type mixed --operations 1000 --workers 8 --read-pct 80 --plot --output bench_report
```

---

## 🗂️ Project Structure

```
distributed-dynamo-system/
├── backend/                  # Core distributed key-value store
│   ├── configs/              # Node configurations (nodeA.json, nodeB.json, etc.)
│   ├── data/                 # Persistence storage (optional)
│   ├── logs/                 # Runtime logs
│   ├── benchmark.py          # Performance analyzer
│   ├── check_cluster.sh      # Cluster health check script
│   ├── config.go             # Configuration parsing
│   ├── consistent_hash.go    # Consistent hashing implementation
│   ├── gossip.go             # Gossip protocol
│   ├── main.go               # Node entry point
│   ├── merkle_tree.go        # Merkle tree implementation (anti-entropy)
│   ├── node.go               # Core node operations (coordinator logic)
│   ├── node_persist.go       # Disk persistence
│   ├── run_cluster.sh        # Cluster management script
│   ├── stats.go              # Metrics collection
│   ├── test_dynamo.sh        # Functional test suite
│   ├── vector_clock.go       # Vector clock implementation
│   ├── go.mod                # Dependency management
│   └── go.sum                # Dependency checksums
└── admin-dashboard/          # Cluster monitoring UI
    ├── templates/            # HTML templates
    │   └── index.gohtml      # Main dashboard template
    ├── main.go               # UI server entry point
    ├── go.mod                # UI dependencies
    └── go.sum                # UI dependency checksums
```

---

## 📡 Monitoring Endpoints

Access these while nodes are running (replace `<node-port>` with actual port):

* `http://localhost:<node-port>/stats`
  → Node metrics (requests, latency, errors)

* `http://localhost:<node-port>/ring`
  → Hash ring status (nodes, partitions)

* `http://localhost:<node-port>/gossip`
  → Membership information (live/dead nodes)

* `http://localhost:<node-port>/merkle`
  → Merkle tree status (anti-entropy progress)

---

## 📬 Contact

Feel free to contribute or raise issues if you find bugs or improvements!
Maintainer: *\[Nikhil Singh]*
License: MIT
