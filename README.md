# DistributedSystems-DynamoDB

This repository contains a Dynamo-inspired distributed key-value store implemented in Go, along with a small admin dashboard.

Implemented features:
- consistent hashing
- replication with quorum-based reads and writes
- gossip-based membership and failure detection
- vector clocks for version tracking
- hinted handoff and sloppy quorum
- Merkle-tree-based anti-entropy using bucket diffs and targeted key repair

## Prerequisites

Install these system tools before running the project:
- Go 1.18 or newer
- bash
- curl
- lsof
- pkill

Only for the optional benchmark:
- Python 3

The project was validated on Linux.

## Repository Layout

```text
DistributedSystems-DynamoDB/
├── setup.sh                  # one-time dependency setup
├── backend/                  # distributed key-value store
│   ├── run_cluster.sh        # starts/stops the 4-node cluster
│   ├── test_dynamo.sh        # end-to-end integration test suite
│   ├── benchmark.py          # optional benchmark tool
│   └── requirements.txt      # Python packages for benchmarking
└── admin-dashboard/          # browser-based cluster dashboard
```

## One-Time Setup

From the repository root, run:

```bash
bash setup.sh
```

What `setup.sh` does:
- checks for required commands
- downloads Go module dependencies for `backend` and `admin-dashboard`
- builds both Go modules
- runs backend unit tests

Default setup does not install benchmark dependencies.

To also install the optional Python packages used by `backend/benchmark.py`, run:

```bash
bash setup.sh --with-benchmark
```

## Running the Backend Cluster

From the repository root:

```bash
cd backend
./run_cluster.sh
```

This starts 4 local nodes on:
- `nodeA` -> `localhost:5000`
- `nodeB` -> `localhost:5001`
- `nodeC` -> `localhost:5002`
- `nodeD` -> `localhost:5003`

Notes:
- `run_cluster.sh` regenerates `backend/configs/nodeA.json` through `nodeD.json` each time it starts the cluster.
- Runtime logs are written under `backend/logs/`.
- Persistent local snapshots are written under `backend/data/`.

## Running the Admin Dashboard

Open a second terminal from the repository root:

```bash
cd admin-dashboard
go run main.go
```

Then visit:

```text
http://localhost:8080
```

## Verifying the System

### Run the automated integration test suite

From the repository root:

```bash
cd backend
bash test_dynamo.sh
```

The test suite covers:
- basic PUT/GET
- vector clock behavior
- sloppy quorum during node failure
- hinted handoff
- Merkle-based anti-entropy sync
- conflict handling after partition-like scenarios

### Run Go unit tests manually

From the repository root:

```bash
cd backend
go test ./...
```

## API Usage

With the cluster running, send requests to any node. `nodeA` on port `5000` is the default coordinator in the examples below.

### Store a value

```bash
curl -X PUT http://localhost:5000/kv/mykey \
  -H 'Content-Type: application/json' \
  -d '{"value":"hello"}'
```

### Read a value

```bash
curl http://localhost:5000/kv/mykey
```

### Cluster status

```bash
curl http://localhost:5000/admin/cluster
```

### Force an anti-entropy sync

This triggers the same Merkle-tree-based anti-entropy path used by the background scheduler.

```bash
curl -X POST http://localhost:5000/admin/sync \
  -H 'Content-Type: application/json' \
  -d '{}'
```

## Optional Benchmark

The benchmark tool is optional and is not required to run the key-value store.

Install the benchmark dependencies first:

```bash
bash setup.sh --with-benchmark
```

If a virtual environment was created, activate it first:

```bash
source .venv/bin/activate
```

Then run the benchmark from the repository root:

```bash
cd backend
python3 benchmark.py --type mixed --operations 1000 --workers 8 --read-pct 80
```

## Stopping the System

From the repository root:

```bash
cd backend
./run_cluster.sh stop
```

## Troubleshooting

- If ports `5000`-`5003` are already in use, stop the existing processes before starting the cluster.
- If the dashboard does not load, make sure the backend cluster is already running.
- If benchmark commands fail, run `bash setup.sh --with-benchmark` to install or recreate the optional Python environment.
