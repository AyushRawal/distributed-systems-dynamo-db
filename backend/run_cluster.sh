#!/usr/bin/env bash
# run_cluster.sh — start / stop the local Dynamo‑style KV‑store cluster
# =====================================================================
set -u          # abort on unset vars
IFS=$'\n\t'

# ───────────────────────────  ANSI colours  ──────────────────────────
ESC=$'\033'                     # real ESC char
RESET="${ESC}[0m"
BOLD="${ESC}[1m"
FG_RED="${ESC}[31m"
FG_GRN="${ESC}[32m"
FG_YLW="${ESC}[33m"
FG_CYN="${ESC}[36m"
FG_MAG="${ESC}[35m"

info()    { printf "${FG_CYN}ℹ %s${RESET}\n" "$*"; }
success() { printf "${FG_GRN}✓ %s${RESET}\n" "$*"; }
warn()    { printf "${FG_YLW}⚠ %s${RESET}\n" "$*"; }
error()   { printf "${FG_RED}✗ %s${RESET}\n" "$*"; }
step()    { printf "\n${BOLD}%s${RESET}\n" "$*"; }

# ────────────────  curl wrapper (hides libcurl banner)  ──────────────
curlx() {
  curl -sS "$@" \
       2> >(grep -v "no version information available (required by curl)" >&2)
}

# ────────────────────────────  constants  ────────────────────────────
NODES=(nodeA nodeB nodeC nodeD)
PORTS=(5000 5001 5002 5003)
GRPC_PORTS=(6000 6001 6002 6003)
LOG_DIR="logs"
CFG_DIR="configs"
mkdir -p "$LOG_DIR" "$CFG_DIR"

# ──────────────────────────  stop_cluster  ───────────────────────────
stop_cluster() {
  step "Stopping any running nodes…"
  for port in "${PORTS[@]}" "${GRPC_PORTS[@]}"; do
    if pid=$(lsof -t -i:"$port" 2>/dev/null); then
      info "Stopping process on port $port (PID $pid)"
      kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 2
  for port in "${PORTS[@]}" "${GRPC_PORTS[@]}"; do
    if pid=$(lsof -t -i:"$port" 2>/dev/null); then
      warn "Force‑killing stubborn process on port $port (PID $pid)"
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  pkill -f "go run .*" 2>/dev/null || true
  success "All nodes stopped."
}

# ─────────────────────────  generate_configs  ────────────────────────
generate_configs() {
  step "Generating fresh config files…"
  for i in "${!NODES[@]}"; do
    node=${NODES[$i]}
    port=${PORTS[$i]}
    grpc_port=${GRPC_PORTS[$i]}
    peers=()
    for j in "${!NODES[@]}"; do
      [[ $i == $j ]] && continue
      peers+=( "{\"node_id\":\"${NODES[$j]}\",\"host\":\"localhost\",\"port\":${PORTS[$j]},\"grpc_port\":${GRPC_PORTS[$j]}}" )
    done
    peers_json=$(IFS=,; echo "${peers[*]}")

    cat > "$CFG_DIR/$node.json" <<EOF
{
  "node_id": "$node",
  "host": "localhost",
  "port": $port,
  "grpc_port": $grpc_port,
  "peers": [ $peers_json ],
  "replication_factor": 3,
  "read_quorum": 2,
  "write_quorum": 2,
  "gossip_interval_ms": 1000,
  "failure_check_interval_ms": 2000,
  "gossip_timeout_ms": 1000,
  "anti_entropy_interval_ms": 30000,
  "gossip_fanout": 2,
  "gossip_retries": 3,
  "gossip_retry_backoff_ms": 500,
  "failure_timeout_ms": 10000,
  "suspicion_timeout_ms": 5000
}
EOF
    success "Config for $node → $CFG_DIR/$node.json"
  done
}

# ──────────────────────────  start_cluster  ──────────────────────────
start_cluster() {
  step "Starting DynamoDB cluster…"
  stop_cluster
  generate_configs

  for i in "${!NODES[@]}"; do
    node=${NODES[$i]}
    port=${PORTS[$i]}
    cfg="$CFG_DIR/$node.json"

    info "Launching $node on :$port"
    # Build a dedicated binary to avoid 'go run' including *_test.go files.
    mkdir -p bin
    go build -o bin/node . > /dev/null 2>&1 || {
      error "go build failed; see build output"
      # capture build output to the node log for easier debugging
      go build -o bin/node . >"$LOG_DIR/${node}.log" 2>&1 || true
    }
    ./bin/node -config="$cfg" >"$LOG_DIR/$node.log" 2>&1 &
    pid=$!
    echo "$pid" >"$LOG_DIR/$node.pid"
    sleep 2
    if ps -p "$pid" &>/dev/null; then
      success "$node running (PID $pid)"
    else
      error "$node failed to start — see ${LOG_DIR}/${node}.log"
    fi
  done

  # probe admin endpoint
  step "Verifying cluster responsiveness…"
  for attempt in {1..10}; do
    if curlx "http://localhost:${PORTS[0]}/admin/cluster" >/dev/null; then
      success "Cluster is up!"
      show_usage
      return
    fi
    info "Waiting for cluster… (attempt $attempt/10)"
    sleep 2
  done

  warn "Cluster did not respond in time. Logs follow:"
  for node in "${NODES[@]}"; do
    tail -n 20 "$LOG_DIR/$node.log" | sed "s/^/[${node}] /"
  done
}

# ────────────────────────────  banner  ───────────────────────────────
show_usage() {
  local BAR="───────────────────────────────────────────────────────────────"
  printf "${BOLD}%s${RESET}\n" "$BAR"
  printf "Cluster running with %d nodes.\n\n" "${#NODES[@]}"

  printf "%-10s %s\n" "${FG_MAG}PUT${RESET}"    "curl -X PUT http://localhost:5000/kv/mykey \\"
  printf "%-10s %s\n" ""                        "     -d '{\"value\":\"hello\"}' -H 'Content-Type: application/json'"
  printf "%-10s %s\n" "${FG_MAG}GET${RESET}"    "curl http://localhost:5000/kv/mykey"
  printf "%-10s %s\n" "${FG_MAG}STATUS${RESET}" "curl http://localhost:5000/admin/cluster"
  printf "%-10s %s\n" "${FG_MAG}STOP${RESET}"   "./run_cluster.sh stop"

  printf "${BOLD}%s${RESET}\n" "$BAR"
}

# ──────────────────────────────  main  ───────────────────────────────
case "${1:-start}" in
  stop) stop_cluster ;;
  *)    start_cluster ;;
esac
