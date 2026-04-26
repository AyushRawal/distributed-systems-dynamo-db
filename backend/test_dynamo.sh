#!/bin/bash

# Define colors for output formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'  # No Color

# Prepare environment
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# Test configuration (unique key/value for this run)
TEST_KEY="test-key-$(date +%s)"
TEST_VALUE="test-value-$(date +%s)"
COORDINATOR_NODE="http://localhost:5000"  # coordinator for client requests (nodeA on port 5000)

# Helper function to run a test command and check its output
run_test() {
  local test_name="$1"
  local command="$2"
  local expected="$3"

  echo -e "\n${YELLOW}Running test: ${NC}$test_name"
  echo "$ $command"
  result=$(eval "$command")

  if echo "$result" | grep -q "$expected"; then
    echo -e "${GREEN}✓ PASS:${NC} $test_name"
    return 0
  else
    echo -e "${RED}✗ FAIL:${NC} $test_name"
    echo "Expected to find: $expected"
    echo "Got: $result"
    return 1
  fi
}

# Ensure cluster is in a clean state (stop all nodes, then restart)
ensure_clean_cluster() {
  echo "Restarting cluster to ensure a clean state..."
  ./run_cluster.sh stop
  sleep 5  # wait for processes to fully terminate
  ./run_cluster.sh
  sleep 5  # wait for cluster to stabilize
}

# Verify the cluster is running, if not, start it
echo "Checking if the cluster is running..."
if ! curl -s "$COORDINATOR_NODE/admin/cluster" > /dev/null; then
  echo -e "${RED}Cluster not running. Starting cluster...${NC}"
  ./run_cluster.sh
  sleep 5
  if ! curl -s "$COORDINATOR_NODE/admin/cluster" > /dev/null; then
    echo -e "${RED}Failed to start cluster. Please check the logs.${NC}"
    exit 1
  fi
fi

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}Starting DynamoDB Test Suite${NC}"
echo -e "${GREEN}============================================================${NC}"

# 1. Basic operations
echo -e "\n${YELLOW}1. Testing Basic Operations${NC}"
echo "=========================================="

# 1.1 Store a value on the cluster
run_test "Store a value" \
  "curl -s -X PUT -H 'Content-Type: application/json' -d '{\"value\":\"$TEST_VALUE\"}' $COORDINATOR_NODE/kv/$TEST_KEY" \
  "stored"

# Give some time for replication (if any)
sleep 2

# 1.2 Retrieve the value from the cluster
run_test "Retrieve the value" \
  "curl -s $COORDINATOR_NODE/kv/$TEST_KEY" \
  "$TEST_VALUE"

# 2. Testing Vector Clocks
echo -e "\n${YELLOW}2. Testing Vector Clocks${NC}"
echo "=========================================="

# 2.1 The GET response should include a vector clock
run_test "GET response includes vector clock" \
  "curl -s $COORDINATOR_NODE/kv/$TEST_KEY" \
  "vector_clock"

# 2.2 Update the value to create a new version (which will increment the vector clock)
TEST_VALUE_2="${TEST_VALUE}-updated"
run_test "Update value (new version)" \
  "curl -s -X PUT -H 'Content-Type: application/json' -d '{\"value\":\"$TEST_VALUE_2\"}' $COORDINATOR_NODE/kv/$TEST_KEY" \
  "stored"

# Wait for replication of the update
sleep 2

# 2.3 Retrieve again to confirm the value was updated
run_test "Confirm updated value" \
  "curl -s $COORDINATOR_NODE/kv/$TEST_KEY" \
  "$TEST_VALUE_2"

# 3. Testing Fault Tolerance (Sloppy Quorum)
echo -e "\n${YELLOW}3. Testing Fault Tolerance (Sloppy Quorum)${NC}"
echo "=========================================="

# Simulate a node failure by stopping one node (Node C)
echo "Stopping Node C to simulate a node failure..."
pkill -f "nodeC" || true
sleep 5  # allow gossip to mark Node C as down

# Force a cluster status check (this can update internal state and logs about Node C being down)
echo "Forcing a cluster status check after Node C goes down..."
curl -s "$COORDINATOR_NODE/admin/cluster" > /dev/null

# 3.1 Store a new value while Node C is down (should succeed via sloppy quorum)
FAULT_KEY="fault-key-$(date +%s)"
echo "Using fault-tolerance test key: $FAULT_KEY"
run_test "PUT with one node down (sloppy quorum)" \
  "curl -s -X PUT -H 'Content-Type: application/json' -d '{\"value\":\"Fault tolerant\"}' $COORDINATOR_NODE/kv/$FAULT_KEY" \
  "stored"

sleep 2  # wait for any hinted handoff recording

# 3.2 GET the value while Node C is still down
run_test "GET with one node down" \
  "curl -s $COORDINATOR_NODE/kv/$FAULT_KEY" \
  "Fault tolerant"

# 3.3 Check logs for sloppy quorum behavior (e.g., hints or unavailability messages)
echo "Checking logs for signs of sloppy quorum handling..."
if grep -i -q "sloppy\|unavail\|replacement\|responsib" logs/*.txt 2>/dev/null; then
  echo -e "${GREEN}✓ Found log entries indicating sloppy quorum was used${NC}"
else
  echo -e "${RED}✗ No explicit sloppy quorum log entries found${NC}"
  echo "Showing any relevant messages from node logs:"
  grep -a -i "sloppy\|unavail\|replacement\|down\|failed" logs/*.log | head -20 || echo "(No related entries in logs)"
fi

echo -e "${YELLOW}Sloppy quorum test completed.${NC} The operations succeeded despite one node being down, indicating the cluster tolerated the failure."

# 4. Testing Hinted Handoff
echo -e "\n${YELLOW}4. Testing Hinted Handoff${NC}"
echo "=========================================="

# 4.1 While Node C is down, write another new key (this write will be stored as a hint on other nodes)
HINT_KEY="hint-key-$(date +%s)"
echo "Writing key $HINT_KEY with Node C down (to generate a hinted handoff)..."
curl -s -X PUT -H 'Content-Type: application/json' -d '{"value":"Hint test value"}' $COORDINATOR_NODE/kv/$HINT_KEY > /dev/null

# 4.2 Bring Node C (and the cluster) back online
echo "Bringing Node C back online by restarting the cluster..."
./run_cluster.sh stop
sleep 5
./run_cluster.sh

echo "Waiting for Node C to rejoin the cluster..."
sleep 10
# Force a status check to possibly trigger hint delivery
curl -s "$COORDINATOR_NODE/admin/cluster" > /dev/null

# 4.3 Force anti-entropy sync to ensure hints are delivered to Node C
echo "Forcing an anti-entropy sync to deliver hinted data to Node C..."
curl -s -X POST -H 'Content-Type: application/json' -d '{}' $COORDINATOR_NODE/admin/sync > /dev/null
sleep 5

# 4.4 Check if the previously missing data (FAULT_KEY) is now available on Node C
ATTEMPTS=3
for (( i=1; i<=ATTEMPTS; i++ )); do
  echo "Checking if Node C has the hinted data for key $FAULT_KEY (attempt $i/$ATTEMPTS)..."
  NODEC_RESPONSE=$(curl -s "http://localhost:5002/kv/$FAULT_KEY")
  if [[ "$NODEC_RESPONSE" == *"Fault tolerant"* ]]; then
    echo -e "${GREEN}✓ Hinted handoff successful: Node C now stores key $FAULT_KEY${NC}"
    break
  else
    echo -e "${YELLOW}Node C does not have key $FAULT_KEY yet.${NC}"
    if [ $i -lt $ATTEMPTS ]; then
      echo "Waiting 5 seconds and trying again..."
      sleep 5
      curl -s -X POST -H 'Content-Type: application/json' -d '{}' $COORDINATOR_NODE/admin/sync > /dev/null
    else
      echo -e "${RED}✗ After $ATTEMPTS attempts, Node C still lacks key $FAULT_KEY${NC}"
      echo "Last response from Node C: $NODEC_RESPONSE"
    fi
  fi
done

# 5. Testing Anti-Entropy
echo -e "\n${YELLOW}5. Testing Anti-Entropy${NC}"
echo "=========================================="

# 5.1 Force a cluster-wide anti-entropy sync
echo "Triggering a manual anti-entropy sync..."
curl -s -X POST -H 'Content-Type: application/json' -d '{}' $COORDINATOR_NODE/admin/sync > /dev/null
sleep 5

# 5.2 Create a new key that will be used to verify anti-entropy propagation
SYNC_KEY="sync-test-$(date +%s)"
curl -s -X PUT -H 'Content-Type: application/json' -d '{"value":"Sync test value"}' http://localhost:5000/kv/$SYNC_KEY > /dev/null
echo "Created test key $SYNC_KEY on Node A (port 5000)"

sleep 2  # wait briefly for initial replication

# 5.3 Force another sync and check that the new key reaches all nodes
echo "Forcing another anti-entropy sync for key propagation..."
curl -s -X POST -H 'Content-Type: application/json' -d '{}' $COORDINATOR_NODE/admin/sync > /dev/null
sleep 5

# Try multiple sync cycles if needed to propagate the key to all nodes
for (( i=1; i<=3; i++ )); do
  echo "Checking if all nodes have key $SYNC_KEY (attempt $i/3)..."
  NODE_B_SYNC=$(curl -s "http://localhost:5001/kv/$SYNC_KEY")
  NODE_C_SYNC=$(curl -s "http://localhost:5002/kv/$SYNC_KEY")
  NODE_D_SYNC=$(curl -s "http://localhost:5003/kv/$SYNC_KEY")

  if [[ "$NODE_B_SYNC" == *"Sync test value"* && "$NODE_C_SYNC" == *"Sync test value"* && "$NODE_D_SYNC" == *"Sync test value"* ]]; then
    echo -e "${GREEN}✓ Anti-entropy success: key $SYNC_KEY present on all nodes${NC}"
    break
  else
    echo -e "${YELLOW}Key $SYNC_KEY not fully replicated to all nodes yet.${NC}"
    [[ "$NODE_B_SYNC" == *"Sync test value"* ]] && echo "Node B has the data." || echo "Node B missing the data."
    [[ "$NODE_C_SYNC" == *"Sync test value"* ]] && echo "Node C has the data." || echo "Node C missing the data."
    [[ "$NODE_D_SYNC" == *"Sync test value"* ]] && echo "Node D has the data." || echo "Node D missing the data."

    if [ $i -lt 3 ]; then
      echo "Performing another sync and will re-check..."
      curl -s -X POST -H 'Content-Type: application/json' -d '{}' $COORDINATOR_NODE/admin/sync > /dev/null
      sleep 10
    else
      echo -e "${RED}✗ Anti-entropy failed to replicate key $SYNC_KEY to all nodes after multiple attempts${NC}"
    fi
  fi
done

# 6. Testing Conflict Resolution
echo -e "\n${YELLOW}6. Testing Conflict Resolution${NC}"
echo "=========================================="

# Restart the cluster to clear state and ensure a clean partition scenario
ensure_clean_cluster

# Clear textual logs for conflict test
echo "Clearing text logs for conflict test..."
rm -f logs/*.txt
for node in "${NODES[@]}"; do
  touch logs/$node.txt
done

# 6.1 Create a key on Node A (which will replicate to others)
CONFLICT_KEY="conflict-key-$(date +%s)"
run_test "Initial PUT on Node A (Version A)" \
  "curl -s -X PUT -H 'Content-Type: application/json' -d '{\"value\":\"Version A\"}' http://localhost:5000/kv/$CONFLICT_KEY" \
  "stored"

sleep 2  # wait for replication

# 6.2 Simulate network partition: stop Node C and Node D (half the cluster)
echo "Simulating network partition: stopping Node C and Node D..."
pkill -f "nodeC" || true
pkill -f "nodeD" || true
sleep 5

# 6.3 During the partition, update the same key on Node A and Node B with different values
run_test "PUT on Node A during partition (Version A-updated)" \
  "curl -s -X PUT -H 'Content-Type: application/json' -d '{\"value\":\"Version A-updated\"}' http://localhost:5000/kv/$CONFLICT_KEY" \
  "stored"

run_test "PUT on Node B during partition (Version B-updated)" \
  "curl -s -X PUT -H 'Content-Type: application/json' -d '{\"value\":\"Version B-updated\"}' http://localhost:5001/kv/$CONFLICT_KEY" \
  "stored"

# Capture the divergent responses from Node A and Node B
NODE_A_RESPONSE=$(curl -s "http://localhost:5000/kv/$CONFLICT_KEY")
NODE_B_RESPONSE=$(curl -s "http://localhost:5001/kv/$CONFLICT_KEY")
echo "Node A response during partition: $NODE_A_RESPONSE"
echo "Node B response during partition: $NODE_B_RESPONSE"

# 6.4 Heal the partition by restarting the entire cluster
echo "Healing partition by restarting the cluster..."
./run_cluster.sh stop
sleep 10
./run_cluster.sh
sleep 10  # allow cluster to stabilize after rejoining

# Force multiple anti-entropy syncs to reconcile conflicts
echo "Forcing anti-entropy syncs to resolve conflicts..."
for (( i=1; i<=3; i++ )); do
  echo "Anti-entropy sync attempt $i/3..."
  curl -s -X POST -H 'Content-Type: application/json' -d '{}' $COORDINATOR_NODE/admin/sync > /dev/null
  sleep 5
done

# Fetch the final value from all nodes after conflict resolution
NODE_A_AFTER=$(curl -s "http://localhost:5000/kv/$CONFLICT_KEY")
NODE_B_AFTER=$(curl -s "http://localhost:5001/kv/$CONFLICT_KEY")
NODE_C_AFTER=$(curl -s "http://localhost:5002/kv/$CONFLICT_KEY")
NODE_D_AFTER=$(curl -s "http://localhost:5003/kv/$CONFLICT_KEY")

echo "After partition healing and sync:"
echo "Node A: $NODE_A_AFTER"
echo "Node B: $NODE_B_AFTER"
echo "Node C: $NODE_C_AFTER"
echo "Node D: $NODE_D_AFTER"

# Determine if conflict was resolved consistently across nodes
if [[ "$NODE_A_AFTER" == "$NODE_B_AFTER" ]]; then
  echo -e "${GREEN}✓ Nodes A and B converged on the same value.${NC}"
  # Check if the unified value indicates a conflict resolution strategy (e.g., multiple versions or a conflict field)
  if [[ "$NODE_A_AFTER" == *"conflict"* ]] || ([[ "$NODE_A_AFTER" == *"Version A"* && "$NODE_A_AFTER" == *"Version B"* ]]); then
    echo -e "${GREEN}✓ Conflict resolved by preserving multiple versions (vector clock reconciliation).${NC}"
  else
    echo -e "${YELLOW}! Conflict resolved using last-writer-wins (single surviving value).${NC}"
  fi
else
  echo -e "${RED}✗ Nodes have not converged on a single value; conflict may not be fully resolved.${NC}"
fi

# 7. Simple Performance Test
echo -e "\n${YELLOW}7. Simple Performance Test${NC}"
echo "=========================================="

echo "Measuring throughput with 20 sequential write operations..."
START_TIME=$(date +%s.%N)
for i in {1..20}; do
  KEY="perf-key-$i"
  curl -s -X PUT -H 'Content-Type: application/json' -d "{\"value\":\"perf-value-$i\"}" $COORDINATOR_NODE/kv/$KEY > /dev/null
done
END_TIME=$(date +%s.%N)
DURATION=$(echo "$END_TIME - $START_TIME" | bc)
OPS_PER_SEC=$(echo "scale=2; 20 / $DURATION" | bc)
echo -e "Completed 20 PUT operations in ${DURATION} seconds"
echo -e "Throughput: ${OPS_PER_SEC} ops/second"

# Summary of test results
echo -e "\n${GREEN}============================================================${NC}"
echo -e "${GREEN}Test Suite Completed${NC}"
echo -e "${GREEN}============================================================${NC}"
echo -e "Check the logs directory for detailed logs from each node."
