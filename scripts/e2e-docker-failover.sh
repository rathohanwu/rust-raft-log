#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT="raft-e2e-failover-${RUN_ID:-$$}"
ARTIFACT_DIR="$ROOT_DIR/artifacts/$PROJECT"
COMPOSE=(docker compose -p "$PROJECT" -f "$ROOT_DIR/docker/compose.e2e.yaml")

# Preserve a per-run Buildx configuration with the diagnostics.
export BUILDX_CONFIG=${BUILDX_CONFIG:-"$ARTIFACT_DIR/buildx"}

cleanup() {
  local status=$?

  mkdir -p "$ARTIFACT_DIR"
  "${COMPOSE[@]}" logs --no-color > "$ARTIFACT_DIR/compose.log" || true
  "${COMPOSE[@]}" down -v --remove-orphans || true
  exit "$status"
}

query_state() {
  "${COMPOSE[@]}" run --rm --no-deps raft-state \
    --config /etc/raft/cluster.docker.yaml \
    --node-id "$1"
}

all_nodes_have_value() {
  local expected=$1
  local node_id
  local output

  for node_id in 1 2 3; do
    output=$(query_state "$node_id")
    printf '%s\n' "$output"
    printf '%s' "$output" | grep -q "\"value\":$expected"
    printf '%s' "$output" | grep -q "\"applied_commands\":$expected"
  done
}

trap cleanup EXIT
mkdir -p "$BUILDX_CONFIG"

echo "Building arithmetic-state Docker test node..."
"${COMPOSE[@]}" build node1

echo "Starting Raft nodes..."
"${COMPOSE[@]}" up -d --wait node1 node2 node3

echo "Writing add(1) and verifying every node reports value=1..."
first_write=$("${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload '{"action":"add","value":1}' 2>&1)
printf '%s\n' "$first_write"
leader=$(printf '%s\n' "$first_write" | sed -n 's/.*Leader: Node \([0-9][0-9]*\).*/\1/p' | tail -1)
test -n "$leader"
all_nodes_have_value 1

echo "Stopping leader node$leader and writing add(1) through the new leader..."
"${COMPOSE[@]}" stop "node$leader"
second_write=$("${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload '{"action":"add","value":1}' \
  --max-retries 15 \
  --retry-delay-ms 500 2>&1)
printf '%s\n' "$second_write"
new_leader=$(printf '%s\n' "$second_write" | sed -n 's/.*Leader: Node \([0-9][0-9]*\).*/\1/p' | tail -1)
test -n "$new_leader"
test "$leader" != "$new_leader"

echo "Restarting node$leader and writing add(1) to drive catch-up..."
"${COMPOSE[@]}" start "node$leader"
"${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload '{"action":"add","value":1}' \
  --max-retries 15 \
  --retry-delay-ms 500

echo "Waiting for all three state machines to converge on value=3..."
deadline=$((SECONDS + 15))
until all_nodes_have_value 3; do
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for arithmetic state convergence" >&2
    exit 1
  fi
  sleep 1
done

echo "Docker failover and state-machine verification passed (project: $PROJECT)."
