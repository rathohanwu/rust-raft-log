#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT="raft-e2e-recovery-${RUN_ID:-$$}"
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

all_nodes_recovered() {
  local node_id
  local output

  for node_id in 1 2 3; do
    output=$(query_state "$node_id")
    printf '%s\n' "$output"
    printf '%s' "$output" | grep -q '"value":9'
    printf '%s' "$output" | grep -q '"applied_commands":1'
    # A new leader commits its current-term NoOp at index 3. That commit
    # drives replay of the recovered command at index 2 from volatile index 0.
    printf '%s' "$output" | grep -q '"last_applied":3'
  done
}

trap cleanup EXIT
mkdir -p "$BUILDX_CONFIG"

echo "Building arithmetic-state Docker test node..."
"${COMPOSE[@]}" build node1

echo "Starting Raft nodes and committing add(9)..."
"${COMPOSE[@]}" up -d --wait node1 node2 node3
"${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload '{"action":"add","value":9}'

echo "Stopping every node without removing its volumes..."
"${COMPOSE[@]}" stop node1 node2 node3

echo "Starting every node from the same volumes..."
"${COMPOSE[@]}" up -d --wait node1 node2 node3

echo "Waiting for the elected leader's NoOp to replay the recovered prefix..."
deadline=$((SECONDS + 20))
until all_nodes_recovered; do
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for full-cluster state-machine recovery" >&2
    exit 1
  fi
  sleep 1
done

echo "Docker full-restart recovery verification passed (project: $PROJECT)."
