#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT="raft-e2e-${RUN_ID:-$$}"
ARTIFACT_DIR="$ROOT_DIR/artifacts/$PROJECT"
PAYLOAD=${PAYLOAD:-docker-e2e-smoke}
KEEP=${KEEP:-0}
COMPOSE=(docker compose -p "$PROJECT" -f "$ROOT_DIR/docker/compose.e2e.yaml")

# Keep Buildx state with this run's diagnostics. This also avoids inheriting an
# unusable Docker Desktop Buildx directory when a machine has moved to Colima.
export BUILDX_CONFIG=${BUILDX_CONFIG:-"$ARTIFACT_DIR/buildx"}

cleanup() {
  local status=$?

  mkdir -p "$ARTIFACT_DIR"
  "${COMPOSE[@]}" logs --no-color > "$ARTIFACT_DIR/compose.log" || true

  if [[ "$KEEP" == "1" ]]; then
    echo "Keeping Docker project $PROJECT for inspection."
  else
    "${COMPOSE[@]}" down -v --remove-orphans || true
  fi

  exit "$status"
}

trap cleanup EXIT

mkdir -p "$BUILDX_CONFIG"

echo "Building Docker image for project $PROJECT..."
"${COMPOSE[@]}" build

echo "Starting Raft nodes..."
"${COMPOSE[@]}" up -d --wait node1 node2 node3

echo "Submitting Docker smoke-test write..."
"${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload "$PAYLOAD"

echo "Docker E2E smoke test passed (project: $PROJECT)."
