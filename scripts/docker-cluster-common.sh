#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT=${PROJECT:-raft-lab}
ARTIFACT_DIR="$ROOT_DIR/artifacts/$PROJECT"
COMPOSE=(docker compose -p "$PROJECT" -f "$ROOT_DIR/docker/compose.e2e.yaml")

# Avoid stale Docker Desktop Buildx state when using Colima. The directory is
# retained with the local run artifacts and is ignored by Git.
export BUILDX_CONFIG=${BUILDX_CONFIG:-"$ARTIFACT_DIR/buildx"}

require_node_id() {
  local node_id=${1:-}
  case "$node_id" in
    1|2|3|4|5) ;;
    *)
      echo "Usage: $0 <node-id: 1|2|3|4|5>" >&2
      exit 2
      ;;
  esac
}
