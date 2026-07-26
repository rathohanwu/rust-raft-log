#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

case "${1:-}" in
  "")
    mkdir -p "$BUILDX_CONFIG"
    echo "Building the Docker test-node image..."
    "${COMPOSE[@]}" build node1
    ;;
  --skip-build)
    echo "Reusing the existing Docker test-node image..."
    ;;
  *)
    echo "Usage: $0 [--skip-build]" >&2
    exit 2
    ;;
esac

echo "Starting nodes 1 through 5 in project $PROJECT..."
"${COMPOSE[@]}" up -d --wait node1 node2 node3 node4 node5
"${COMPOSE[@]}" ps

cat <<EOF

Cluster is ready. Try:
  scripts/docker-cluster-command.sh add 1
  scripts/docker-cluster-apply-log.sh
  scripts/docker-node-stop.sh 1
  scripts/docker-node-start.sh 1
  scripts/docker-cluster-stop.sh
  scripts/docker-cluster-start.sh
  scripts/docker-cluster-logs.sh
  scripts/docker-cluster-down.sh                 # preserve data
  scripts/docker-cluster-down.sh --delete-data   # fresh cluster
EOF
