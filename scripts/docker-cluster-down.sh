#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

case "${1:-}" in
  "")
    # `docker compose down` removes containers and the network, but retains
    # named volumes. A later cluster-up therefore tests log/state recovery.
    "${COMPOSE[@]}" down --remove-orphans
    echo "Cluster removed; data volumes were preserved."
    ;;
  --delete-data)
    "${COMPOSE[@]}" down -v --remove-orphans
    echo "Cluster and data volumes were removed."
    ;;
  *)
    echo "Usage: $0 [--delete-data]" >&2
    exit 2
    ;;
esac
