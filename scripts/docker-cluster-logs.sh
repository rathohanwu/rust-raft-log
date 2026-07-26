#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

case "${1:-}" in
  "")
    "${COMPOSE[@]}" logs -f node1 node2 node3 node4 node5
    ;;
  --no-follow)
    "${COMPOSE[@]}" logs node1 node2 node3 node4 node5
    ;;
  *)
    echo "Usage: $0 [--no-follow]" >&2
    exit 2
    ;;
esac
