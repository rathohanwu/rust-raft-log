#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [node-id: 1|2|3]" >&2
  exit 2
fi

node_ids=(1 2 3)
if [[ $# -eq 1 ]]; then
  require_node_id "$1"
  node_ids=("$1")
fi

for node_id in "${node_ids[@]}"; do
  "${COMPOSE[@]}" run --rm --no-deps raft-state \
    --config /etc/raft/cluster.docker.yaml \
    --node-id "$node_id"
done
