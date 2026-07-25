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

unavailable=0

for node_id in "${node_ids[@]}"; do
  if output=$("${COMPOSE[@]}" run --rm --no-deps raft-state \
    --config /etc/raft/cluster.docker.yaml \
    --node-id "$node_id" 2>&1); then
    printf '%s\n' "$output"
  else
    unavailable=1
    echo "node $node_id is unavailable; continuing with the remaining nodes" >&2
    printf '%s\n' "$output" >&2
  fi
done

# A partial result is useful interactively, but callers can still detect that
# one or more nodes were unavailable from the exit status.
exit "$unavailable"
