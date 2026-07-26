#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

payload=${1:-}

if [[ -z "$payload" ]]; then
  echo "Usage: $0 <payload>" >&2
  exit 2
fi

"${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload "$payload"
