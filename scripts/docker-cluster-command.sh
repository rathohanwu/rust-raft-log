#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

action=${1:-}
value=${2:-}

case "$action" in
  add|subtract|multiply|divide) ;;
  *)
    echo "Usage: $0 <add|subtract|multiply|divide> <integer>" >&2
    exit 2
    ;;
esac

if [[ ! "$value" =~ ^-?[0-9]+$ ]]; then
  echo "Value must be an integer: $value" >&2
  exit 2
fi

payload=$(printf '{"action":"%s","value":%s}' "$action" "$value")
"${COMPOSE[@]}" run --rm --no-deps raft-client \
  --config /etc/raft/cluster.docker.yaml \
  --payload "$payload"
