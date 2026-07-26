#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

usage() {
  echo "Usage: $0 [-l lines] [node-id: 1|2|3|4|5]" >&2
  exit 2
}

lines=
while getopts ":l:" option; do
  case "$option" in
    l)
      lines=$OPTARG
      [[ "$lines" =~ ^[1-9][0-9]*$ ]] || usage
      ;;
    *) usage ;;
  esac
done
shift $((OPTIND - 1))

[[ $# -le 1 ]] || usage

node_ids=(1 2 3 4 5)
if [[ $# -eq 1 ]]; then
  require_node_id "$1"
  node_ids=("$1")
fi

# Override with, for example, SINCE=1h to inspect a longer interval.
since=${SINCE:-10m}

for node_id in "${node_ids[@]}"; do
  echo "== node ${node_id}: applied arithmetic commands since ${since} =="
  command=("${COMPOSE[@]}" logs --timestamps --since "$since" "node${node_id}")
  if [[ -n "$lines" ]]; then
    "${command[@]}" | grep 'Applied arithmetic' | tail -n "$lines" || true
  else
    "${command[@]}" | grep 'Applied arithmetic' || true
  fi
done
