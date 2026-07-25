#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

require_node_id "${1:-}"
"${COMPOSE[@]}" stop "node$1"
"${COMPOSE[@]}" ps "node$1"
