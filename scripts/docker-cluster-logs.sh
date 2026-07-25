#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

"${COMPOSE[@]}" logs -f node1 node2 node3
