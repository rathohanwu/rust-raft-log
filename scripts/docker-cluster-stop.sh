#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

# Stop processes without removing their containers or named data volumes.
"${COMPOSE[@]}" stop node1 node2 node3
"${COMPOSE[@]}" ps
