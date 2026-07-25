#!/usr/bin/env bash

source "$(dirname "${BASH_SOURCE[0]}")/docker-cluster-common.sh"

# Recreate or start all nodes using their existing named data volumes. The
# first startup must use docker-cluster-up.sh so the image is built.
"${COMPOSE[@]}" up -d --wait node1 node2 node3
"${COMPOSE[@]}" ps
