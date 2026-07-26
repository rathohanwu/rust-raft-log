#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

case "${1:-}" in
  "")
    "$ROOT_DIR/scripts/docker-cluster-down.sh" --delete-data
    "$ROOT_DIR/scripts/docker-cluster-up.sh"
    ;;
  --skip-build)
    "$ROOT_DIR/scripts/docker-cluster-down.sh" --delete-data
    "$ROOT_DIR/scripts/docker-cluster-up.sh" --skip-build
    ;;
  *)
    echo "Usage: $0 [--skip-build]" >&2
    exit 2
    ;;
esac
