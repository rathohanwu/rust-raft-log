#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROJECT=${PROJECT:-"raft-e2e-5node-${RUN_ID:-$$}"}
ARTIFACT_DIR="$ROOT_DIR/artifacts/$PROJECT"
KEEP=${KEEP:-0}
export PROJECT
export BUILDX_CONFIG=${BUILDX_CONFIG:-"$ARTIFACT_DIR/buildx"}
export SINCE=${SINCE:-30m}

source "$ROOT_DIR/scripts/docker-cluster-common.sh"

current_scenario=
first_scenario=1

capture_logs() {
  local name=$1
  mkdir -p "$ARTIFACT_DIR"
  "${COMPOSE[@]}" logs --no-color > "$ARTIFACT_DIR/$name.log" || true
}

cleanup() {
  local status=$?

  if [[ -n "$current_scenario" ]]; then
    capture_logs "$current_scenario"
    if [[ "$KEEP" == "1" ]]; then
      echo "Keeping failed scenario project $PROJECT for inspection."
    else
      "$ROOT_DIR/scripts/docker-cluster-down.sh" --delete-data || true
    fi
  fi

  exit "$status"
}
trap cleanup EXIT

write() {
  local output
  output=$("$ROOT_DIR/scripts/docker-cluster-command.sh" "$@" 2>&1)
  printf '%s\n' "$output"
  leader=$(printf '%s\n' "$output" |
    sed -n 's/.*Leader: Node \([0-9][0-9]*\).*/\1/p' | tail -1)
  [[ -n "$leader" ]]
}

pick_excluding() {
  local node candidate skip
  for node in 1 2 3 4 5; do
    skip=
    for candidate in "$@"; do
      [[ "$node" == "$candidate" ]] && skip=1
    done
    [[ -z "$skip" ]] && { printf '%s\n' "$node"; return 0; }
  done
  return 1
}

start_scenario() {
  current_scenario=$1
  echo "=== $current_scenario ==="
  if [[ "$first_scenario" == "1" ]]; then
    "$ROOT_DIR/scripts/docker-cluster-reset.sh"
    first_scenario=0
  else
    "$ROOT_DIR/scripts/docker-cluster-reset.sh" --skip-build
  fi
}

assert_final_value() {
  local expected=$1 attempt node output actual all_match

  for attempt in {1..10}; do
    all_match=1
    for node in 1 2 3 4 5; do
      output=$("$ROOT_DIR/scripts/docker-cluster-apply-log.sh" "$node")
      actual=$(printf '%s\n' "$output" |
        sed -n 's/.*state_value=\(-\{0,1\}[0-9][0-9]*\).*/\1/p' |
        tail -1)
      [[ "$actual" == "$expected" ]] || { all_match=0; break; }
    done
    [[ "$all_match" == "1" ]] && {
      echo "ASSERT final value $expected on all five nodes: PASS"
      return 0
    }
    sleep 1
  done

  echo "ASSERT final value $expected on all five nodes: FAIL" >&2
  return 1
}

finish_scenario() {
  "$ROOT_DIR/scripts/docker-cluster-apply-log.sh"
  capture_logs "$current_scenario"
  "$ROOT_DIR/scripts/docker-cluster-down.sh" --delete-data
  current_scenario=
}

start_scenario s1
write add 7
assert_final_value 7
finish_scenario

start_scenario s2
write add 3
write multiply 5
write subtract 7
write multiply 3
write divide 2
write add 100
assert_final_value 112
finish_scenario

start_scenario s3
write add 10
follower=$(pick_excluding "$leader")
"$ROOT_DIR/scripts/docker-node-stop.sh" "$follower"
write multiply 4
write subtract 15
"$ROOT_DIR/scripts/docker-node-start.sh" "$follower"
write add 5
assert_final_value 30
finish_scenario

start_scenario s4
write add 3
old_leader=$leader
"$ROOT_DIR/scripts/docker-node-stop.sh" "$old_leader"
write add 4
[[ "$leader" != "$old_leader" ]]
"$ROOT_DIR/scripts/docker-node-start.sh" "$old_leader"
write multiply 6
assert_final_value 42
finish_scenario

start_scenario s5
write add 9
old_leader=$leader
follower=$(pick_excluding "$old_leader")
"$ROOT_DIR/scripts/docker-node-stop.sh" "$old_leader"
"$ROOT_DIR/scripts/docker-node-stop.sh" "$follower"
write subtract 4
write multiply 8
"$ROOT_DIR/scripts/docker-node-start.sh" "$old_leader"
"$ROOT_DIR/scripts/docker-node-start.sh" "$follower"
write add 2
assert_final_value 42
finish_scenario

start_scenario s6
write add 6
old_leader=$leader
follower_one=$(pick_excluding "$old_leader")
follower_two=$(pick_excluding "$old_leader" "$follower_one")
"$ROOT_DIR/scripts/docker-node-stop.sh" "$old_leader"
"$ROOT_DIR/scripts/docker-node-stop.sh" "$follower_one"
"$ROOT_DIR/scripts/docker-node-stop.sh" "$follower_two"
if rejected_output=$("$ROOT_DIR/scripts/docker-cluster-command.sh" add 7777 2>&1); then
  printf '%s\n' "$rejected_output"
  echo "S6 rejected write was acknowledged" >&2
  exit 1
fi
printf '%s\n' "$rejected_output"
"$ROOT_DIR/scripts/docker-node-start.sh" "$follower_one"
write add 4
"$ROOT_DIR/scripts/docker-node-start.sh" "$old_leader"
"$ROOT_DIR/scripts/docker-node-start.sh" "$follower_two"
write add 1
assert_final_value 11
if "$ROOT_DIR/scripts/docker-cluster-apply-log.sh" | grep -q 'value=7777'; then
  echo "S6 rejected marker was applied" >&2
  exit 1
fi
echo "ASSERT rejected marker was never applied: PASS"
finish_scenario

start_scenario s7
write add 8
write multiply 9
"$ROOT_DIR/scripts/docker-cluster-stop.sh"
"$ROOT_DIR/scripts/docker-cluster-start.sh"
write add 28
assert_final_value 100
finish_scenario

start_scenario s8
for node in 1 2 3 4 5; do
  "$ROOT_DIR/scripts/docker-node-stop.sh" "$node"
  write add 10
  "$ROOT_DIR/scripts/docker-node-start.sh" "$node"
done
write add 10
assert_final_value 60
finish_scenario

start_scenario s9
for iteration in 1 2 3; do
  write add 1
  old_leader=$leader
  "$ROOT_DIR/scripts/docker-node-stop.sh" "$old_leader"
  write add 1
  [[ "$leader" != "$old_leader" ]]
  "$ROOT_DIR/scripts/docker-node-start.sh" "$old_leader"
done
write multiply 111
assert_final_value 666
finish_scenario

start_scenario s10
write add 5
write divide 0
"$ROOT_DIR/scripts/docker-cluster-raw-command.sh" not-json-poison-pill
write multiply 20
assert_final_value 100
warnings=$("$ROOT_DIR/scripts/docker-cluster-logs.sh" --no-follow |
  grep -c 'Ignoring invalid arithmetic command' || true)
[[ "$warnings" -ge 10 ]]
echo "ASSERT invalid-command warnings ($warnings): PASS"
finish_scenario

echo "All five-node E2E scenarios passed."
