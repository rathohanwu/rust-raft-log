# Docker Raft cluster playbook

This is the human-facing runbook for the interactive three-node Docker cluster. For the future Docker-test architecture and outstanding design work, see [the Docker E2E testing plan](docker_e2e_testing_plan.md).

Run these commands from the repository root with Docker running.

## Start a cluster

```bash
scripts/docker-cluster-up.sh
```

The script builds the current `rust-raft-log:e2e` image, starts `node1`, `node2`, and `node3`, and waits for their TCP health checks. It uses the `raft-lab` Compose project by default.

To run an independent cluster, set `PROJECT` before every related command:

```bash
export PROJECT=raft-play
scripts/docker-cluster-up.sh
```

## Send commands and inspect application

The Docker test node embeds an arithmetic state machine. Send commands through the retrying Raft client:

```bash
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-command.sh multiply 5
scripts/docker-cluster-command.sh subtract 2
```

The client reports the elected leader and committed log index. Inspect applied arithmetic commands in each node's logs with:

```bash
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-apply-log.sh 2
scripts/docker-cluster-logs.sh
```

`docker-cluster-apply-log.sh` accepts an optional node ID and `-l <lines>` to limit its output.

## Observe failover and recovery

Stop the leader named by the client output, then submit another command. The two remaining nodes form a quorum and elect a replacement:

```bash
scripts/docker-node-stop.sh 2
scripts/docker-cluster-command.sh add 1
scripts/docker-node-start.sh 2
scripts/docker-cluster-command.sh add 1
```

For a full recoverable restart, stop and start all nodes. Their named volumes retain the Raft log and hard state:

```bash
scripts/docker-cluster-stop.sh
scripts/docker-cluster-start.sh
```

## Cleanup

Remove containers and the network while retaining data volumes:

```bash
scripts/docker-cluster-down.sh
```

Use `--delete-data` only when you want a completely fresh cluster:

```bash
scripts/docker-cluster-down.sh --delete-data
```

## Automated checks

```bash
make e2e-docker
make e2e-docker-failover
```

Each check creates an isolated Compose project, builds the image, gathers logs under `artifacts/`, and tears down its temporary volumes.
