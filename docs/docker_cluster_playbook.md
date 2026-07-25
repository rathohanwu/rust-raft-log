# Docker Raft cluster playbook

This guide starts the three-node Docker Raft cluster, sends arithmetic commands,
and lets you observe leader failover and state-machine recovery.

Run every command from the repository root. Docker must be running first:

```bash
docker info
```

## Quick start: one local cluster

For normal use, do **not** set any environment variable:

```bash
scripts/docker-cluster-up.sh
```

Every script defaults to the same Docker Compose project, `raft-lab`, so the
following commands automatically operate on that cluster:

```bash
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-state.sh
scripts/docker-node-stop.sh 2
scripts/docker-node-start.sh 2
scripts/docker-cluster-down.sh
```

The first startup builds `rust-raft-log:e2e`; later starts reuse the image. It
starts `node1`, `node2`, and `node3` and waits for their TCP health checks.

## Optional: choose a Docker project name

`PROJECT` is **not** a Raft setting and is not needed to operate one cluster. It
is the Docker Compose resource prefix: it determines the names of the
containers, network, and volumes. Use it only when you want a separate cluster
for another experiment or need to target a non-default cluster from a second
terminal.

```bash
export PROJECT=raft-play
scripts/docker-cluster-up.sh
```

This creates resources such as `raft-play-node1-1`,
`raft-play_raft-e2e-net`, and `raft-play_data1`, instead of the default
`raft-lab-*` resources. Every later command that should control this particular
cluster must use the same value:

```bash
export PROJECT=raft-play
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-state.sh
```

In a new terminal, run the same `export PROJECT=raft-play` before using the
scripts. If you forget it, the command will instead target the default
`raft-lab` cluster.

## Send commands and inspect the state machine

The Docker test node embeds an arithmetic state machine. It accepts `add`,
`subtract`, `multiply`, and `divide`, each with an integer operand.

```bash
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-command.sh multiply 5
scripts/docker-cluster-command.sh subtract 2
scripts/docker-cluster-state.sh
```

The client output identifies the leader that accepted the command:

```text
Leader: Node 2
Log index: 4
```

`docker-cluster-state.sh` queries every node. After the commands above, all
three should eventually report the same application state:

```json
{"last_applied":4,"node_id":1,"state":{"applied_commands":3,"value":3}}
```

The exact `last_applied` value may be larger than `applied_commands`: Raft adds
NoOp entries when a leader is elected, and those advance `last_applied` without
changing the arithmetic value.

If a node is stopped, the command prints that node as unavailable **and still
queries the remaining reachable nodes**. It returns a non-zero exit status to
indicate that the displayed result is partial; this is expected while you are
testing a failure.

Query just one node with:

```bash
scripts/docker-cluster-state.sh 2
```

## Stop the leader and observe failover

1. Send a command and note `Leader: Node N` in the output.
2. Stop that node.
3. Send another command. The client retries leader discovery; its output should
   name a different leader.

For example, if the first command says `Leader: Node 2`:

```bash
scripts/docker-cluster-command.sh add 1
scripts/docker-node-stop.sh 2
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-state.sh
```

The remaining two nodes form a majority, elect a replacement leader, and accept
the second command. The stopped node's volume is retained.

## Restart a failed node and observe catch-up

Start the stopped node again, send one more command to give replication a fresh
round, and poll its state:

```bash
scripts/docker-node-start.sh 2
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-state.sh
```

All three nodes should converge on the same `value` and `applied_commands`.

## Full-cluster restart and recovery

Use `stop` and `start` for a recoverable full shutdown. They preserve each
node's named Docker volume, including its Raft log and persistent term/vote
metadata.

```bash
scripts/docker-cluster-stop.sh
scripts/docker-cluster-start.sh
scripts/docker-cluster-state.sh
```

After restart, `commit_index` and `last_applied` begin at zero, as Raft requires.
Once a leader is elected, it commits a current-term NoOp. That safely establishes
the recovered committed prefix and makes every test state machine replay the
durable normal commands. Do not expect a node to apply every entry blindly at
boot: uncommitted log entries must remain unapplied.

Run the automated proof of this behavior with:

```bash
make e2e-docker-recovery
```

It commits `add(9)`, stops all three nodes, starts them from the same volumes,
and verifies all three report `value: 9` and `applied_commands: 1`.

## Logs and cleanup

Follow all node logs:

```bash
scripts/docker-cluster-logs.sh
```

Remove the containers and network while **keeping data volumes**:

```bash
scripts/docker-cluster-down.sh
```

Start again with `scripts/docker-cluster-up.sh`; the prior Raft logs will still
be present. To delete the cluster data and start completely fresh, use:

```bash
scripts/docker-cluster-down.sh --delete-data
```

## Automated checks

```bash
# Basic three-node write and applied-state check
make e2e-docker

# Kill the leader, elect a replacement, restart it, and converge
make e2e-docker-failover

# Stop all nodes, restart from retained volumes, and rebuild state
make e2e-docker-recovery
```
