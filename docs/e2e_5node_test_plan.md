# Five-Node Raft Cluster Scenarios

Run each scenario from the repository root. Use the actual scripts shown below;
there are no helper functions or hidden setup steps.

To execute every scenario with the same checks, run `make e2e-5node`. It
builds the image once, then removes the containers, network, and named volumes
between scenarios while reusing that image.

- `scripts/docker-cluster-reset.sh` removes the previous test data and starts
  a fresh five-node cluster.
- `scripts/docker-cluster-command.sh <action> <integer>` submits a normal
  arithmetic command. It prints the current leader as `Leader: Node N`.
- Whenever a step says `<leader>`, substitute the node ID printed by the
  preceding command. Choose any other node when a step says `<follower>`.
- Finish each scenario with `scripts/docker-cluster-apply-log.sh`: it prints
  one section per node so the applied commands and final value can be checked.
  Then run `scripts/docker-cluster-down.sh --delete-data`.

## S1 — Smoke: one write reaches all five nodes

Expected final value on every node: **7**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 7
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S2 — Ordered non-commutative workload

Expected final value on every node: **112**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 3
scripts/docker-cluster-command.sh multiply 5
scripts/docker-cluster-command.sh subtract 7
scripts/docker-cluster-command.sh multiply 3
scripts/docker-cluster-command.sh divide 2
scripts/docker-cluster-command.sh add 100
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S3 — Follower crash and catch-up

After `add 10`, note the reported leader and choose a different node as
`<follower>`. Expected final value on every node: **30**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 10
scripts/docker-node-stop.sh <follower>
scripts/docker-cluster-command.sh multiply 4
scripts/docker-cluster-command.sh subtract 15
scripts/docker-node-start.sh <follower>
scripts/docker-cluster-command.sh add 5
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S4 — Leader failover

After `add 3`, stop the reported `<leader>`. The next command must succeed
through a different leader. Expected final value on every node: **42**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 3
scripts/docker-node-stop.sh <leader>
scripts/docker-cluster-command.sh add 4
scripts/docker-node-start.sh <leader>
scripts/docker-cluster-command.sh multiply 6
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S5 — Double failure, including the leader

After `add 9`, stop the reported `<leader>` and one different
`<follower>`. Three nodes remain, which is the five-node quorum. Expected
final value on every node after both nodes return: **42**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 9
scripts/docker-node-stop.sh <leader>
scripts/docker-node-stop.sh <follower>
scripts/docker-cluster-command.sh subtract 4
scripts/docker-cluster-command.sh multiply 8
scripts/docker-node-start.sh <leader>
scripts/docker-node-start.sh <follower>
scripts/docker-cluster-command.sh add 2
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S6 — Quorum loss rejects writes

After `add 6`, stop the reported `<leader>` and two different followers.
Only two nodes remain, so the `add 7777` command must fail. Start one stopped
follower to restore a quorum, then start the remaining two nodes. Expected
final value on every node: **11**; no apply-log line may contain `value=7777`.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 6
scripts/docker-node-stop.sh <leader>
scripts/docker-node-stop.sh <follower-1>
scripts/docker-node-stop.sh <follower-2>
scripts/docker-cluster-command.sh add 7777
scripts/docker-node-start.sh <follower-1>
scripts/docker-cluster-command.sh add 4
scripts/docker-node-start.sh <leader>
scripts/docker-node-start.sh <follower-2>
scripts/docker-cluster-command.sh add 1
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S7 — Full-cluster restart preserves data

Expected final value on every node after restart: **100**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 8
scripts/docker-cluster-command.sh multiply 9
scripts/docker-cluster-stop.sh
scripts/docker-cluster-start.sh
scripts/docker-cluster-command.sh add 28
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S8 — Rolling restart under writes

Stop and restart each node in turn. The client retries while a new leader is
elected when needed. Expected final value on every node: **60**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-node-stop.sh 1
scripts/docker-cluster-command.sh add 10
scripts/docker-node-start.sh 1
scripts/docker-node-stop.sh 2
scripts/docker-cluster-command.sh add 10
scripts/docker-node-start.sh 2
scripts/docker-node-stop.sh 3
scripts/docker-cluster-command.sh add 10
scripts/docker-node-start.sh 3
scripts/docker-node-stop.sh 4
scripts/docker-cluster-command.sh add 10
scripts/docker-node-start.sh 4
scripts/docker-node-stop.sh 5
scripts/docker-cluster-command.sh add 10
scripts/docker-node-start.sh 5
scripts/docker-cluster-command.sh add 10
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S9 — Repeated leader kills

Repeat the leader-stop/write/start sequence three times. Each post-stop write
must report a leader different from the node just stopped. Expected final value
on every node: **666**.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 1
scripts/docker-node-stop.sh <leader-1>
scripts/docker-cluster-command.sh add 1
scripts/docker-node-start.sh <leader-1>
scripts/docker-cluster-command.sh add 1
scripts/docker-node-stop.sh <leader-2>
scripts/docker-cluster-command.sh add 1
scripts/docker-node-start.sh <leader-2>
scripts/docker-cluster-command.sh add 1
scripts/docker-node-stop.sh <leader-3>
scripts/docker-cluster-command.sh add 1
scripts/docker-node-start.sh <leader-3>
scripts/docker-cluster-command.sh multiply 111
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-down.sh --delete-data
```

## S10 — Poison payloads are committed but ignored

The raw-payload script deliberately bypasses arithmetic argument validation.
Expected final value on every node: **100**. The apply log should include only
the valid `add 5` and `multiply 20` operations; the two invalid payloads
should produce ignored-command warnings.

```bash
scripts/docker-cluster-reset.sh
scripts/docker-cluster-command.sh add 5
scripts/docker-cluster-command.sh divide 0
scripts/docker-cluster-raw-command.sh not-json-poison-pill
scripts/docker-cluster-command.sh multiply 20
scripts/docker-cluster-apply-log.sh
scripts/docker-cluster-logs.sh --no-follow
scripts/docker-cluster-down.sh --delete-data
```
