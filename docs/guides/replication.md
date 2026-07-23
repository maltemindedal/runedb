# Setting up replication

Stash supports leader/follower replication over the standard Redis handshake (`REPLCONF`, `PSYNC`). A replica connects to a master, receives a snapshot of current state, then applies a live stream of propagated commands.

> **Scope:** replication covers the `REPLCONF`, `PSYNC`, and `WAIT` surface. It is not a complete Redis replication implementation — there is no partial resynchronization, no replica chaining, and no automatic failover.

## Start a master and a replica

Run the master on the default port:

```bash
go run ./cmd/stash --port 6379
```

In a second terminal, start a replica pointed at it:

```bash
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379
```

The replica logs a completed handshake:

```
level=INFO msg="replica handshake completed" master_addr=127.0.0.1:6379 listening_port=6380
```

## Verify it works

Write to the master:

```bash
redis-cli -p 6379 SET greeting hello
```

Read from the replica:

```bash
redis-cli -p 6380 GET greeting
```

Check roles and connected replicas with `INFO replication`:

```bash
redis-cli -p 6379 INFO replication
```

```
# Replication
role:master
master_replid:...
master_repl_offset:31
slave_repl_offset:0
connected_slaves:1
slave0:id=1,port=6380,offset=31
```

> **TODO(verify):** the field names above are taken from `appendInfoReplication` in `internal/command/info.go`, but the sample values are illustrative rather than captured from a live master/replica pair. Run the two-server setup above and replace this block with real output.

## Replicate against a protected master

A password-protected master requires the replica to authenticate before `REPLCONF` or `PSYNC`. Supply the password with `--masterauth`:

```bash
# Master
go run ./cmd/stash --port 6379 --requirepass secret

# Replica
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379 --masterauth secret
```

An unauthenticated replica handshake against a protected master is rejected.

## Wait for acknowledgements

`WAIT` blocks until a given number of replicas have acknowledged the connection's last write, or until a timeout in milliseconds elapses:

```
127.0.0.1:6379> SET k v
OK
127.0.0.1:6379> WAIT 1 1000
(integer) 1
```

The reply is the number of replicas that acknowledged, which may be lower than requested if the timeout fires first. A timeout of `0` returns the current count immediately without waiting.

`WAIT` is not supported in `--event-loop` mode when it would actually have to wait — commands execute inline on the loop goroutine, so blocking would stall every connection. It returns an explicit error instead. The immediately satisfiable case (enough replicas already acknowledged, or a `0` timeout) still succeeds.

## What gets replicated

Commands that mutate state are forwarded to replicas; read commands are not. Two cases are deliberately asymmetric:

- **`PUBLISH` is replicated but not persisted.** It mutates no durable state, but subscribers connected to a replica should still receive the message.
- **`XADD` is persisted but not replicated.** Stream writes reach the AOF but are not currently forwarded.

The full per-command breakdown is in the [command reference](../reference/commands.md).

## Related

- [Configuration reference](../reference/configuration.md) — `--replicaof`, `--masterauth`, `--requirepass`
- [Securing a server](securing-a-server.md)
- [Architecture overview](../architecture/overview.md) — why persistence and replication use separate paths
