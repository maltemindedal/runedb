# RuneDB Context

RuneDB is a Go implementation of a Redis-compatible TCP key/value server. The project is intentionally implementation-led: each feature should preserve Redis wire compatibility where supported while keeping package boundaries small and explicit.

## Domain Glossary

- RuneDB: the server binary and codebase for the Redis-compatible store.
- RESP: the Redis Serialization Protocol used on client, persistence, and replication command streams.
- Command executor: the `internal/command` component that decodes RESP arrays, validates commands, applies connection state rules, executes handlers, and returns RESP values.
- Store: the `internal/storage` in-memory key/value engine.
- Shard: one fixed partition of the store guarded by a shard-local lock.
- Value kind: the logical Redis data type stored for a key: string, hash, list, set, sorted set, or stream.
- TTL: an expiry timestamp stored in Unix milliseconds and enforced by passive reads and active eviction.
- AOF: append-only-file durability, stored as replayable RESP command frames.
- AOF rewrite: background compaction that snapshots live durable state into a replacement append-only file.
- RDB snapshot: Redis database file support used for startup loading and graceful-shutdown snapshots.
- Client state: per-connection state for authentication, transactions, pub/sub mode, monitor mode, and replication role.
- Connection state machine: the non-blocking per-connection component that buffers readable bytes, parses complete RESP requests, executes commands, and flushes buffered responses incrementally without a dedicated blocking goroutine.
- Event loop: the opt-in networking mode (`--event-loop`) that serves all client connections from one goroutine backed by OS readiness notifications (`epoll` on Linux, `kqueue` on macOS), dispatching readable and writable sockets through connection state machines.
- Transaction: Redis-style `MULTI` / `EXEC` command queueing with `WATCH`-based optimistic invalidation.
- Pub/sub registry: exact-channel subscription bookkeeping used by `SUBSCRIBE`, `UNSUBSCRIBE`, and `PUBLISH`.
- Replica: a downstream server connected through the supported replication handshake.
- Master: an upstream server that accepts replica handshakes and forwards propagated command frames.
- Slowlog: bounded in-memory command timing log used by `SLOWLOG`.
- Monitor: command-event fan-out mode used by `MONITOR`.
- Maxmemory: approximate keyspace memory pressure limit that triggers probabilistic LRU eviction.
- HyperLogLog: fixed-size approximate cardinality register set stored as a string value and used by `PFADD` and `PFCOUNT`.
- Geohash score: a 52-bit interleaved longitude/latitude encoding stored as a sorted-set score and used by `GEOADD`, `GEODIST`, and `GEORADIUS`.

## Current Capabilities

- RESP2-centric TCP server with one goroutine per client by default, an opt-in OS I/O multiplexing event loop, and graceful signal-driven shutdown.
- Thread-safe sharded in-memory storage for strings, hashes, lists, sets, sorted sets, streams, and bitmap and HyperLogLog operations over string values.
- Geospatial commands (`GEOADD`, `GEODIST`, `GEORADIUS`) backed by geohash scores in regular sorted sets.
- TTL handling, append-only durability, startup RDB loading, shutdown RDB snapshots, and background AOF rewrite.
- Authentication, transactions, pub/sub, basic replication, memory-pressure eviction, slowlog, monitor, and `INFO` visibility.
- Unit, integration, race, vet, lint, and benchmark coverage are part of the expected verification workflow.

## Boundaries

- RESP3 support is limited to protocol-layer boolean/null parsing, encoding, and coercion; command behavior remains RESP2-centric.
- RDB loading is limited to DB `0` string keys.
- Memory accounting is approximate keyspace accounting, not exact process RSS accounting.
- Replication supports the current `REPLCONF`, `PSYNC`, and `WAIT` surface but is not a complete Redis replication implementation.
- Redis compatibility is scoped to commands explicitly implemented in `internal/command`; unsupported modifiers should fail explicitly rather than being silently accepted.
- The event loop is opt-in and supported on Linux (`epoll`) and macOS (`kqueue`); other platforms, including Windows, fall back to goroutine-per-connection networking with a startup warning.
- In event-loop mode, commands execute inline on the loop goroutine, so a blocking command such as `WAIT` with a long timeout stalls the other connections served by the loop for its duration.

## Documentation Rules

- Use the glossary terms above when naming issues, tests, refactors, and architecture notes.
- Update this file when a new domain concept becomes stable in the codebase.
- Use `docs/ARCHITECTURE.md` for package responsibility details and design rationale.
