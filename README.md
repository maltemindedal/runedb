# RuneDB

A high-performance, concurrent Redis-compatible key-value store built from scratch in Go.

## Current status

- raw TCP listener with one goroutine per client
- RESP parser and writer
- sharded, thread-safe in-memory store with TTL support
- append-only-file durability with startup replay, configurable `appendfsync`, and `BGREWRITEAOF`
- optional RDB startup loading and graceful shutdown snapshots
- active connection registry for cleaner shutdowns
- Redis-compatible command coverage for strings, hashes, lists, sets, sorted sets, streams, transactions, pub/sub, replication, and observability
- password-gated connections via `--requirepass` and `AUTH <password>`
- Redis-style execution error prefixes for command failures
- connection-scoped client state for auth, transactions, pub/sub, and replication
- `--maxmemory` memory-pressure eviction with approximate accounting and probabilistic LRU sampling
- `INFO`, `SLOWLOG`, and `MONITOR` support for operational visibility
- multi-client shutdown coverage
- parser and store contention benchmarks
- RESP3 boolean/null parsing, encoding, and coercion coverage
- unit, integration, race, vet, and lint validation

## Implemented features

### Network and lifecycle

- TCP server built on `net.Listen` / `Accept`
- one goroutine per client connection
- graceful shutdown through context cancellation and tracked active clients
- per-connection `ClientState` attached to request contexts for `AUTH`, transactions, pub/sub, and replication state

### Protocol

- RESP2 request/response support for:
  - simple strings
  - errors
  - integers
  - bulk strings
  - arrays
- fragmented TCP packet handling in the parser
- partial RESP3 coverage for:
  - booleans
  - nulls

### Storage

- unified value model for strings, hashes, lists, sets, sorted sets, and streams
- fixed shard set with per-shard `sync.RWMutex` locking
- passive TTL eviction on reads
- active TTL eviction in a background loop
- atomic `INCR` at the storage layer
- approximate keyspace memory accounting when `--maxmemory` is enabled
- probabilistic LRU eviction under memory pressure

### Persistence

- startup RDB loading via `--rdb`
- graceful shutdown RDB snapshots via `--dump`
- append-only file replay via `--aof`
- configurable `--appendfsync=always|everysec|no`
- background AOF compaction via `BGREWRITEAOF`

### Commands

- `PING`
- `AUTH <password>`
- `ECHO <message>`
- `SET <key> <value> [EX seconds|PX milliseconds]`
- `GET <key>`
- `DEL <key> [key ...]`
- `INCR <key>`
- `HSET <key> <field> <value> [field value ...]`
- `HGET <key> <field>`
- `HDEL <key> <field> [field ...]`
- `HGETALL <key>`
- `LPUSH <key> <element> [element ...]`
- `RPUSH <key> <element> [element ...]`
- `LPOP <key> [count]`
- `RPOP <key> [count]`
- `LRANGE <key> <start> <stop>`
- `BLPOP <key>`
- `SADD <key> <member> [member ...]`
- `SISMEMBER <key> <member>`
- `SREM <key> <member> [member ...]`
- `SMEMBERS <key>`
- `ZADD <key> <score> <member> [score member ...]`
- `ZRANGE <key> <start> <stop> [WITHSCORES]`
- `XADD <key> <id|*> <field> <value> [field value ...]`
- `XREAD STREAMS <key> <id>`
- `MULTI`
- `EXEC`
- `DISCARD`
- `WATCH <key> [key ...]`
- `SUBSCRIBE <channel> [channel ...]`
- `UNSUBSCRIBE [channel ...]`
- `PUBLISH <channel> <message>`
- `REPLCONF <subcommand> <arg>`
- `PSYNC ? -1`
- `WAIT <numreplicas> <timeout>`
- `BGREWRITEAOF`
- `INFO [default|all|memory|replication|clients]`
- `SLOWLOG GET [count]`
- `SLOWLOG LEN`
- `SLOWLOG RESET`
- `MONITOR`

### Quality and verification

- table-driven unit tests
- end-to-end TCP integration tests
- multi-client shutdown test coverage
- parser and store benchmarks
- `go vet`, race detector, and `golangci-lint` validation

## Project layout

- `cmd/runedb` — application entrypoint
- `internal/config` — runtime config and flags
- `internal/logger` — shared `log/slog` setup
- `internal/protocol` — RESP parsing and encoding
- `internal/storage` — key/value store and TTL eviction
- `internal/command` — command decoding and execution
- `internal/server` — TCP listener, handlers, and connection registry
- `test` — end-to-end TCP integration tests
- `docs` — PRD, architecture notes, and implementation checklist

## Running locally

From `d:\10_personal\runedb`:

### Start the server

- `go run ./cmd/runedb --port 6379`
- `go run ./cmd/runedb --port 6379 --aof appendonly.aof --appendfsync everysec`
- `go run ./cmd/runedb --port 6379 --maxmemory 104857600`
- `go run ./cmd/runedb --port 6380 --replicaof 127.0.0.1:6379 --masterauth secret`

### Validate the project

- `go test ./...`
- `go vet ./...`
- `go test -race ./...`
- `golangci-lint run`

### Run benchmarks

- `go test -run ^$ -bench . ./internal/protocol ./internal/storage`

Then connect with `redis-cli` or any RESP-capable TCP client.

## Quick smoke test with redis-cli

After starting the server, open another terminal and connect:

- `redis-cli -p 6379`

Then try:

- `PING` → `PONG`
- `ECHO hello` → `hello`
- `SET name RuneDB` → `OK`
- `GET name` → `RuneDB`
- `DEL name` → `(integer) 1`
- `INCR counter` → `(integer) 1`
- `INCR counter` → `(integer) 2`
- `SET temp 1 PX 100`
- wait a moment, then `GET temp` → `(nil)`
- `SET bad hello` → `OK`
- `INCR bad` → `ERR value is not an integer or out of range`

## Command notes

- command names are case-insensitive on the wire
- `PING` optionally accepts one payload and returns it as a bulk string
- `SET` currently supports only `EX` and `PX`
- unsupported `SET` modifiers still return syntax-style errors instead of being ignored
- `GET` on a missing key returns a null bulk string
- `DEL` ignores missing keys and returns the number of keys removed
- `INCR` initializes missing keys to `1` and works on base-10 signed 64-bit integer strings
- type-specific commands return Redis-style `WRONGTYPE` errors when a key holds a different value kind
- subscribed clients may only use `PING`, `SUBSCRIBE`, and `UNSUBSCRIBE`
- monitoring clients may only use `PING`
- `SLOWLOG` redacts `AUTH` arguments before storing command metadata

## Current boundaries

- The store uses a fixed shard set with shard-local locks; cross-shard multi-key commands lock shards in deterministic order.
- TTLs are normalized to Unix milliseconds internally.
- Startup-time RDB loading is available via `--rdb`, currently for DB `0` string keys only.
- Append-only durability is available via `--aof`; when the file exists and is non-empty it takes precedence over `--rdb` during startup.
- `BGREWRITEAOF` compacts the current append-only file in the background by snapshotting live state and atomically swapping in a rewritten command stream.
- `--maxmemory` uses approximate keyspace accounting, not exact process RSS accounting.
- `INFO memory` includes Go heap stats and approximate keyspace metrics; fragmentation is currently reported as a placeholder ratio.
- `SLOWLOG` keeps a bounded in-memory buffer with Redis' default length of 128 entries.
- `MONITOR` streams command events to monitoring clients and disconnects slow monitor consumers after a short write deadline.
- The server is still intentionally RESP2-centric at the command-behavior level, even though RESP3 boolean/null support exists in the protocol layer.
- `--requirepass` and one-argument `AUTH <password>` are implemented. Unauthenticated clients may still use `PING`, but protected masters now require authenticated replica handshakes before `REPLCONF` / `PSYNC`; replica mode can supply that password with `--masterauth`.
- Core replication protocol support exists (`REPLCONF`, `PSYNC`, and `WAIT`), and append-only persistence now covers the supported mutating command surface; broader replication completeness and further persistence hardening remain future milestones.

## Related docs

- `docs/ARCHITECTURE.md` — package responsibilities and design rationale
- `docs/PRD.md` — project goals and later-phase roadmap
