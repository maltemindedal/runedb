# Godis Architecture

This repository is structured as a Phase 1-first implementation of a Redis-compatible TCP server in Go. The current scaffold is intentionally narrow: it provides the networking, protocol, storage, and command boundaries needed for `PING`, `ECHO`, `SET`, and `GET`, while leaving space for RESP3, transactions, replication, and richer data types in later phases.

## Package layout

### `cmd/godis`

Contains the `main` package and the production binary entrypoint. It is responsible for:

- parsing runtime flags
- creating the logger, store, command executor, and TCP server
- starting the server with signal-aware shutdown

### `internal/config`

Holds startup configuration such as host, port, log level, and TTL eviction settings.

### `internal/logger`

Provides a small `log/slog` initializer so the whole server shares one structured logging setup.

### `internal/protocol`

Owns RESP parsing and encoding.

Current responsibilities:

- parse RESP2 frames from a buffered TCP reader
- handle fragmented bulk strings and arrays correctly
- encode simple strings, errors, integers, bulk strings, and arrays
- include minimal RESP3 placeholder types (`Boolean`, `Null`) for future expansion

### `internal/storage`

Owns the in-memory key/value store.

Current responsibilities:

- protect the global map with `sync.RWMutex`
- support string values for Phase 1
- store TTLs in Unix milliseconds
- perform passive eviction on reads
- perform active eviction in a background loop

### `internal/command`

Translates RESP arrays into executable requests and dispatches them to handlers.

Current command set:

- `PING`
- `ECHO`
- `SET` with optional `EX` / `PX`
- `GET`

### `internal/server`

Owns the TCP lifecycle.

Current responsibilities:

- create the listener with `net.Listen`
- accept client connections in a loop
- spawn one goroutine per client
- parse → execute → respond for each request
- maintain an active connection registry for shutdown
- stop cleanly when the process receives `SIGINT` or `SIGTERM`

### `test`

Contains integration tests that verify the server end to end over a real TCP connection.

## Design notes

### Why a single store lock?

The scaffold starts with one `sync.RWMutex` around the entire keyspace because correctness matters more than cleverness at this stage. It satisfies the PRD and keeps the implementation easy to reason about. If contention becomes a bottleneck later, the store can be sharded or refactored with finer-grained locks.

### Why RESP3 placeholders now?

The user requested RESP3-aware structure, but full RESP3 support is out of scope for the current implementation. The protocol package therefore exposes lightweight placeholder types so later expansion does not require redesigning the core abstraction.

### Why only four commands?

`PING`, `ECHO`, `SET`, and `GET` are enough to validate the entire stack:

- TCP listener and handler loop
- RESP parsing and encoding
- command routing
- thread-safe store access
- TTL behavior

That makes them an excellent Phase 1 foundation.
