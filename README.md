# godis

A high-performance, concurrent Redis-compatible key-value store built from scratch in Go.

## Current status

- raw TCP listener with one goroutine per client
- RESP parser and writer
- thread-safe in-memory store with TTL support
- active connection registry for cleaner shutdowns
- Phase 1 commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `INCR`
- Redis-style execution error prefixes for command failures
- connection-scoped client state scaffolding for future auth/transactions
- multi-client shutdown coverage
- parser and store contention benchmarks
- RESP3 boolean/null parsing, encoding, and coercion coverage
- unit, integration, race, vet, and lint validation

## Implemented features

### Network and lifecycle

- TCP server built on `net.Listen` / `Accept`
- one goroutine per client connection
- graceful shutdown through context cancellation and tracked active clients
- per-connection `ClientState` attached to request contexts for future `AUTH` / transaction support

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

- in-memory string key/value store
- single `sync.RWMutex` for correctness and simplicity
- passive TTL eviction on reads
- active TTL eviction in a background loop
- atomic `INCR` at the storage layer

### Commands

- `PING`
- `ECHO <message>`
- `SET <key> <value> [EX seconds|PX milliseconds]`
- `GET <key>`
- `DEL <key> [key ...]`
- `INCR <key>`

### Quality and verification

- table-driven unit tests
- end-to-end TCP integration tests
- multi-client shutdown test coverage
- parser and store benchmarks
- `go vet`, race detector, and `golangci-lint` validation

## Project layout

- `cmd/godis` — application entrypoint
- `internal/config` — runtime config and flags
- `internal/logger` — shared `log/slog` setup
- `internal/protocol` — RESP parsing and encoding
- `internal/storage` — key/value store and TTL eviction
- `internal/command` — command decoding and execution
- `internal/server` — TCP listener, handlers, and connection registry
- `test` — end-to-end TCP integration tests
- `docs` — PRD, architecture notes, and implementation checklist

## Running locally

From `d:\10_personal\godis`:

### Start the server

- `go run ./cmd/godis --port 6379`

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
- `SET name godis` → `OK`
- `GET name` → `godis`
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

## Current boundaries

- The store currently uses a single `sync.RWMutex` for correctness and simplicity.
- TTLs are normalized to Unix milliseconds internally.
- The server is still intentionally RESP2-centric at the command-behavior level, even though RESP3 boolean/null support exists in the protocol layer.
- `Config.RequirePass` and per-connection client state are scaffolding for future auth/transaction work; full `AUTH`, `MULTI`, `EXEC`, `DISCARD`, and related behavior are not implemented yet.
- Replication, persistence, richer data structures, pub/sub, and full security features remain future milestones.

## Related docs

- `docs/ARCHITECTURE.md` — package responsibilities and design rationale
- `docs/PRD.md` — project goals and later-phase roadmap
