# Product Requirements Document (PRD): RuneDB

- **Status:** Draft
- **Target Language:** Go (Golang)
- **Primary Focus:** Concurrency, low-level networking (TCP), custom protocols (RESP), and distributed systems
- **Description:** A high-performance, concurrent Redis-compatible key-value store built from scratch

### Current Implementation Status

Legend: `[x]` done, `[~]` partially done, `[ ]` not done.

- **Phase 1:** Done
- **Phase 2:** Done
- **Phase 3:** Done
- **Phase 4:** Done (`RDB` startup loading is implemented for DB 0 string keys, and replication handshake/propagation/`WAIT` support is implemented)
- **Phase 5:** Done
- **Phase 6:** Done
- **Phase 7:** Planned
- **Phase 8:** Done
- **Phase 9:** Done
- **Phase 10:** Planned
- **Phase 11:** Planned

## 1. Executive Summary

This project aims to build **RuneDB**, a production-ready, highly concurrent, in-memory key-value data store from scratch, fully compliant with the **Redis Serialization Protocol (RESP)**.

The goal is to demonstrate advanced systems engineering capabilities, including:

- Managing TCP socket connections
- Handling concurrent data access safely using Go primitives
- Parsing custom binary protocols
- Implementing distributed systems features like replication and persistence

## 2. Technical Architecture & Constraints

To ensure this project accurately reflects senior-level engineering, the following constraints are strictly enforced:

- **No web frameworks:** Communication must be handled via raw TCP sockets using Go's `net` package (`net.Listen`, `net.Accept`)
- **Thread safety:** The core data store must utilize `sync.RWMutex` to allow concurrent reads while safely locking writes
- **Memory efficiency:** Protocol parsing must minimize heap allocations by utilizing Go byte slices (`[]byte`) efficiently and `bufio.Reader` / `bufio.Writer` for buffered I/O
- **Protocol:** Must implement the Redis Serialization Protocol (**RESP2/RESP3**) natively to ensure compatibility with the official `redis-cli`

## 3. Scope & Milestones (Epics)

### Phase 1: Foundation & The Network Layer — **Done**

Establish the core TCP loop, memory store, and parser.

#### Networking (TCP Loop) — **Done**

- [x] Implement `net.Listen("tcp", ":6379")`
- [x] Use an infinite `for` loop calling `Accept()`
- [x] For each incoming connection, spawn a new goroutine: `go handleConnection(conn)`
- [x] Maintain a thread-safe registry of active client connections to handle clean disconnects

#### Protocol Parsing (RESP) — **Done**

- [x] Implement a parser that reads from the TCP socket using `bufio.Reader`
- [x] Parse RESP data types:
  - [x] Simple Strings (`+`)
  - [x] Errors (`-`)
  - [x] Integers (`:`)
  - [x] Bulk Strings (`$`)
  - [x] Arrays (`*`)
- [x] Handle fragmented TCP packets (e.g., a bulk string might arrive in multiple TCP segments)
- [x] Rely on the lengths specified in the RESP protocol (`$<length>\r\n`), not network frame sizes

#### Basic Commands (`PING`, `ECHO`) — **Done**

- [x] `PING`: Return `+PONG\r\n` (Simple String)
- [x] `ECHO <msg>`: Return the exact argument as a Bulk String: `$<len>\r\n<msg>\r\n`

#### Core Storage Engine (`SET`, `GET`) — **Done**

- [x] Create a global state struct containing a `map[string]Value` and a `sync.RWMutex`
- [x] `Value` should be a struct containing:
  - [x] `Data ([]byte)` _(implemented as the string payload field on the stored value type)_
  - [x] `ExpiresAt (int64)`
- [x] `GET`:
  - [x] Acquire `RLock()`
  - [x] Read map
  - [x] Release `RUnlock()`
- [x] `SET`:
  - [x] Acquire `Lock()`
  - [x] Write map
  - [x] Release `Unlock()`

#### State Management (Key Expiry / TTL) — **Done**

- [x] Implement `PX` (milliseconds) and `EX` (seconds) arguments for `SET`
- [x] **Passive eviction:** On `GET`, check if `time.Now().UnixMilli() > ExpiresAt`; if true, delete the key and return `nil`
- [x] **Active eviction:** Create a background goroutine that wakes up every `100ms`, samples a batch of keys with TTLs, and deletes expired keys to prevent memory bloat from unread keys

### Phase 2: Advanced Data Structures — **Done**

Expand the data model, requiring specific memory layouts for performance.

#### Lists (`LPUSH`, `RPUSH`, `LRANGE`) — **Done**

- [x] Values in the store must support different types (`interface{}` or tagged unions)
- [x] Use a Go slice `[][]byte` for the list
- [x] For blocking commands (`BLPOP`), implement a pub/sub-like channel system where the client's goroutine pauses (`select` on a Go channel) until a background process or another client `PUSH`es to that key

#### Sorted Sets (`ZADD`, `ZRANGE`) — **Done**

Implementing this efficiently is complex. You need a composite data structure:

- [x] A `map[string]float64` for $O(1)$ lookups of a member's score
- [x] A Skip List (or a balanced Binary Search Tree) for $O(\log N)$ insertions and range queries based on scores

#### Streams (`XADD`, `XREAD`) — **Done**

- [x] IDs are formatted as `<millisecondsTime>-<sequenceNumber>`
- [x] Implement auto-generation of IDs (e.g., `XADD mystream * value`)
- [x] Handle the edge case where multiple commands arrive in the same millisecond by incrementing the sequence number
- [x] Store stream entries in an append-only slice or Radix tree

### Phase 3: ACID Transactions & Concurrency Control — **Done**

Handle complex, multi-step operations safely.

#### Basic Atomicity (`INCR`) — **Done**

- [x] Read the value, parse it as a base-10 integer, increment, convert back to bytes, and save
- [x] Wrap this entire read-modify-write cycle in a single write `Lock()` to prevent race conditions

#### Transaction Queueing (`MULTI`, `EXEC`, `DISCARD`) — **Done**

- [x] Add state to the client connection struct:
  - [x] `InTransaction (bool)`
  - [x] `TxQueue ([]Command)`
- [x] When `MULTI` is received:
  - [x] Set `InTransaction = true`
  - [x] Return `+OK\r\n`
- [x] Subsequent commands are parsed but **not** executed:
  - [x] Append them to `TxQueue`
  - [x] Return `+QUEUED\r\n`
- [x] On `EXEC`:
  - [x] Iterate through `TxQueue`
  - [x] Execute all commands sequentially
  - [x] Return a RESP Array of their results

#### Optimistic Locking (`WATCH`) — **Done**

- [x] Create a global `map[string][]net.Conn` mapping keys to watching clients _(implemented with a shared watch registry keyed by key and client state)_
- [x] Whenever a key is modified (e.g., via `SET`), check this map
- [x] If a watching client is found, mark their connection state `TxFailed = true`
- [x] When a client calls `EXEC`, if `TxFailed` is `true`, abort the transaction and return a Null Array: `*-1\r\n`

### Phase 4: Distributed Systems & High Availability — **Done**

Implement the Redis replication protocol.

#### RDB Persistence (Disk I/O) — **Done**

- [x] Parse the `.rdb` binary format on server startup
- [x] Read the 9-byte magic string `REDIS0011`
- [x] Handle database selectors (`0xFE`) and expiry metadata opcodes (`0xFD`, `0xFC`)
- [x] Load the keys into the in-memory map before accepting TCP connections

Current implementation scope: startup-time loading via `--rdb` plus graceful shutdown snapshots to `dump.rdb`, DB `0` only, and string values only. Unsupported databases or value types fail fast during startup, while unsupported in-memory value types are skipped with a warning during shutdown snapshotting.

#### Master / Replica Handshake — **Done**

- [x] When acting as a **Replica**:
  - [x] Connect to the Master's TCP port
  - [x] Send `PING`, `REPLCONF listening-port <port>`, and `PSYNC ? -1`
- [x] When acting as a **Master**:
  - [x] Respond to `PSYNC` with `+FULLRESYNC <master_replid> 0\r\n`
  - [x] Send an empty RDB file over the socket as a heavily formatted Bulk String

#### Command Propagation — **Done**

- [x] The Master must maintain a list of active Replica TCP connections
- [x] Whenever a state-mutating command (`SET`, `DEL`) is executed successfully, the Master encodes it back into RESP and writes it to every Replica's network socket

#### Synchronization (`WAIT <numreplicas> <timeout>`) — **Done**

- [x] Master must track the replication offset (number of bytes of commands sent)
- [x] Master pauses the client's goroutine
- [x] Master sends `REPLCONF GETACK *` to replicas
- [x] Replicas reply with `REPLCONF ACK <offset>`
- [x] Master resumes the client when `<numreplicas>` have reported an offset greater than or equal to the Master's offset, or when `<timeout>` is reached

### Phase 5: Pub/Sub & Security — **Done**

#### Pub/Sub Engine (`SUBSCRIBE`, `PUBLISH`) — **Done**

- [x] Maintain a global `map[string][]net.Conn` mapping channel names to active client sockets _(implemented as a shared channel-to-client-state registry with synchronized connection writers)_
- [x] When a client issues `SUBSCRIBE`, flag their connection
- [x] They can now only issue `PING`, `SUBSCRIBE`, and `UNSUBSCRIBE` commands
- [x] `PUBLISH <channel> <msg>`:
  - [x] Look up the channel in the map
  - [x] Format a RESP Array (e.g., `*3\r\n$7\r\nmessage\r\n...`)
  - [x] Write it to all matching client sockets

#### Authentication (`AUTH <password>`) — **Done**

- [x] Allow passing a `--requirepass` flag on server startup
- [x] Allow replicas to authenticate to protected masters via a `masterauth`-style configuration path
- [x] If set, all client connections initialize with `Authenticated = false`
- [x] Reject all commands (return `-NOAUTH Authentication required.\r\n`) except `AUTH` and `PING` until `AUTH` is successfully called

### Phase 6: Unified Value Model & Additional Types — **Done**

Extend the current value model so additional Redis-compatible data types can share a consistent storage abstraction.

#### Value Object Refactor — **Done**

- [x] Transition the underlying store to `map[string]*ValueObject`.
- [x] Let `ValueObject` hold the logical type, payload, TTL metadata, and future access metadata.
- [x] Update `SET`, `GET`, and `INCR` to operate through the unified value representation.

#### Strict Type Enforcement — **Done**

- [x] Introduce the Redis-style `WRONGTYPE` error path.
- [x] Validate the stored value kind before executing type-specific commands.

#### Hashes (`HSET`, `HGET`, `HDEL`, `HGETALL`) — **Done**

- [x] Back hashes with a `map[string]string`-style structure.
- [x] Implement `HSET` and `HGET`.
- [x] Implement `HDEL` and `HGETALL`, including RESP array packing.

#### Lists (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`) — **Done**

- [x] Normalize list storage behind `ValueObject`.
- [x] Round out list operations with `LPOP`, `RPOP`, and `LRANGE`.

#### Sets (`SADD`, `SISMEMBER`, `SREM`, `SMEMBERS`) — **Done**

- [x] Back sets with `map[string]struct{}` for $O(1)$ membership checks.
- [x] Implement `SADD`, `SISMEMBER`, `SREM`, and `SMEMBERS`.

### Phase 7: Sharded Concurrency & Scaling — **In Progress**

Reduce the single-lock bottleneck by making key ownership explicit and shard-local.

#### Shard Architecture — **Done**

- [x] Introduce a `Shard` struct containing its own `map[string]*ValueObject` and `sync.RWMutex`.
- [x] Refactor the top-level store to own a fixed set of shards (for example, 256 shards).

#### Key Hashing — **Done**

- [x] Add a fast key-to-shard hashing function, such as `maphash` or FNV-1a.
- [x] Route commands by calculating `hash(key) % shardCount`.

#### Single-Key Command Refactor — **Done**

- [x] Update `GET`, `SET`, and `INCR` to lock only the shard that owns the target key.
- [x] Preserve throughput by keeping unrelated keys and the accept loop unblocked.

#### Cross-Shard Coordination — **Done**

- [x] Handle multi-key commands such as `DEL key1 key2 key3` across different shards.
- [x] Sort shard IDs before locking to avoid deadlocks, then unlock in reverse order.

### Phase 8: Durability via Append-Only Files — **Done**

Add an AOF-based durability path that complements the current snapshot-oriented persistence support.

#### AOF Writer — **Done**

- [x] Create a background service that opens and manages a `.aof` file.
- [x] Append raw RESP for successful mutating commands such as `SET`, `DEL`, and `INCR`.

#### AOF Recovery — **Done**

- [x] On startup, detect the `.aof` file before accepting TCP connections.
- [x] Parse and replay commands to rebuild in-memory state.

#### `appendfsync` Policies — **Done**

- [x] Add an `--appendfsync` flag.
- [x] Support at least `always` and `everysec` policies.
- [x] For `everysec`, flush to disk from a background goroutine once per second.

#### Background Rewrite (`BGREWRITEAOF`) — **Done**

- [x] Compact large AOF files by writing a fresh command stream to a temporary file.
- [x] Emit the shortest practical rebuild sequence, then atomically rename it into place.

### Phase 9: Transaction Execution Hardening — **Done**

Formalize and harden the existing transaction semantics around queued execution and optimistic locking.

#### Command Queue — **Done**

- [x] Keep explicit queued command state and transaction mode in `ClientState`.
- [x] Preserve watched-key invalidation across `WATCH` → external write → `MULTI`.
- [x] Continue tightening transaction lifecycle invariants and regression coverage.

#### `MULTI` & `DISCARD` — **Done**

- [x] Queue subsequent commands after `MULTI` and return `+QUEUED`.
- [x] Clear queued state and exit transaction mode on `DISCARD`.
- [x] Expand coverage around `DISCARD` cleanup and queue invalidation edge cases.

#### `EXEC` — **Done**

- [x] Execute queued commands sequentially against the store.
- [x] Package per-command results into a RESP array and clear the queue.
- [x] Harden propagation and durability coverage for mixed-success `EXEC` payloads.

#### `WATCH` (Optimistic Locking) — **Done**

- [x] Track watched keys in shared state and associate them with clients.
- [x] Abort `EXEC` with a null array when a watched key changed before commit.
- [x] Expand lifecycle coverage around pre-`MULTI` invalidation and connection cleanup.

### Phase 10: Pub/Sub Broker Hardening — **Planned**

Strengthen Pub/Sub delivery semantics and subscription bookkeeping.

#### Broker Registry — **Planned**

- [ ] Maintain a central channel-to-client registry for subscriber tracking.

#### Subscriber Mode Enforcement — **Planned**

- [ ] Restrict subscribed clients to the allowed command surface.
- [ ] Keep subscription bookkeeping synchronized with connection lifecycle changes.

#### Fan-Out Delivery — **Planned**

- [ ] Deliver published payloads to every subscribed client on the target channel.
- [ ] Harden writer synchronization and cleanup on disconnect.

### Phase 11: Memory Limits & Eviction — **Planned**

Protect the server from unbounded memory growth under sustained write pressure.

#### Access Tracking — **Planned**

- [ ] Extend `ValueObject` with a `lastAccessed` timestamp.
- [ ] Refresh the timestamp on reads and writes such as `GET` and `SET`.

#### `maxmemory` — **Planned**

- [ ] Add a `--maxmemory` flag.
- [ ] Track approximate memory usage, or start with a simpler proxy such as key count.
- [ ] Reject new writes or trigger eviction when the configured limit is reached.

#### Probabilistic LRU Eviction — **Planned**

- [ ] Sample a small random set of keys instead of maintaining a strict global LRU list.
- [ ] Evict the stalest candidate and repeat until memory usage falls below the limit.

## 4. Non-Functional Requirements (Production Readiness)

- [x] **High concurrency performance:** Avoid blocking the main accept loop. Lock contention is minimized, and `GET` uses `RLock()` / `RUnlock()` paths.
- [x] **Graceful shutdown:** Signal-driven shutdown, listener stop, client cleanup, handler waiting, and shutdown-time flushing of the supported RDB snapshot scope to `dump.rdb` are implemented.
- [x] **Observability:** `log/slog` structured logging is in place for connection / parser / response issues and now includes replication-specific lifecycle events such as replica registration/removal, FULLRESYNC snapshot application, ACK handling, `WAIT`, and propagation outcomes.

## 5. Out of Scope (For V1)

- Redis Cluster Mode (sharding / gossip protocol)
- Lua scripting execution (`EVAL`)
- Multi-part AOF manifests / RDB preambles / automatic rewrite thresholds
