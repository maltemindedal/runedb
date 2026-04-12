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
- **Phase 5:** Partial (`--requirepass` / client auth state scaffold exists, but pub/sub and full AUTH flow are not implemented)

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
  - [x] `Data ([]byte)` *(implemented as the string payload field on the stored value type)*
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

- [x] Create a global `map[string][]net.Conn` mapping keys to watching clients *(implemented with a shared watch registry keyed by key and client state)*
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

Current implementation scope: startup-time loading via `--rdb`, DB `0` only, and string values only. Unsupported databases or value types fail fast during startup.

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

### Phase 5: Pub/Sub & Security — **Partial**

#### Pub/Sub Engine (`SUBSCRIBE`, `PUBLISH`) — **Not done**

- [ ] Maintain a global `map[string][]net.Conn` mapping channel names to active client sockets
- [ ] When a client issues `SUBSCRIBE`, flag their connection
- [ ] They can now only issue `PING`, `SUBSCRIBE`, and `UNSUBSCRIBE` commands
- [ ] `PUBLISH <channel> <msg>`:
  - [ ] Look up the channel in the map
  - [ ] Format a RESP Array (e.g., `*3\r\n$7\r\nmessage\r\n...`)
  - [ ] Write it to all matching client sockets

#### Authentication (`AUTH <password>`) — **Partial**

- [x] Allow passing a `--requirepass` flag on server startup
- [x] If set, all client connections initialize with `Authenticated = false`
- [ ] Reject all commands (return `-NOAUTH Authentication required.\r\n`) except `AUTH` and `PING` until `AUTH` is successfully called

## 4. Non-Functional Requirements (Production Readiness)

- [x] **High concurrency performance:** Avoid blocking the main accept loop. Lock contention is minimized, and `GET` uses `RLock()` / `RUnlock()` paths.
- [~] **Graceful shutdown:** Signal-driven shutdown, listener stop, client cleanup, and handler waiting are implemented; flushing state to `dump.rdb` is not.
- [~] **Observability:** `log/slog` structured logging is in place and logs connection / parser / response issues; replication is implemented, but dedicated replication-specific metrics/logging are still limited.

## 5. Out of Scope (For V1)

- Redis Cluster Mode (sharding / gossip protocol)
- Lua scripting execution (`EVAL`)
- Continuous AOF (Append Only File) writing
