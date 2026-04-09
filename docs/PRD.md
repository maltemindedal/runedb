# Product Requirements Document (PRD): Godis

- **Status:** Draft
- **Target Language:** Go (Golang)
- **Primary Focus:** Concurrency, low-level networking (TCP), custom protocols (RESP), and distributed systems
- **Description:** A high-performance, concurrent Redis-compatible key-value store built from scratch

## 1. Executive Summary

This project aims to build **Godis**, a production-ready, highly concurrent, in-memory key-value data store from scratch, fully compliant with the **Redis Serialization Protocol (RESP)**.

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

### Phase 1: Foundation & The Network Layer

Establish the core TCP loop, memory store, and parser.

#### Networking (TCP Loop)

- Implement `net.Listen("tcp", ":6379")`
- Use an infinite `for` loop calling `Accept()`
- For each incoming connection, spawn a new goroutine: `go handleConnection(conn)`
- Maintain a thread-safe registry of active client connections to handle clean disconnects

#### Protocol Parsing (RESP)

- Implement a parser that reads from the TCP socket using `bufio.Reader`
- Parse RESP data types:
  - Simple Strings (`+`)
  - Errors (`-`)
  - Integers (`:`)
  - Bulk Strings (`$`)
  - Arrays (`*`)
- Crucial: Handle fragmented TCP packets (e.g., a bulk string might arrive in multiple TCP segments)
- Rely on the lengths specified in the RESP protocol (`$<length>\r\n`), not network frame sizes

#### Basic Commands (`PING`, `ECHO`)

- `PING`: Return `+PONG\r\n` (Simple String)
- `ECHO <msg>`: Return the exact argument as a Bulk String: `$<len>\r\n<msg>\r\n`

#### Core Storage Engine (`SET`, `GET`)

- Create a global state struct containing a `map[string]Value` and a `sync.RWMutex`
- `Value` should be a struct containing:
  - `Data ([]byte)`
  - `ExpiresAt (int64)`
- `GET`:
  - Acquire `RLock()`
  - Read map
  - Release `RUnlock()`
- `SET`:
  - Acquire `Lock()`
  - Write map
  - Release `Unlock()`

#### State Management (Key Expiry / TTL)

- Implement `PX` (milliseconds) and `EX` (seconds) arguments for `SET`
- **Passive eviction:** On `GET`, check if `time.Now().UnixMilli() > ExpiresAt`; if true, delete the key and return `nil`
- **Active eviction:** Create a background goroutine that wakes up every `100ms`, samples a batch of keys with TTLs, and deletes expired keys to prevent memory bloat from unread keys

### Phase 2: Advanced Data Structures

Expand the data model, requiring specific memory layouts for performance.

#### Lists (`LPUSH`, `RPUSH`, `LRANGE`)

- Values in the store must support different types (`interface{}` or tagged unions)
- Use a Go slice `[][]byte` for the list
- For blocking commands (`BLPOP`), implement a pub/sub-like channel system where the client's goroutine pauses (`select` on a Go channel) until a background process or another client `PUSH`es to that key

#### Sorted Sets (`ZADD`, `ZRANGE`)

Implementing this efficiently is complex. You need a composite data structure:

- A `map[string]float64` for $O(1)$ lookups of a member's score
- A Skip List (or a balanced Binary Search Tree) for $O(\log N)$ insertions and range queries based on scores

#### Streams (`XADD`, `XREAD`)

- IDs are formatted as `<millisecondsTime>-<sequenceNumber>`
- Implement auto-generation of IDs (e.g., `XADD mystream * value`)
- Handle the edge case where multiple commands arrive in the same millisecond by incrementing the sequence number
- Store stream entries in an append-only slice or Radix tree

### Phase 3: ACID Transactions & Concurrency Control

Handle complex, multi-step operations safely.

#### Basic Atomicity (`INCR`)

- Read the value, parse it as a base-10 integer, increment, convert back to bytes, and save
- Wrap this entire read-modify-write cycle in a single write `Lock()` to prevent race conditions

#### Transaction Queueing (`MULTI`, `EXEC`, `DISCARD`)

- Add state to the client connection struct:
  - `InTransaction (bool)`
  - `TxQueue ([]Command)`
- When `MULTI` is received:
  - Set `InTransaction = true`
  - Return `+OK\r\n`
- Subsequent commands are parsed but **not** executed:
  - Append them to `TxQueue`
  - Return `+QUEUED\r\n`
- On `EXEC`:
  - Iterate through `TxQueue`
  - Execute all commands sequentially
  - Return a RESP Array of their results

#### Optimistic Locking (`WATCH`)

- Create a global `map[string][]net.Conn` mapping keys to watching clients
- Whenever a key is modified (e.g., via `SET`), check this map
- If a watching client is found, mark their connection state `TxFailed = true`
- When a client calls `EXEC`, if `TxFailed` is `true`, abort the transaction and return a Null Array: `*-1\r\n`

### Phase 4: Distributed Systems & High Availability

Implement the Redis replication protocol.

#### RDB Persistence (Disk I/O)

- Parse the `.rdb` binary format on server startup
- Read the 9-byte magic string `REDIS0011`
- Handle database selectors (`0xFE`) and expiry metadata opcodes (`0xFD`, `0xFC`)
- Load the keys into the in-memory map before accepting TCP connections

#### Master / Replica Handshake

- When acting as a **Replica**:
  - Connect to the Master's TCP port
  - Send `PING`, `REPLCONF listening-port <port>`, and `PSYNC ? -1`
- When acting as a **Master**:
  - Respond to `PSYNC` with `+FULLRESYNC <master_replid> 0\r\n`
  - Send an empty RDB file over the socket as a heavily formatted Bulk String

#### Command Propagation

- The Master must maintain a list of active Replica TCP connections
- Whenever a state-mutating command (`SET`, `DEL`) is executed successfully, the Master encodes it back into RESP and writes it to every Replica's network socket

#### Synchronization (`WAIT <numreplicas> <timeout>`)

- Master must track the replication offset (number of bytes of commands sent)
- Master pauses the client's goroutine
- Master sends `REPLCONF GETACK *` to replicas
- Replicas reply with `REPLCONF ACK <offset>`
- Master resumes the client when `<numreplicas>` have reported an offset greater than or equal to the Master's offset, or when `<timeout>` is reached

### Phase 5: Pub/Sub & Security

#### Pub/Sub Engine (`SUBSCRIBE`, `PUBLISH`)

- Maintain a global `map[string][]net.Conn` mapping channel names to active client sockets
- When a client issues `SUBSCRIBE`, flag their connection
- They can now only issue `PING`, `SUBSCRIBE`, and `UNSUBSCRIBE` commands
- `PUBLISH <channel> <msg>`:
  - Look up the channel in the map
  - Format a RESP Array (e.g., `*3\r\n$7\r\nmessage\r\n...`)
  - Write it to all matching client sockets

#### Authentication (`AUTH <password>`)

- Allow passing a `--requirepass` flag on server startup
- If set, all client connections initialize with `Authenticated = false`
- Reject all commands (return `-NOAUTH Authentication required.\r\n`) except `AUTH` and `PING` until `AUTH` is successfully called

## 4. Non-Functional Requirements (Production Readiness)

- **High concurrency performance:** Avoid blocking the main accept loop. Lock contention must be minimized. Use `RLock()` instead of `Lock()` for `GET` operations
- **Graceful shutdown:** Use `os/signal.Notify(chan, syscall.SIGINT, syscall.SIGTERM)`. Upon signal, stop accepting new connections, wait for active `TxQueue`s to finish `EXEC`, flush the state to `dump.rdb` (if persistence is enabled), and exit
- **Observability:** Use `log/slog` for structured logging. Log connection drops, replication syncs, and parsing errors

## 5. Out of Scope (For V1)

- Redis Cluster Mode (sharding / gossip protocol)
- Lua scripting execution (`EVAL`)
- Continuous AOF (Append Only File) writing
