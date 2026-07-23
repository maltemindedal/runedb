# Memory limits and eviction

RuneDB has two independent eviction mechanisms: **TTL eviction**, which removes keys whose expiry has passed, and **memory-pressure eviction**, which removes live keys when the keyspace exceeds `--maxmemory`.

## TTL eviction

TTLs are stored internally as absolute Unix millisecond timestamps. Expired keys are removed two ways:

- **Passively**, when a read touches an expired key. The read behaves as though the key is gone.
- **Actively**, by a background loop that samples keys on a fixed interval.

Both are always on; there is no flag to disable them.

```bash
# Sample 50 keys every 250ms instead of the default 20 every 100ms
go run ./cmd/runedb --eviction-interval 250ms --eviction-sample-size 50
```

| Flag | Default | Effect |
| --- | --- | --- |
| `--eviction-interval` | `100ms` | Time between active eviction passes |
| `--eviction-sample-size` | `20` | Keys sampled per pass |

The active loop samples rather than scanning the full keyspace, so expired keys that are never read are removed probabilistically rather than immediately. A larger sample size reclaims memory sooner at the cost of more work per pass. A non-positive sample size falls back to the built-in default.

## Memory-pressure eviction

Set `--maxmemory` to a byte count to enable approximate keyspace accounting and LRU eviction:

```bash
# 100 MB limit
go run ./cmd/runedb --maxmemory 104857600
```

`0`, the default, disables the feature entirely and skips the accounting overhead. Negative values are rejected at startup.

When a write pushes the keyspace over the limit, the store evicts sampled least-recently-used candidates until usage is back at or below the limit. The limit is also enforced once at startup, after any AOF or RDB load — that pass logs:

```
level=INFO msg="applied startup maxmemory eviction" evicted_keys=42 used_memory=104857000 maxmemory=104857600
```

### Accounting is approximate

`used_memory` is the store's own estimate, built from per-entry overhead constants plus key and value byte lengths. It is **not** process RSS and will not match what the OS reports. Go runtime overhead, the RESP buffers, connection state, and the AOF write buffer are all outside this number.

Budget accordingly: set `--maxmemory` meaningfully below the memory you actually want the process to occupy, and watch `go_heap_sys` from `INFO memory` for the real figure.

### Watching it work

```bash
redis-cli -p 6379 INFO memory
```

```
used_memory:104857000
used_memory_human:100.00M
maxmemory:104857600
maxmemory_human:100.00M
```

`mem_fragmentation_ratio` in that output is a hardcoded placeholder, not a measurement. See [Observability](observability.md).

## Compact encodings

Small collections use compact internal encodings that store entries in a flat structure rather than a map, reducing per-key overhead for the common case:

| Type | Compact encoding | Promotes to a map when |
| --- | --- | --- |
| Hash | Flat entry slice | It outgrows the small-hash threshold |
| Sorted set | Flat entry slice | It outgrows the small-zset threshold |
| Set | Sorted `int64` slice (`IntSet`) | A non-integer member is added, or it passes 512 entries |

The 512-entry `IntSet` bound mirrors Redis' `set-max-intset-entries` default: past that size, O(n) inserts on a sorted slice cost more than the map. Promotion is automatic and one-way, with no configuration surface.

## Related

- [Configuration reference](../reference/configuration.md)
- [Observability](observability.md) — reading `INFO memory`
- [Architecture overview](../architecture/overview.md) — sharding and lock strategy
