# Securing a server

Stash defaults to a safe posture: it binds loopback and accepts no password. Both of those change the moment you make the server reachable from the network.

## The default binding

`--host` defaults to `127.0.0.1`, so a server started with no flags accepts connections only from the local machine. This is deliberate — an out-of-the-box Stash has no authentication, and binding a public interface without a password exposes an unauthenticated datastore.

## Require a password

Set `--requirepass` and clients must authenticate before issuing commands:

```bash
go run ./cmd/stash --port 6379 --requirepass "$STASH_PASSWORD"
```

```
127.0.0.1:6379> GET key
(error) NOAUTH Authentication required.
127.0.0.1:6379> AUTH secret
OK
127.0.0.1:6379> GET key
(nil)
```

`AUTH` takes exactly one argument — the password form only. There are no usernames or ACLs.

Unauthenticated clients may still issue `PING`. Every other command, including the replication handshake, requires authentication first.

## Bind another interface

Only after setting a password:

```bash
go run ./cmd/stash --host 0.0.0.0 --port 6379 --requirepass "$STASH_PASSWORD"
```

An empty `--host` also binds all interfaces.

## Authenticate replicas

A replica of a protected master needs `--masterauth` to complete its handshake:

```bash
go run ./cmd/stash --port 6380 --replicaof 127.0.0.1:6379 --masterauth "$STASH_PASSWORD"
```

See [Setting up replication](replication.md).

## Cap concurrent connections

`--maxclients` bounds concurrent client connections and defaults to `10000`. Setting it to `0` removes the limit.

```bash
go run ./cmd/stash --maxclients 1000
```

## What the slowlog stores

`SLOWLOG` redacts `AUTH` arguments before recording command metadata, so passwords do not leak into the slow query log. Other command arguments are stored verbatim — anything you treat as a secret and pass as a command argument can appear in `SLOWLOG GET` output and in `MONITOR` streams.

## Known limitations

These are properties of the current implementation, not configuration mistakes:

- **No TLS.** All traffic, including `AUTH` passwords, crosses the network in plaintext. Run Stash on a trusted network or behind a TLS-terminating proxy.
- **No ACLs or users.** A single shared password grants full command access.
- **No rename-command or command-level restrictions.** Any authenticated client can issue any implemented command, including `MONITOR` and `BGREWRITEAOF`.

## Related

- [Configuration reference](../reference/configuration.md)
- [Observability](observability.md) — what `MONITOR` and `SLOWLOG` expose
