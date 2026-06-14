# lndg-blitz-go

Go port of [lndg-blitz](https://github.com/warioishere/lndg-blitz).

---

## Requirements

- **Go 1.25+**
- **PostgreSQL** (local or remote)
- **LND** with a reachable gRPC port and a readable `tls.cert` and (admin) macaroon

## Build

```sh
# from the repo root
go build ./...

# or build individual binaries
go build -o bin/migrate    ./cmd/migrate
go build -o bin/web        ./cmd/web
go build -o bin/controller ./cmd/controller
```

## Binaries

| Binary | Purpose |
|---|---|
| `cmd/migrate` | apply the DB schema |
| `cmd/web` | HTTP server (dashboard, REST API, all pages/forms) |
| `cmd/controller` | process supervisor (starts jobs + rebalancer + htlc_stream + graph_watcher) |
| `cmd/jobs` | data sync daemon — standalone |
| `cmd/rebalancer` | rebalancer daemon — standalone |
| `cmd/htlcstream` | HTLC stream daemon — standalone |
| `cmd/graphwatcher` | graph watcher daemon — standalone |

For normal operation you only need **`cmd/web`** + **`cmd/controller`**.

---

## Configuration

Everything is configured via **environment variables**. Optionally, a `lndg.conf` file in
the working directory (or the path in `LNDG_CONFIG_FILE`) may contain `KEY=VALUE` lines;
`#` lines and blank lines are ignored. **Environment variables take precedence over the
file.**

| env var | default | meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://lndg:lndg@localhost:5432/lndg?sslmode=disable` | Postgres DSN (used by all binaries) |
| `LND_RPC_SERVER` | `localhost:10009` | LND gRPC `host:port` |
| `LND_TLS_PATH` | `~/.lnd/tls.cert` | LND TLS certificate (`~` is expanded) |
| `LND_MACAROON_PATH` | `~/.lnd/data/chain/bitcoin/mainnet/admin.macaroon` | macaroon with sufficient permissions |
| `LND_NETWORK` | `mainnet` | `mainnet` or `testnet` (controls e.g. mempool links) |
| `LND_MAX_MESSAGE` | `35` | max gRPC message size in MB |
| `LND_DATABASE_PATH` | `~/.lnd/data/graph/mainnet/channel.db` | path to LND's `channel.db` (for DB-size display only) |
| `WEB_BIND_ADDR` | `0.0.0.0:8889` | bind address of the HTTP server |
| `WEB_BASIC_AUTH_USER` | _(empty)_ | when **both** auth vars are set, HTTP basic auth is enabled |
| `WEB_BASIC_AUTH_PASS` | _(empty)_ | |
| `LOGIN_REQUIRED` | `false` | gate for the auth middleware |
| `DEBUG` | `true` | debug flag |

Example `lndg.conf`:

```ini
DATABASE_URL=postgres://lndg:secret@localhost:5432/lndg?sslmode=disable
LND_RPC_SERVER=10.0.0.5:10009
LND_TLS_PATH=/home/bitcoin/.lnd/tls.cert
LND_MACAROON_PATH=/home/bitcoin/.lnd/data/chain/bitcoin/mainnet/admin.macaroon
LND_NETWORK=mainnet
WEB_BASIC_AUTH_USER=admin
WEB_BASIC_AUTH_PASS=changeme
```

---

## Setup / Run

### 1. Create DB and apply schema

Create a Postgres role and database (names/password must match `DATABASE_URL`):

```sql
CREATE ROLE lndg WITH LOGIN PASSWORD 'secret';
CREATE DATABASE lndg OWNER lndg;
```

Apply the schema:

```sh
DATABASE_URL='postgres://lndg:secret@localhost:5432/lndg?sslmode=disable' \
  go run ./cmd/migrate
# -> migrate: schema up to date
```

### 2. Connect LND

Set `LND_RPC_SERVER`, `LND_TLS_PATH`, and `LND_MACAROON_PATH` to point at your LND node.
The process must be able to read the cert and macaroon files. The `admin.macaroon` is
required for fee updates, rebalances, channel open/close, etc.

The connection is lazy: if LND is down, `cmd/web` still starts and RPC calls fail
individually on access.

### 3. Run

```sh
# background daemons (jobs, rebalancer, htlc_stream, graph_watcher)
go run ./cmd/controller

# web UI + REST API (separate process)
go run ./cmd/web
# -> web: listening on 0.0.0.0:8889
```

Open the dashboard at **http://localhost:8889/** (or the value of `WEB_BIND_ADDR`).

> For production, build the binaries with `go build` and run them as two systemd services
> (`web` + `controller`). If exposed publicly, set `WEB_BASIC_AUTH_USER`/`WEB_BASIC_AUTH_PASS`
> and place a TLS reverse proxy in front.

---

## Tests

```sh
go test ./...          # integration tests require Docker (testcontainers Postgres)
go test -short ./...   # skips container tests
```
