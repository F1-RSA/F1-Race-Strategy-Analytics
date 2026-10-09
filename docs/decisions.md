# Decision log

One entry per work package. Purpose: explain every design choice without notes
(oral defence) and feed the architecture changelog (v0.1 -> v0.2 -> final).

## Template

### <Package ID> · <Title> (<YYYY-MM-DD>)

- **Decision:** what was decided.
- **Alternatives:** what else was considered and why it was rejected.
- **Failure and rerun behaviour:** what happens on error, what happens on a second run.
- **Known limits:** what this does not cover.

## Entries

### M1 · Docker base setup (2026-10-08)

- **Decision:** Two services on one user-defined bridge network (`f1_network`): `db` (PostgreSQL 16.10) and `ingestion` (Python 3.11.14, slim). The ingestion service waits for the database via `depends_on` with `condition: service_healthy`. Both images are pinned to exact versions. The database port is bound to `127.0.0.1` only. Configuration comes from `.env` (copied from `.env.example`, whose defaults work unchanged). The ingestion container is a placeholder (`sleep infinity`) until the ingestion package exists, so that `docker compose exec ingestion ...` works.
- **Alternatives:** `postgres:16` (floating tag) is not reproducible. Publishing the port on all interfaces exposes the database on the local network for no benefit. A one-shot ingestion container that exits was rejected because Kestra and the README both need to run commands inside a running container. Plain `depends_on` without a healthcheck only waits for the container to start, not for Postgres to accept connections.
- **Failure and rerun behaviour:** If Postgres is not healthy, the ingestion container is not started. The data lives in the named volume `pgdata` and survives `docker compose down`; `docker compose down -v` deletes it for a fresh start. Containers restart automatically unless stopped. Containers reach each other by service name (`db`) through Docker's DNS on `f1_network`; without a shared network the hostname `db` would not resolve.
- **Known limits:** The Postgres password in `.env.example` is a local-development value and must not be reused elsewhere. The init SQL (schemas and tables) is not part of this package (M3). A fixed `container_name` prevents running two copies of the stack on one host.
