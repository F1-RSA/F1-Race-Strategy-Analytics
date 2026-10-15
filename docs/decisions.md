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

### M3 · PostgreSQL schema (2026-10-15)

- **Decision:** The init scripts in `db/init/` (mounted read-only into `/docker-entrypoint-initdb.d`) create three schemas: `raw`, `core`, `meta`. Each raw table (`meetings`, `sessions`, `drivers`, `laps`, `stints`, `pit`) has the natural key from the OpenF1 API as primary key, `payload JSONB` with the original record, and `_ingested_at` / `_run_id`. `meta.load_log` records every load attempt per endpoint and session and is the basis for incremental loads. Keys and their reasons are in `docs/data_model.md`.
- **Alternatives:** Fully typed raw columns were rejected because an API change (such as the pit duration fields) would break the load; typing happens in `core`. A surrogate key per row was rejected because it cannot make upserts idempotent. For `raw.pit` the key `(session_key, driver_number, lap_number)` was rejected because a second pit lane entry on the same lap would be overwritten silently; the entry timestamp is used instead. A single status column on the session was rejected in favour of one log row per endpoint, because a session is only complete when all endpoints succeeded.
- **Failure and rerun behaviour:** The scripts use `IF NOT EXISTS` and run only on an empty volume; changing them later needs `docker compose down -v` (data is lost) or a manual migration. Reruns of a load upsert on the primary key and change no row counts. An interrupted load leaves `running` or `failed` rows in `meta.load_log` and is repeated on the next incremental run.
- **Known limits:** No Jolpica tables yet (M5), no `weather` or `race_control` (planned after the midterm). Without a foreign key, a lap can exist without its session row; load order is the responsibility of the ingestion. If OpenF1 corrects a pit timestamp, the old and new row both stay. No migration tool.
