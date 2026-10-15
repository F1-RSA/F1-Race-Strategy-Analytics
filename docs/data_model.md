# Data model (local PostgreSQL)

The schema is created by the scripts in `db/init/` on the first start with an empty
volume (`01_schemas.sql`, `02_raw_tables.sql`, `03_meta.sql`). To recreate it:
`docker compose down -v && docker compose up -d --build`.

## Schemas

| Schema | Purpose |
|--------|---------|
| `raw`  | API responses as loaded. Key columns plus the original JSON record. |
| `core` | Cleaned and joined tables built by transformations (starts with `core.laps_enriched`). Empty until then. |
| `meta` | Pipeline bookkeeping. |

## Raw tables

Every raw table has the key columns below, `payload JSONB` (the unchanged API record),
`_ingested_at` (write time) and `_run_id` (run that wrote the row). Writes are upserts
on the primary key, so a second run over the same data changes no row counts.
Keeping the full record in `payload` protects against API changes, for example the
pit endpoint replacing `pit_duration` by `lane_duration` and `stop_duration`.

| Table | Primary key | Grain: one row per |
|-------|-------------|--------------------|
| `raw.meetings` | `meeting_key` | race weekend |
| `raw.sessions` | `session_key` | session (race, sprint, qualifying, ...) |
| `raw.drivers`  | `session_key, driver_number` | driver in a session |
| `raw.laps`     | `session_key, driver_number, lap_number` | lap of a driver |
| `raw.stints`   | `session_key, driver_number, stint_number` | tyre stint of a driver |
| `raw.pit`      | `session_key, driver_number, date` | pit lane entry of a driver |

`meeting_key` is also stored as a column (not part of the key) on every table except
`raw.meetings`, so that a whole weekend can be selected without parsing JSON.

### Why these keys

- **meeting_key, session_key** are the identifiers OpenF1 assigns and uses to link all
  endpoints. They are globally unique, so no other column is needed.
- **driver_number** is only unique within a session: numbers change between seasons and
  a driver can use a different number later. It is always combined with `session_key`.
- **lap_number, stint_number** count from 1 within a driver and session. The OpenF1
  sample data (`docs/sample_data/`) has one row per combination.
- **raw.pit uses the entry timestamp instead of `lap_number`.** A pit stop has no number
  of its own. `(session_key, driver_number, lap_number)` would silently overwrite a
  second pit lane entry on the same lap (for example a drive-through penalty followed
  by a stop). With `date`, such a case stays visible as two rows. The risk is the
  opposite: if OpenF1 later corrects a timestamp, the correction arrives as a second row
  instead of replacing the first. Both cases are rare; the first loses data, the second
  can be found by a query, so we accept the second.

Lap and stint columns used for transformations (`lap_start`, `lap_end`, `compound`,
`tyre_age_at_start`, durations) are not columns here. They are read from `payload`
in the transformation step.

## Meta tables

`meta.load_log` has one row per load attempt of an endpoint for a session:

| Column | Meaning |
|--------|---------|
| `log_id` | Surrogate key (identity) |
| `run_id` | Run that made the attempt, same value as `_run_id` in the raw tables |
| `source`, `endpoint` | e.g. `openf1`, `laps` |
| `session_key` | Session loaded; NULL for endpoints that are not per session (`meetings`, `sessions`) |
| `status` | `running`, `success` or `failed` (enforced by a check constraint) |
| `row_count` | Rows written, set when the attempt ends |
| `started_at`, `finished_at` | Attempt timing |

**Watermark rule:** a session counts as loaded when every endpoint has a `success`
row for it. An incremental run takes the sessions the API lists, minus the loaded ones.
A session that was interrupted has `running` or `failed` rows only and is picked up
again on the next run. A row is inserted as `running` and updated when the attempt ends;
a new attempt gets a new row, so the history of attempts is kept. The index on `(source, endpoint, session_key, status)` serves this lookup.
