# Data model (local PostgreSQL)

The schema is created by the scripts in `db/init/` on the first start with an empty
volume (`01_schemas.sql`, `02_raw_tables.sql`, `03_meta.sql`, `04_raw_jolpica.sql`). Postgres
runs them only for an empty volume, so **a database created before a script was added does not
get its tables** (for example the Jolpica tables of `04_raw_jolpica.sql`). To recreate it, which
deletes all data: `docker compose down -v && docker compose up -d --build`.

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
| `raw.jolpica_races` | `season, round` | race of the Jolpica season schedule |
| `raw.jolpica_results` | `season, round, driver_id` | result of a driver in a race |

`meeting_key` is also stored as a column (not part of the key) on every OpenF1 table except
`raw.meetings`, so that a whole weekend can be selected without parsing JSON. The Jolpica tables
have no `meeting_key`: Jolpica identifies a race by `season` and `round`, both sent as strings and
stored as integers. Mapping Jolpica races to OpenF1 meetings is left to the transformation.

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

### Jolpica tables

- **`raw.jolpica_races`** keeps the race object of `/{season}/races.json` unchanged (circuit,
  date, time, session dates), one row per `(season, round)`.
- **`raw.jolpica_results`** has one row per result. Its payload is the race object of
  `/{season}/results.json` with `Results` reduced to that one result, so the row keeps the API
  shape (`Driver`, `Constructor`, `grid`, `position`, `points`, `status`, `Time`, `FastestLap`)
  and the race it belongs to. All values stay strings, for example `Time.time` (`"1:31:44.742"`)
  and `points` (`"26"`).
- **Why `driver_id` and not `position` in the key:** a driver finishes a race at most once, in
  the four seasons checked on 2026-10-21 (2023 to 2026, 1772 results; 2026 had 17 of 23 rounds) no
  `(season, round, driverId)` and no `(season, round, position)` occurs twice. `driverId` never changes. A position can change after a
  post-race penalty, and the corrected result would then arrive as a second row, the same trap as
  `raw.pit`. If a duplicate key ever appears in a response, the load fails instead of merging rows.
  Shared drives (one car, two drivers) belong to the early years of the championship, not to the seasons we load.
- **Rows per race:** about 20 (10 teams of 2) until 2025 and 22 in 2026 (11 teams), but not exactly:
  a driver who did not start is missing (2024 round 3 has 19 rows), so
  `races x drivers` is only a plausibility check.

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
| `session_key` | Session loaded; NULL for sources and endpoints that are not per session: Jolpica (see below) and `meetings`. `sessions` is logged per session, because a session counts as loaded only with all five of its endpoints |
| `status` | `running`, `success` or `failed` (enforced by a check constraint) |
| `row_count` | Rows written, set when the attempt ends |
| `started_at`, `finished_at` | Attempt timing |

**Watermark rule:** a session counts as loaded when every endpoint has a `success`
row for it. An incremental run takes the sessions the API lists, minus the loaded ones.
A session that was interrupted has `running` or `failed` rows only and is picked up
again on the next run. A row is inserted as `running` and updated when the attempt ends;
a new attempt gets a new row, so the history of attempts is kept. Data and the `success` update are committed in one transaction, so a `success` row proves the rows are there. A killed run leaves a `running` row; at the start of the next run, `running` rows of earlier runs are set to `failed` (one run at a time is assumed). The index on `(source, endpoint, session_key, status)` serves this lookup.

**Jolpica rows:** Jolpica has no `session_key`, and the table stays as M3 made it. Rows with
`source = 'jolpica'` have `session_key` NULL, and the unit of work is part of the `endpoint`
text: `races:<season>` (schedule), `results:<season>` (the paginated fetch of the season) and
`results:<season>:<round>`, which is written in the same transaction as the rows of that round.
A round counts as loaded when `results:<season>:<round>` has a `success` row. The OpenF1 lookup
only reads `source = 'openf1'` rows with a `session_key`, so the two sources do not see each
other. Queries over the whole table that group by `session_key` must filter on `source`.
The cleaner design, nullable `season` and `round` columns, changes this table and is left for
a later milestone.
