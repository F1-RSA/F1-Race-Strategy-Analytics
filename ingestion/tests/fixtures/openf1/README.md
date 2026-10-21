# OpenF1 fixtures

Recorded once from the live API on 2026-10-19 for session `9472` (2024 Bahrain Grand Prix,
Race), one request per endpoint: `GET https://api.openf1.org/v1/<endpoint>?session_key=9472`.

| File | Records (live) | Records (fixture) |
|------|----------------|-------------------|
| `session_9472/sessions.json` | 1 | 1 |
| `session_9472/drivers.json` | 20 | 20 |
| `session_9472/laps.json` | 1129 | 114 |
| `session_9472/stints.json` | 63 | 63 |
| `session_9472/pit.json` | 43 | 43 |
| `no_results.json` | – | body of the 404 OpenF1 returns for an empty result |

Reduction: `laps` keeps only drivers 1 and 11 (564 KB → 63 KB). All other endpoints are complete.
Records are re-serialised one per line for readable diffs; field names, key order and values
are unchanged (including the deprecated `pit_duration` next to `lane_duration`/`stop_duration`).
