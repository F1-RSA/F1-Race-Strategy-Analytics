# Jolpica fixtures

Recorded once from the live API on 2026-10-21, **unmodified** (the bytes the API sent):

| File | Request | Rows |
|------|---------|------|
| `races_2024.json` | `GET https://api.jolpi.ca/ergast/f1/2024/races.json?limit=100` | 24 races |
| `results_2024_round_1.json` | `.../2024/1/results.json?limit=100` | 20 results |
| `results_2024_round_2.json` | `.../2024/2/results.json?limit=100` | 20 results |
| `results_2024_round_3.json` | `.../2024/3/results.json?limit=100` | 19 results |

The three rounds (59 result rows, in one race fewer than 20 drivers) are the building blocks for the
season endpoint. `tests/test_jolpica.py` serves `.../2024/results.json?limit=&offset=` from them the
way the real API answers (checked live for the full 2024 season, 479 rows):

- `MRData.total` counts result **rows**, not races, so a page border can fall inside a race
  (in 2024: rounds 6, 11, 16 and 21 are split over two pages at `limit=100`).
- The race object, everything except `Results`, is the same on both pages. The race objects in
  these per-round files are identical to the ones in the season response.
- `limit` is capped at 100 and the capped value is echoed in `MRData.limit`.
- `MRData` values are strings. An unknown or empty season answers 200 with `total` 0 and no races.

With a page size of 25 the 59 rows give three pages, and the first border falls inside round 2.
