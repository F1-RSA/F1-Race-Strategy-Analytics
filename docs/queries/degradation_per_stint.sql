-- Tyre degradation per stint: slope of the lap time over the tyre age (seconds per lap of age).
-- Valid laps only (has_lap_time, not a pit out lap, not an in-lap), stints with at least 12 such
-- laps. Positive = the tyre gets slower with age. Bahrain Grand Prix 2024 (session_key 9472).
-- The slope understates the degradation: the car also gets lighter with burnt fuel and gets
-- faster for that, which hides part of it. The figures are not corrected for fuel.
SELECT
    d.name_acronym AS driver,
    l.stint_number,
    l.compound,
    count(*) AS laps,
    min(l.tyre_age_laps) AS first_age,
    max(l.tyre_age_laps) AS last_age,
    round(regr_slope(l.lap_duration_s, l.tyre_age_laps)::numeric, 3) AS degradation_s_per_lap
FROM core.laps_enriched AS l
JOIN LATERAL (SELECT payload ->> 'name_acronym' AS name_acronym
              FROM raw.drivers
              WHERE session_key = l.session_key AND driver_number = l.driver_number) AS d ON true
WHERE l.session_key = 9472
  AND l.has_lap_time
  AND NOT l.is_pit_out_lap
  AND l.compound IN ('SOFT', 'MEDIUM', 'HARD')
  AND NOT EXISTS (
      SELECT 1 FROM raw.pit AS p
      WHERE p.session_key = l.session_key
        AND p.driver_number = l.driver_number
        AND (p.payload ->> 'lap_number')::int = l.lap_number)
GROUP BY d.name_acronym, l.stint_number, l.compound
HAVING count(*) >= 12
ORDER BY degradation_s_per_lap DESC
LIMIT 8;

-- Result (run on 2026-10-21 with the sessions 9472, 9480 and 9488 loaded; the eight stints with
-- the largest slope):
--
--  driver | stint_number | compound | laps | first_age | last_age | degradation_s_per_lap
-- --------+--------------+----------+------+-----------+----------+-----------------------
--  SAR    |            4 | SOFT     |   14 |         1 |       14 |                 0.176
--  RIC    |            3 | SOFT     |   20 |         1 |       20 |                 0.123
--  BOT    |            2 | HARD     |   16 |         1 |       16 |                 0.099
--  MAG    |            3 | HARD     |   23 |         1 |       23 |                 0.087
--  GAS    |            4 | SOFT     |   12 |         1 |       12 |                 0.081
--  GAS    |            2 | HARD     |   17 |         1 |       17 |                 0.072
--  STR    |            2 | HARD     |   16 |         1 |       16 |                 0.071
--  PER    |            3 | SOFT     |   20 |         1 |       20 |                 0.067
