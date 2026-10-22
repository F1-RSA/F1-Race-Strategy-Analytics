-- Average lap time by compound and tyre age: Bahrain Grand Prix 2024 (session_key 9472).
-- Valid laps only: has_lap_time, not a pit out lap, not an in-lap (a lap with a pit entry in raw.pit).
-- Out and in laps are much slower than racing laps and would dominate the averages.
--
-- Read with care: the fresh-tyre band of the SOFT compound is slower than its second band because
-- the first laps of a race are driven with a full tank and in traffic. Tyre age, fuel load and
-- track position are mixed in these averages; the next query compares stints instead.
SELECT
    compound,
    CASE WHEN tyre_age_laps < 10 THEN '00-09'
         WHEN tyre_age_laps < 20 THEN '10-19'
         ELSE '20+' END AS tyre_age_laps,
    count(*) AS laps,
    round(avg(lap_duration_s), 3) AS avg_lap_s,
    round(min(lap_duration_s), 3) AS best_lap_s
FROM core.laps_enriched AS l
WHERE session_key = 9472
  AND has_lap_time
  AND NOT is_pit_out_lap
  AND compound IN ('SOFT', 'MEDIUM', 'HARD')
  AND NOT EXISTS (
      SELECT 1 FROM raw.pit AS p
      WHERE p.session_key = l.session_key
        AND p.driver_number = l.driver_number
        AND (p.payload ->> 'lap_number')::int = l.lap_number)
GROUP BY compound, 2
ORDER BY compound, 2;

-- Result (run on 2026-10-21 with the sessions 9472, 9480 and 9488 loaded; MEDIUM was not used
-- in this race):
--
--  compound | tyre_age_laps | laps | avg_lap_s | best_lap_s
-- ----------+---------------+------+-----------+------------
--  HARD     | 00-09         |  333 |    96.623 |     94.090
--  HARD     | 10-19         |  328 |    96.885 |     94.853
--  HARD     | 20+           |   70 |    96.882 |     95.305
--  SOFT     | 00-09         |  208 |    98.051 |     92.608
--  SOFT     | 10-19         |  102 |    97.135 |     94.340
--  SOFT     | 20+           |    2 |    96.704 |     95.746
