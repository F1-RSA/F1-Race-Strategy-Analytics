-- One row per lap of raw.laps, with the tyre it was driven on. /laps has no compound, so each lap
-- is looked up in /stints by session, driver and lap range. Typed columns instead of JSON.
--
-- Grain: (session_key, driver_number, lap_number), the primary key of raw.laps.
--
-- Overlaps: OpenF1 often lists the lap of a pit stop in both stints (stint 1 ends at lap N, stint 2
-- starts at lap N; the pit entry is on lap N, the out lap is N + 1). A plain BETWEEN join would
-- return that lap twice. A lap that several stints cover belongs to the EARLIEST stint: it is the
-- in-lap, driven on the old tyres. LIMIT 1 per lap keeps the grain without DISTINCT.
--
-- Gaps: a lap that no stint covers (retirement lap, stint without lap_start/lap_end) stays in
-- the view with NULL compound, stint_number and tyre_age_laps. Nothing is guessed.

CREATE OR REPLACE VIEW core.laps_enriched AS
SELECT
    l.session_key,
    l.driver_number,
    l.lap_number,
    l.meeting_key,
    (l.payload ->> 'lap_duration')::numeric AS lap_duration_s,
    s.compound,
    s.stint_number,
    s.tyre_age_at_start + (l.lap_number - s.lap_start) AS tyre_age_laps,
    (l.payload ->> 'is_pit_out_lap')::boolean AS is_pit_out_lap,
    (l.payload ->> 'lap_duration') IS NOT NULL AS has_lap_time
FROM raw.laps AS l
LEFT JOIN LATERAL (
    SELECT stint_number, compound, lap_start, tyre_age_at_start
    FROM (
        SELECT
            st.stint_number,
            st.payload ->> 'compound' AS compound,
            (st.payload ->> 'lap_start')::integer AS lap_start,
            (st.payload ->> 'lap_end')::integer AS lap_end,
            (st.payload ->> 'tyre_age_at_start')::integer AS tyre_age_at_start
        FROM raw.stints AS st
        WHERE st.session_key = l.session_key
          AND st.driver_number = l.driver_number
    ) AS typed
    WHERE l.lap_number BETWEEN typed.lap_start AND typed.lap_end
    ORDER BY stint_number
    LIMIT 1
) AS s ON true;
