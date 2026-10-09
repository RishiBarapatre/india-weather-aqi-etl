-- =============================================================================
--  002_analytics_queries.sql
--  India Weather & AQI ETL Pipeline — Example Analytical Queries
--
--  These queries demonstrate the value of the star schema.  Run them in
--  Adminer (http://localhost:8082) or any SQL client connected to
--  warehouse-postgres on port 5433.
--
--  Three queries, three different advanced SQL techniques:
--    Query 1 — Window function (rolling average)
--    Query 2 — Gaps-and-Islands pattern (consecutive streak detection)
--    Query 3 — RANK() window function (weekly city leaderboard)
-- =============================================================================


-- =============================================================================
--  QUERY 1 — 24-Hour Rolling Average Temperature per City
--
--  Technique: Window function with RANGE frame
--
--  Business question:
--    "What has the smoothed temperature trend been for each city over
--     the last 24 hours, reading by reading?"
--
--  Why a rolling average?
--    Raw readings fluctuate (30°C at noon, 22°C at 3am).
--    A 24h rolling average smooths those spikes and shows the true
--    underlying trend — useful for heatwave detection.
--
--  How the window function works:
--    OVER (
--        PARTITION BY city_id        ← reset the window per city
--        ORDER BY reading_ts         ← rows ordered chronologically
--        RANGE BETWEEN               ← frame defined by VALUE range, not row count
--            INTERVAL '24 hours' PRECEDING   ← include rows from 24h before current row
--            AND CURRENT ROW                 ← up to and including this row
--    )
--
--    RANGE vs ROWS:
--      ROWS BETWEEN 7 PRECEDING AND CURRENT ROW → always exactly 8 rows
--      RANGE BETWEEN '24 hours' PRECEDING AND CURRENT ROW → variable number of
--      rows depending on how many readings fall within that time window.
--      We use RANGE because readings might be missing for some hours.
-- =============================================================================

SELECT
    c.city_name,
    c.state,
    f.reading_ts,
    f.temperature_c                         AS raw_temperature_c,

    -- Rolling 24-hour average (variable number of readings per window)
    ROUND(
        AVG(f.temperature_c) OVER (
            PARTITION BY f.city_id
            ORDER BY f.reading_ts
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    )                                       AS rolling_24h_avg_temp_c,

    -- How many readings contributed to this window?
    COUNT(*) OVER (
        PARTITION BY f.city_id
        ORDER BY f.reading_ts
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
    )                                       AS readings_in_window

FROM fact_weather_reading   f
JOIN dim_city               c  ON f.city_id = c.city_id

-- Only show the last 7 days to keep the result manageable
WHERE f.reading_ts >= NOW() - INTERVAL '7 days'

ORDER BY c.city_name, f.reading_ts DESC;


-- =============================================================================
--  QUERY 2 — Unhealthy AQI Streaks (> 150 for 3+ Consecutive Readings)
--
--  Technique: Gaps-and-Islands pattern
--
--  Business question:
--    "Which cities had sustained periods of dangerously high AQI?
--     Show me streaks of 3 or more consecutive readings above 150."
--
--  AQI > 150 = "Unhealthy" on the US EPA scale (significant health risk).
--
--  The Gaps-and-Islands problem:
--    You have a sequence of readings.  You want to find consecutive runs
--    ("islands") where a condition is true, separated by gaps where it's false.
--
--    The trick:
--      rn  = row number within the city (across ALL readings)
--      rn2 = row number within the city (only among UNHEALTHY readings)
--
--      For a perfect streak:  (rn - rn2) stays CONSTANT.
--      The moment a normal reading breaks the streak, rn increments but
--      rn2 doesn't (because it skips non-unhealthy rows), so (rn - rn2)
--      changes → a new island starts.
--
--  Example for Mumbai:
--    reading  aqi   rn   rn2   (rn - rn2)   island group
--    14:00    80    1    -     -            (excluded — below 150)
--    17:00    160   2    1     1            ← island A starts
--    20:00    175   3    2     1            ← island A continues
--    23:00    190   4    3     1            ← island A continues
--    02:00    95    5    -     -            (gap — excluded)
--    05:00    210   6    4     2            ← island B starts
-- =============================================================================

WITH

-- Step 1: Number all readings and number only the unhealthy readings
aqi_numbered AS (
    SELECT
        city_id,
        reading_ts,
        aqi_value,

        -- Row number across ALL readings for this city
        ROW_NUMBER() OVER (
            PARTITION BY city_id
            ORDER BY reading_ts
        )                                                           AS rn,

        -- Row number only among UNHEALTHY readings for this city
        ROW_NUMBER() OVER (
            PARTITION BY city_id, (aqi_value > 150)::INT
            ORDER BY reading_ts
        )                                                           AS rn2

    FROM fact_air_quality_reading
    WHERE aqi_value IS NOT NULL
),

-- Step 2: Keep only unhealthy readings; (rn - rn2) is the island ID
unhealthy_only AS (
    SELECT
        city_id,
        reading_ts,
        aqi_value,
        (rn - rn2)                                                  AS island_id
    FROM aqi_numbered
    WHERE aqi_value > 150
),

-- Step 3: Collapse each island into one summary row
streaks AS (
    SELECT
        city_id,
        island_id,
        MIN(reading_ts)                                             AS streak_start,
        MAX(reading_ts)                                             AS streak_end,
        COUNT(*)                                                     AS consecutive_readings,
        ROUND(AVG(aqi_value)::NUMERIC, 1)                          AS avg_aqi_in_streak,
        MAX(aqi_value)                                              AS peak_aqi
    FROM unhealthy_only
    GROUP BY city_id, island_id
    -- Only keep streaks of 3 or more consecutive unhealthy readings
    HAVING COUNT(*) >= 3
)

-- Step 4: Join to dim_city for readable output
SELECT
    c.city_name,
    c.state,
    s.streak_start,
    s.streak_end,
    s.consecutive_readings,
    s.avg_aqi_in_streak,
    s.peak_aqi,

    -- How long did this streak last?
    AGE(s.streak_end, s.streak_start)                              AS streak_duration

FROM streaks          s
JOIN dim_city         c  ON s.city_id = c.city_id

ORDER BY s.consecutive_readings DESC, s.avg_aqi_in_streak DESC;


-- =============================================================================
--  QUERY 3 — City Weekly AQI Leaderboard (Worst to Best)
--
--  Technique: RANK() window function + dim_date join
--
--  Business question:
--    "For each week, which cities had the worst average AQI?
--     Show the top 5 most polluted cities per week."
--
--  Why dim_date makes this easy:
--    Without dim_date: GROUP BY EXTRACT(YEAR...), EXTRACT(WEEK...)
--    With dim_date:    GROUP BY d.year, d.week_of_year
--    The join also gives us the readable week context for free.
--
--  RANK() vs ROW_NUMBER() vs DENSE_RANK():
--    RANK()        → tied cities share the same rank; next rank skips (1,1,3)
--    DENSE_RANK()  → tied cities share the same rank; next rank doesn't skip (1,1,2)
--    ROW_NUMBER()  → no ties; every row gets a unique number (1,2,3)
--    We use RANK() because for a "top polluted" list, ties deserve the same rank.
-- =============================================================================

WITH

-- Step 1: Compute each city's average AQI per calendar week
weekly_city_aqi AS (
    SELECT
        f.city_id,
        d.year,
        d.week_of_year,
        -- First day of that ISO week (for display)
        MIN(d.full_date)                                            AS week_start_date,
        ROUND(AVG(f.aqi_value)::NUMERIC, 1)                        AS avg_aqi,
        COUNT(*)                                                     AS readings_count
    FROM fact_air_quality_reading   f
    JOIN dim_date                   d  ON f.date_key = d.date_key
    WHERE f.aqi_value IS NOT NULL
    GROUP BY f.city_id, d.year, d.week_of_year
),

-- Step 2: Rank cities within each week by average AQI (highest = worst = rank 1)
ranked AS (
    SELECT
        city_id,
        year,
        week_of_year,
        week_start_date,
        avg_aqi,
        readings_count,
        RANK() OVER (
            PARTITION BY year, week_of_year   -- rank resets each week
            ORDER BY avg_aqi DESC             -- highest AQI = rank 1 (worst)
        )                                                           AS rank_worst_aqi
    FROM weekly_city_aqi
)

-- Step 3: Filter to top 5 per week and join city names
SELECT
    r.year,
    r.week_of_year,
    r.week_start_date,
    r.rank_worst_aqi,
    c.city_name,
    c.state,
    r.avg_aqi,
    r.readings_count,

    -- AQI category label for readability
    CASE
        WHEN r.avg_aqi <= 50  THEN 'Good'
        WHEN r.avg_aqi <= 100 THEN 'Moderate'
        WHEN r.avg_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN r.avg_aqi <= 200 THEN 'Unhealthy'
        WHEN r.avg_aqi <= 300 THEN 'Very Unhealthy'
        ELSE                       'Hazardous'
    END                                                             AS aqi_category

FROM ranked         r
JOIN dim_city       c  ON r.city_id = c.city_id

-- Only top 5 per week
WHERE r.rank_worst_aqi <= 5

ORDER BY r.year DESC, r.week_of_year DESC, r.rank_worst_aqi;
