-- =============================================================================
--  001_create_warehouse_schema.sql
--  India Weather & AQI ETL Pipeline — Data Warehouse DDL
--
--  This file is auto-executed by the warehouse-postgres container on first boot
--  because it is mounted into /docker-entrypoint-initdb.d/
--
--  Execution order matters — foreign keys require the referenced table to
--  exist first. Order: dims → facts → operational/audit tables
-- =============================================================================


-- -----------------------------------------------------------------------------
--  EXTENSIONS
-- -----------------------------------------------------------------------------
-- pg_trgm: enables trigram-based similarity searches (used for fuzzy city
-- matching during development/debugging via SQL; real matching is in Python)
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- =============================================================================
--  DIMENSION TABLES
--  Dimensions are the "who/what/where/when" of your data.
--  They change slowly and are looked up by the fact tables.
-- =============================================================================


-- -----------------------------------------------------------------------------
--  dim_city
--  One row per canonical Indian city.
--  "Canonical" means the single, cleaned, official version of the name.
--  e.g. "Mumbai" — not "Bombay", "MUMBAI", " Mumbai ".
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_city (
    city_id         SERIAL          PRIMARY KEY,

    -- The cleaned, official city name (e.g. "Mumbai")
    city_name       VARCHAR(100)    NOT NULL,

    state           VARCHAR(100)    NOT NULL,
    latitude        NUMERIC(9, 6)   NOT NULL,   -- e.g. 19.076000
    longitude       NUMERIC(9, 6)   NOT NULL,   -- e.g. 72.877700

    -- Tier 1 = metro, Tier 2 = large city, Tier 3 = smaller city
    tier            SMALLINT        CHECK (tier IN (1, 2, 3)),

    population      BIGINT,

    -- Soft-delete flag: set to FALSE if a city is removed from tracking
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,

    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- A city name + state combination should be unique
    CONSTRAINT uq_city_name_state UNIQUE (city_name, state)
);

COMMENT ON TABLE dim_city IS
    'Canonical city master. One row per tracked Indian city after cleaning.';
COMMENT ON COLUMN dim_city.tier IS
    '1 = Tier-1 metro, 2 = large city, 3 = smaller/emerging city.';


-- -----------------------------------------------------------------------------
--  dim_city_alias
--  Maps every messy/historical name variant back to the canonical city.
--
--  Why a separate table?  The raw API responses and the CSV contain names
--  like "Bombay", "DELHI", " Bangalore ".  We can't change those sources.
--  Instead, once we resolve a raw name → canonical city (either exactly or
--  via fuzzy match), we write that mapping here so future runs are instant
--  lookups instead of expensive fuzzy searches.
--
--  This is a "learned alias cache" — it grows automatically as the pipeline
--  encounters new name variants.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_city_alias (
    alias_id            SERIAL          PRIMARY KEY,

    -- The raw, uncleaned name as it appeared in the source data
    raw_alias           VARCHAR(200)    NOT NULL,

    -- Which canonical city does this alias map to?
    canonical_city_id   INT             NOT NULL
                        REFERENCES dim_city (city_id) ON DELETE CASCADE,

    -- How was this alias resolved?
    -- 'exact'   → stripped + lowercased, found directly in dim_city_alias
    -- 'fuzzy'   → rapidfuzz token-sort ratio >= 90 against dim_city.city_name
    -- 'manual'  → a human corrected it by hand
    match_method        VARCHAR(20)     NOT NULL
                        CHECK (match_method IN ('exact', 'fuzzy', 'manual')),

    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- The same raw alias should never map to two different cities
    CONSTRAINT uq_raw_alias UNIQUE (raw_alias)
);

COMMENT ON TABLE dim_city_alias IS
    'Alias/variant → canonical city mapping. Acts as a learned lookup cache.';
COMMENT ON COLUMN dim_city_alias.match_method IS
    'How the alias was resolved: exact lookup, rapidfuzz fuzzy match, or manual correction.';


-- -----------------------------------------------------------------------------
--  dim_date
--  Pre-populated date spine — one row per calendar day.
--  Joining facts to this dimension enables easy "group by month/quarter/year"
--  queries without calling date-extraction functions on every row.
--
--  date_key is an integer in YYYYMMDD format (e.g. 20240926).
--  This is a classic data-warehouse pattern — integer keys join faster than
--  DATE/TIMESTAMP columns and are instantly human-readable.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT             PRIMARY KEY,    -- YYYYMMDD e.g. 20240926

    full_date       DATE            NOT NULL UNIQUE,

    day_of_week     SMALLINT        NOT NULL,       -- 0 = Sunday … 6 = Saturday
    day_name        VARCHAR(9)      NOT NULL,       -- 'Monday', 'Tuesday', ...
    day_of_month    SMALLINT        NOT NULL,       -- 1–31
    day_of_year     SMALLINT        NOT NULL,       -- 1–366

    week_of_year    SMALLINT        NOT NULL,       -- ISO week number 1–53

    month           SMALLINT        NOT NULL,       -- 1–12
    month_name      VARCHAR(9)      NOT NULL,       -- 'January', …

    quarter         SMALLINT        NOT NULL        -- 1–4
                    CHECK (quarter BETWEEN 1 AND 4),

    year            SMALLINT        NOT NULL,

    is_weekend      BOOLEAN         NOT NULL        -- TRUE for Sat/Sun
);

COMMENT ON TABLE dim_date IS
    'Pre-populated date spine used for time-based analytics without runtime date functions.';
COMMENT ON COLUMN dim_date.date_key IS
    'Integer surrogate key in YYYYMMDD format. Faster joins than DATE type.';

-- Populate dim_date for 2020-01-01 through 2030-12-31
-- generate_series creates one row per day; all other columns are derived
INSERT INTO dim_date (
    date_key, full_date,
    day_of_week, day_name, day_of_month, day_of_year,
    week_of_year,
    month, month_name,
    quarter, year,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT                         AS date_key,
    d::DATE                                              AS full_date,
    EXTRACT(DOW  FROM d)::SMALLINT                       AS day_of_week,
    TO_CHAR(d, 'Day')                                    AS day_name,
    EXTRACT(DAY  FROM d)::SMALLINT                       AS day_of_month,
    EXTRACT(DOY  FROM d)::SMALLINT                       AS day_of_year,
    EXTRACT(WEEK FROM d)::SMALLINT                       AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT                      AS month,
    TO_CHAR(d, 'Month')                                  AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT                    AS quarter,
    EXTRACT(YEAR FROM d)::SMALLINT                       AS year,
    EXTRACT(DOW FROM d) IN (0, 6)                        AS is_weekend
FROM generate_series(
    '2020-01-01'::TIMESTAMP,
    '2030-12-31'::TIMESTAMP,
    '1 day'::INTERVAL
) AS gs(d)
ON CONFLICT (date_key) DO NOTHING;   -- safe to re-run (idempotent)


-- =============================================================================
--  FACT TABLES
--  Facts are the measurements — numerical readings timestamped and linked
--  to dimension keys.  They are wide (many columns) and tall (many rows).
-- =============================================================================


-- -----------------------------------------------------------------------------
--  fact_weather_reading
--  One row = one weather snapshot for one city at one point in time.
--  Sources from Open-Meteo API, pulled every 3 hours.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_weather_reading (
    reading_id          BIGSERIAL       PRIMARY KEY,

    -- Foreign keys to dimensions
    city_id             INT             NOT NULL
                        REFERENCES dim_city (city_id) ON DELETE RESTRICT,
    date_key            INT             NOT NULL
                        REFERENCES dim_date (date_key) ON DELETE RESTRICT,

    -- When the reading was recorded at the source (API's own timestamp)
    reading_ts          TIMESTAMPTZ     NOT NULL,

    -- Measurements — all stored in SI / standard units
    temperature_c       NUMERIC(5, 2),              -- degrees Celsius
    humidity_pct        NUMERIC(5, 2),              -- 0–100 %
    precipitation_mm    NUMERIC(7, 2),              -- millimetres
    pressure_hpa        NUMERIC(7, 2),              -- hectopascals
    wind_speed_kmh      NUMERIC(6, 2),              -- km/h

    -- Audit: when we fetched this reading
    fetched_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Idempotency constraint: the same city + timestamp should never produce
    -- two rows.  ON CONFLICT (city_id, reading_ts) DO UPDATE in our loader
    -- makes every insert upsert-safe.
    CONSTRAINT uq_weather_city_ts UNIQUE (city_id, reading_ts)
);

COMMENT ON TABLE fact_weather_reading IS
    'Weather snapshots from Open-Meteo API. One row per city per reading timestamp.';
COMMENT ON COLUMN fact_weather_reading.reading_ts IS
    'Timestamp from the API source, stored in UTC (TIMESTAMPTZ handles tz conversion).';

-- Index to speed up time-range queries (most analytical queries filter by time)
CREATE INDEX IF NOT EXISTS idx_weather_city_ts
    ON fact_weather_reading (city_id, reading_ts DESC);

CREATE INDEX IF NOT EXISTS idx_weather_date_key
    ON fact_weather_reading (date_key);


-- -----------------------------------------------------------------------------
--  fact_air_quality_reading
--  One row = one AQI snapshot for one city at one point in time.
--  Sources from WAQI / aqicn.org API, pulled every 3 hours.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_air_quality_reading (
    reading_id          BIGSERIAL       PRIMARY KEY,

    city_id             INT             NOT NULL
                        REFERENCES dim_city (city_id) ON DELETE RESTRICT,
    date_key            INT             NOT NULL
                        REFERENCES dim_date (date_key) ON DELETE RESTRICT,

    reading_ts          TIMESTAMPTZ     NOT NULL,

    -- Overall AQI index (0 = good, 50 = moderate, 100 = unhealthy, 200+ = hazardous)
    aqi_value           SMALLINT,

    -- Individual pollutant concentrations (µg/m³ or ppb depending on pollutant)
    pm25                NUMERIC(7, 2),   -- Fine particulate matter
    pm10                NUMERIC(7, 2),   -- Coarse particulate matter
    no2                 NUMERIC(7, 2),   -- Nitrogen dioxide
    so2                 NUMERIC(7, 2),   -- Sulphur dioxide
    co                  NUMERIC(7, 2),   -- Carbon monoxide
    o3                  NUMERIC(7, 2),   -- Ozone

    -- The pollutant driving the overall AQI score (e.g. 'pm25', 'o3')
    dominant_pollutant  VARCHAR(10),

    fetched_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_aqi_city_ts UNIQUE (city_id, reading_ts)
);

COMMENT ON TABLE fact_air_quality_reading IS
    'Air quality snapshots from WAQI API. One row per city per reading timestamp.';

CREATE INDEX IF NOT EXISTS idx_aqi_city_ts
    ON fact_air_quality_reading (city_id, reading_ts DESC);

CREATE INDEX IF NOT EXISTS idx_aqi_date_key
    ON fact_air_quality_reading (date_key);


-- =============================================================================
--  OPERATIONAL / AUDIT TABLES
--  These are not part of the star schema for analytics — they exist for
--  pipeline observability, debugging, and data-quality tracking.
-- =============================================================================


-- -----------------------------------------------------------------------------
--  pipeline_run_log
--  One row per Airflow task execution.  Written at the end of every task so
--  we have a permanent audit trail independent of Airflow's own metadata DB.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          BIGSERIAL       PRIMARY KEY,

    -- Airflow identifiers so we can cross-reference the Airflow UI
    dag_id          VARCHAR(250)    NOT NULL,
    task_id         VARCHAR(250)    NOT NULL,
    logical_date    DATE            NOT NULL,   -- Airflow's "data interval" date

    started_at      TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,

    -- 'success', 'partial_success', 'failure'
    status          VARCHAR(20)     NOT NULL,

    -- Counters for monitoring dashboards
    rows_extracted  INT             DEFAULT 0,
    rows_loaded     INT             DEFAULT 0,
    rows_rejected   INT             DEFAULT 0,

    -- JSON list of city names that failed in this run (partial failure pattern)
    cities_failed   JSONB           DEFAULT '[]'::JSONB,

    error_message   TEXT
);

COMMENT ON TABLE pipeline_run_log IS
    'Permanent audit log of every Airflow task run, independent of Airflow metadata DB.';


-- -----------------------------------------------------------------------------
--  dq_check_results
--  One row per data-quality check per pipeline run.
--  Stores pass/fail + detail so we can track quality trends over time.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_check_results (
    id              BIGSERIAL       PRIMARY KEY,

    -- Link back to the pipeline run that triggered this check
    run_id          BIGINT          REFERENCES pipeline_run_log (run_id) ON DELETE CASCADE,

    -- e.g. 'range_temperature', 'null_critical_fields', 'freshness'
    check_name      VARCHAR(100)    NOT NULL,

    -- Which table was checked?
    table_name      VARCHAR(100),

    -- 'pass', 'warn', 'fail'
    status          VARCHAR(10)     NOT NULL
                    CHECK (status IN ('pass', 'warn', 'fail')),

    -- Free-form detail: e.g. "3 rows had temperature > 60°C"
    details         TEXT,

    checked_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE dq_check_results IS
    'Results of automated data quality checks. One row per check per pipeline run.';

CREATE INDEX IF NOT EXISTS idx_dq_run_id
    ON dq_check_results (run_id);


-- -----------------------------------------------------------------------------
--  quarantine_unmatched_city_names
--  When the city reconciliation algorithm cannot resolve a raw city name
--  (neither exact nor fuzzy match), it ends up here instead of being silently
--  dropped.  A human can then inspect, correct, and re-trigger the pipeline.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantine_unmatched_city_names (
    id                  BIGSERIAL       PRIMARY KEY,

    -- The raw name we couldn't resolve (e.g. "Navi Mumbai", "Dilli")
    raw_name            VARCHAR(200)    NOT NULL,

    -- Which source sent this name? 'open_meteo', 'waqi', 'csv'
    source              VARCHAR(50),

    first_seen_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- How many times has this unresolved name appeared?
    -- High count = priority to fix
    occurrence_count    INT             NOT NULL DEFAULT 1,

    -- Set to TRUE once a human manually maps it via dim_city_alias
    resolved            BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT uq_quarantine_raw_name UNIQUE (raw_name)
);

COMMENT ON TABLE quarantine_unmatched_city_names IS
    'Holding area for raw city names that could not be resolved to a canonical dim_city row.';


-- =============================================================================
--  SUMMARY
-- =============================================================================
--
--  Star Schema layout:
--
--       dim_date ──────────────────────────────┐
--                                              │
--       dim_city ──── dim_city_alias           │
--          │                          fact_weather_reading
--          └──────────────────────── fact_air_quality_reading
--
--  Audit tables (not in star schema):
--       pipeline_run_log ──── dq_check_results
--       quarantine_unmatched_city_names
--
-- =============================================================================
