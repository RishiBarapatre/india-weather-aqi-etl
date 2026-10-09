# India Weather & Air Quality Index (AQI) ETL Pipeline

An end-to-end, production-grade Data Engineering pipeline that ingests, cleans, validates, and models weather and air quality data for Indian cities into a Star Schema data warehouse.

Built with Apache Airflow, MongoDB, PostgreSQL, Pandas, and Docker Compose.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Source Data and Cleaning Logic](#source-data-and-cleaning-logic)
- [Data Warehouse Schema](#data-warehouse-schema)
- [Automated Data Quality Framework](#automated-data-quality-framework)
- [Airflow DAG Architecture](#airflow-dag-architecture)
- [Repository Structure](#repository-structure)
- [Quick Start Guide](#quick-start-guide)
- [Testing](#testing)
- [Analytical Queries](#analytical-queries)

---

## Project Overview

Indian metropolitan areas experience significant seasonal variations in temperature alongside severe particulate matter (PM2.5, PM10) pollution events. Analyzing these environmental patterns requires joining data from multiple external sources:

- **Open-Meteo API**: High-frequency meteorological measurements (temperature, relative humidity, wind speed, precipitation, surface pressure).
- **WAQI (World Air Quality Index) API**: Station-level air quality observations (AQI index, dominant pollutant, PM2.5, PM10, NO2, SO2, CO, O3).
- **Legacy Administrative Datasets**: Dirty CSV records containing historical city names, inconsistent formatting, and coordinate inaccuracies.

This project builds an automated, fault-tolerant ETL pipeline that ingests data from these disparate APIs, stages raw responses in a document store (MongoDB), reconciles municipal naming discrepancies, enforces strict data quality gates, and loads the data into a normalized Star Schema data warehouse (PostgreSQL) optimized for analytical queries.

---

## Architecture

The platform follows a Medallion (Bronze-Silver-Gold) architecture:

```
                       +----------------------------------------------+
                       |               EXTERNAL APIS                  |
                       |  - Open-Meteo API (Multi-coordinate batched) |
                       |  - WAQI API (Per-city station endpoint)      |
                       +----------------------+-----------------------+
                                              |
                                              v
+---------------------------------------------------------------------------------------------+
| BRONZE ZONE / RAW LAKE (MongoDB: weather_aqi_raw)                                           |
| - raw_weather_readings   (verbatim JSON payloads, execution timestamps, run IDs)            |
| - raw_aqi_readings       (verbatim JSON payloads, station metadata, run IDs)                |
+---------------------------------------------+-----------------------------------------------+
                                              |
                                              v
+---------------------------------------------------------------------------------------------+
| SILVER ZONE / TRANSFORMATION & RECONCILIATION (Airflow / Pandas)                            |
| - Timestamp normalization (ISO-8601 UTC conversion)                                         |
| - Type coercion, outlier sanitization, safe numeric parsing                                 |
| - Multi-tier city resolution (exact match, alias lookup, fuzzy distance matching)          |
+---------------------------------------------+-----------------------------------------------+
                                              |
                                              v
+---------------------------------------------------------------------------------------------+
| GOLD ZONE / DATA WAREHOUSE (PostgreSQL: weather_aqi_dwh)                                    |
| - Dimensions : dim_city, dim_city_alias, dim_date (pre-populated 4,000+ dates)              |
| - Facts      : fact_weather_reading, fact_air_quality_reading                               |
| - Governance : quarantine_unmatched_city_names, dq_check_results, pipeline_run_log          |
+---------------------------------------------+-----------------------------------------------+
                                              |
                                              v
+---------------------------------------------------------------------------------------------+
| QUALITY ASSURANCE & ANALYTICS                                                               |
| - 10 Automated Data Quality Checks (atomic transaction, status logging in dq_check_results) |
| - Advanced Analytical SQL (Rolling 24h averages, Gaps-and-Islands streaks, RANK() rankings) |
+---------------------------------------------------------------------------------------------+
```

### Component Roles

| Service | Technology | Port | Purpose |
|---|---|---|---|
| Airflow Webserver | Apache Airflow 2.9.3 | 8080 | DAG monitoring, manual triggers, task log inspection |
| Airflow Scheduler | Apache Airflow 2.9.3 | -- | Task execution, dependency management, scheduling |
| Metadata DB | PostgreSQL 16 | 5432 (internal) | Airflow state, DAG runs, task instance metadata |
| Raw Landing Lake | MongoDB 7.0 | 27017 | Schema-agnostic storage of raw API JSON payloads |
| Mongo Express | Mongo Express 1.0.2 | 8081 | Web UI for inspecting MongoDB raw collections |
| Data Warehouse | PostgreSQL 16 | 5433 | Star Schema dimensional warehouse (facts + dimensions) |
| Adminer | Adminer 4.8.1 | 8082 | Web SQL client for querying the data warehouse |

---

## Key Engineering Decisions

### 1. Document Store for Raw Payloads (Bronze Zone)
External APIs frequently introduce non-breaking schema shifts (e.g., WAQI's known typo `dominentpol`, optional pollutant readings depending on local sensor availability). Storing raw responses verbatim in MongoDB prior to any transformation preserves the original source data for auditing, backfilling, and debugging without risking pipeline crashes due to unexpected JSON keys.

### 2. Airflow Dynamic Task Mapping
The WAQI API does not support multi-location batch queries; each station requires a separate HTTP request. Using Airflow's Dynamic Task Mapping (`extract_aqi_city.expand(city=...)`), the pipeline generates an isolated task instance for each city at runtime. If a single station endpoint times out or returns an error, only that city's task retries or fails, isolating partial failures from the rest of the pipeline.

### 3. Batched Network Requests for Weather
Open-Meteo allows multi-coordinate requests in a single GET query (comma-separated latitudes and longitudes). The weather extractor splits the target cities into batches of 10, reducing 35+ individual HTTP requests to just 4 network calls. This minimizes latency and reduces external API connection overhead.

### 4. Idempotent Warehouse Loading
All warehouse inserts utilize PostgreSQL's `ON CONFLICT (city_id, reading_ts) DO UPDATE` pattern. Re-running a DAG for any date or execution interval updates existing records in place without duplicating fact rows or corrupting analytical aggregates.

### 5. Multi-Tier City Name Reconciliation
To reconcile raw city names against canonical warehouse records, the pipeline applies a three-tier resolution strategy:
1. Exact matching against `dim_city.city_name`.
2. Normalized alias matching against `dim_city_alias.raw_alias`.
3. Fuzzy string matching using RapidFuzz (Levenshtein distance with a score threshold >= 85).
Unresolved names below the confidence threshold are directed to `quarantine_unmatched_city_names` rather than silently dropped or erroneously joined.

---

## Source Data and Cleaning Logic

The seed dataset (`data/raw/indian_cities.csv`) incorporates 8 deliberate dirty data anomalies mirroring real-world legacy data issues:

| # | Anomaly | Raw Example | Cleaning Strategy |
|---|---|---|---|
| 1 | Surrounding Whitespace | `" Mumbai "` | Strips leading and trailing whitespace across all string columns |
| 2 | Inconsistent Capitalization | `"DELHI"`, `"bangalore"` | Standardizes to Title Case (`"Delhi"`, `"Bangalore"`) |
| 3 | Historical City Names | `"Bombay"`, `"Calcutta"`, `"Madras"` | Maps historical names to canonical modern names (*Mumbai*, *Kolkata*, *Chennai*) and seeds `dim_city_alias` |
| 4 | Missing Coordinates | Blank latitude / longitude | Validates coordinate ranges (lat 6-38, lon 68-98) and drops incomplete records |
| 5 | Non-numeric Placeholders | `"N/A"`, `"null"` | Coerces to standard IEEE NaN values before type casting |
| 6 | Duplicate Rows | Repeated city entries | Deduplicates on canonical city name, keeping the most complete record |
| 7 | Malformed Delimiters | Extra commas / trailing separators | Parsed using Python's flexible CSV engine with error handling |
| 8 | Fully Blank Rows | Empty carriage returns | Automatically skipped during initial CSV ingestion |

---

## Data Warehouse Schema

The data warehouse (`weather_aqi_dwh`) is modeled as a Star Schema in PostgreSQL:

### Dimension Tables

- **`dim_city`**:
  - `city_id` (PK, SERIAL): Surrogate city key.
  - `city_name` (VARCHAR): Canonical city name.
  - `state` (VARCHAR): State or union territory.
  - `tier` (SMALLINT): Administrative classification (Tier 1, Tier 2, Tier 3).
  - `latitude` / `longitude` (NUMERIC): Geographic coordinates.
  - `is_active` (BOOLEAN): Soft-delete flag for pipeline inclusion.
- **`dim_city_alias`**:
  - `alias_id` (PK, SERIAL): Surrogate alias key.
  - `raw_alias` (VARCHAR, UNIQUE): Historical or alternate name (e.g., *Bombay*, *Poona*).
  - `canonical_city_id` (FK -> `dim_city.city_id`): Destination canonical city.
  - `match_method` (VARCHAR): `exact`, `fuzzy`, or `manual`.
- **`dim_date`**:
  - Pre-populated with 4,000+ calendar dates (2020 through 2030).
  - Includes `date_key` (PK, YYYYMMDD integer), `full_date`, `day_of_week`, `day_name`, `month`, `month_name`, `quarter`, `year`, `is_weekend`.

### Fact Tables

- **`fact_weather_reading`**:
  - `reading_id` (PK, BIGSERIAL): Surrogate measurement key.
  - `city_id` (FK -> `dim_city.city_id`): Foreign key to city dimension.
  - `date_key` (FK -> `dim_date.date_key`): Foreign key to date dimension.
  - `reading_ts` (TIMESTAMPTZ): Observation timestamp.
  - `temperature_c` (NUMERIC(5,2)): Air temperature in Celsius.
  - `humidity_pct` (NUMERIC(5,2)): Relative humidity percentage.
  - `precipitation_mm` (NUMERIC(7,2)): Precipitation amount in millimeters.
  - `pressure_hpa` (NUMERIC(7,2)): Atmospheric pressure at surface in hPa.
  - `wind_speed_kmh` (NUMERIC(6,2)): Wind speed in kilometers per hour.
  - `fetched_at` (TIMESTAMPTZ): Ingestion timestamp.
  - Unique Constraint: `(city_id, reading_ts)`.
- **`fact_air_quality_reading`**:
  - `reading_id` (PK, BIGSERIAL): Surrogate measurement key.
  - `city_id` (FK -> `dim_city.city_id`): Foreign key to city dimension.
  - `date_key` (FK -> `dim_date.date_key`): Foreign key to date dimension.
  - `reading_ts` (TIMESTAMPTZ): Observation timestamp.
  - `aqi_value` (SMALLINT): Calculated Air Quality Index.
  - `pm25`, `pm10`, `no2`, `so2`, `co`, `o3` (NUMERIC(7,2)): Individual pollutant concentrations.
  - `dominant_pollutant` (VARCHAR(10)): Primary pollutant driving the overall AQI score.
  - `fetched_at` (TIMESTAMPTZ): Ingestion timestamp.
  - Unique Constraint: `(city_id, reading_ts)`.

### Audit and Governance Tables

- **`pipeline_run_log`**: Tracks DAG execution lifecycle, run IDs, duration, and loaded row counts.
- **`dq_check_results`**: Stores the outcome, check name, target table, severity, and diagnostic details of every executed data quality check.
- **`quarantine_unmatched_city_names`**: Isolates unrecognized raw names encountered during ingestion for administrative review.

---

## Automated Data Quality Framework

Data quality checks execute as an automated gate at the conclusion of each ETL cycle (`run_dq_checks` task). All 10 checks run within a single transaction and write results to `dq_check_results`:

| # | Check Name | Target Table | Type | Pass Threshold | Severity on Violation |
|---|---|---|---|---|---|
| 1 | `schema_presence` | Both fact tables | Schema | All expected columns present | FAIL (Task fails) |
| 2 | `freshness` | `fact_weather_reading` | Freshness | Latest observation <= 3.0 hours old | WARN (3-6h) / FAIL (>6h) |
| 3 | `completeness` | `fact_weather_reading` | Completeness | >= 80% active cities present | WARN (50-80%) / FAIL (<50%) |
| 4 | `null_critical_fields` | Both fact tables | Integrity | Zero NULLs in `city_id`, `date_key`, `reading_ts` | FAIL (Task fails) |
| 5 | `range_temperature` | `fact_weather_reading` | Range | Temperature between -10 deg C and 55 deg C | WARN (Logged for review) |
| 6 | `range_humidity` | `fact_weather_reading` | Range | Humidity between 0% and 100% | FAIL (Physically impossible) |
| 7 | `range_aqi` | `fact_air_quality_reading`| Range | AQI between 0 and 500 | FAIL (Standard index bounds) |
| 8 | `row_count_sanity` | Both fact tables | Sanity | Non-zero rows inserted in current run | WARN (Low count alert) |
| 9 | `city_resolution` | `quarantine_unmatched_city_names` | Reconciliation | Unresolved city count <= 5 | WARN (>5) / FAIL (>15) |
| 10| `uniqueness` | Both fact tables | Uniqueness | Zero duplicate `(city_id, reading_ts)` tuples | FAIL (Constraint violation) |

If any check flags a `FAIL` severity, the runner raises a `RuntimeError`, immediately marking the Airflow task red and preventing unverified downstream usage.

---

## Airflow DAG Architecture

### 1. `dim_city_pipeline`
- **Schedule**: Weekly (`@weekly`), `catchup=False`.
- **Purpose**: Cleans the seed CSV, handles historical city resolution, loads `dim_city`, and populates `dim_city_alias`.
- **Tasks**: `extract_csv` -> `transform_cities` -> `load_cities` -> `seed_aliases`.

### 2. `weather_aqi_pipeline`
- **Schedule**: Every 3 hours (`0 */3 * * *`), `catchup=False`.
- **Purpose**: End-to-end ingestion, staging, loading, and quality auditing for weather and air quality.
- **Tasks**:
  1. `get_active_cities`: Queries `dim_city` in PostgreSQL for active coordinate records.
  2. `extract_weather`: Batches requests to Open-Meteo and writes raw JSON to MongoDB (`raw_weather_readings`).
  3. `extract_aqi_city`: Dynamically mapped task per city (`expand`); queries WAQI and writes raw JSON to MongoDB (`raw_aqi_readings`).
  4. `transform_and_load`: Pulls raw records from MongoDB, transforms into structured DataFrames, reconciles foreign keys, and idempotently upserts to PostgreSQL fact tables.
  5. `run_dq_checks`: Executes the 10 data quality checks and records execution audit rows.

---

## Repository Structure

```
Weather_ETL/
├── dags/
│   ├── dim_city_pipeline.py         # Dimension DAG: CSV cleaning & city loading
│   └── weather_aqi_pipeline.py      # Primary ETL DAG: Ingestion, loading, DQ checks
├── include/
│   ├── extract/
│   │   ├── weather.py               # Open-Meteo batched extractor -> MongoDB
│   │   └── aqi.py                   # WAQI per-city extractor -> MongoDB
│   ├── transform/
│   │   ├── cities.py                # CSV cleaning & historical name resolution
│   │   ├── weather.py               # Weather JSON parser & normalizer
│   │   ├── aqi.py                   # AQI JSON parser & normalizer
│   │   └── city_resolver.py         # Multi-tier exact & fuzzy city reconciler
│   ├── load/
│   │   ├── dim_city_loader.py       # Dimension loader (dim_city, dim_city_alias)
│   │   └── fact_loader.py           # Fact table idempotent upsert loader
│   ├── quality/
│   │   ├── checks.py                # 10 production Data Quality check functions
│   │   └── runner.py                # DQ orchestrator & database persistence
│   └── utils/
│       ├── db.py                    # PostgreSQL & MongoDB connection utilities
│       └── retry.py                 # Exponential backoff retry decorator
├── data/
│   └── raw/
│       └── indian_cities.csv        # Seed dataset with deliberate dirty data anomalies
├── sql/
│   ├── 001_create_warehouse_schema.sql  # DDL: Star schema, tables, indexes, constraints
│   └── 002_analytics_queries.sql         # 3 analytical queries (window, gaps & islands, rank)
├── tests/
│   ├── conftest.py                  # Pytest fixtures & mock DB cursors
│   ├── test_transform_cities.py     # 18 unit tests for CSV cleaning
│   ├── test_transform_weather.py    # 14 unit tests for weather JSON parsing
│   ├── test_transform_aqi.py        # 11 unit tests for AQI parsing
│   ├── test_dq_checks.py            # 24 unit tests for all 10 DQ checks
│   └── test_dag_integrity.py        # Airflow DagBag integrity & DAG validation
├── docker/
│   └── Dockerfile.airflow           # Custom Airflow container with ETL dependencies
├── docker-compose.yaml              # Multi-container orchestration (7 services)
├── pytest.ini                       # Test configuration
├── requirements.txt                 # Pinned Python dependencies
├── .env.example                     # Environment variable template
└── .gitignore                       # Git ignore rules
```

---

## Quick Start Guide

### 1. Prerequisites
- Docker Desktop (with Compose v2)
- At least 4 GB of RAM allocated to Docker
- Free WAQI API token: [aqicn.org/data-platform/token](https://aqicn.org/data-platform/token/)

### 2. Configure Environment Variables
Copy the template configuration file:
```bash
cp .env.example .env
```
Open `.env` and add your WAQI API token:
```dotenv
WAQI_TOKEN=your_token_here
```

### 3. Start All Containers
```bash
docker compose up --build -d
```
Verify that all 7 containers report healthy or running:
```bash
docker compose ps
```

### 4. Run the Pipelines
Trigger the dimension pipeline first to populate cities:
```bash
docker compose exec airflow-scheduler airflow dags trigger dim_city_pipeline
```
Trigger the weather and AQI pipeline:
```bash
docker compose exec airflow-scheduler airflow dags trigger weather_aqi_pipeline
```

### 5. Access Web Interfaces
- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (Credentials: `admin` / `admin`)
- **Mongo Express**: [http://localhost:8081](http://localhost:8081)
- **Adminer (PostgreSQL Client)**: [http://localhost:8082](http://localhost:8082)
  - System: `PostgreSQL`
  - Server: `warehouse-postgres`
  - Username: `warehouse`
  - Password: `warehouse_secret`
  - Database: `weather_aqi_dwh`

---

## Testing

The project includes unit tests for data cleaning, transformation, parsing, and data quality functions, as well as an Airflow DAG integrity test. All database and API calls in unit tests are mocked.

### Running Local Unit Tests (No Docker Required)
```bash
python -m venv .venv
.\.venv\Scripts\activate      # Windows
# source .venv/bin/activate   # Linux/macOS
pip install -r requirements.txt
pytest
```
*Result: 77 passed, 1 skipped in under 2 seconds.*

### Running End-to-End Tests Inside Docker
```bash
docker compose exec airflow-scheduler pytest tests/ -v
```

---

## Analytical Queries

The data warehouse supports complex analytical queries across time, location, and environmental metrics.

### Query 1: 24-Hour Rolling Average Temperature per City
Demonstrates the use of a window function with a `RANGE` frame to smooth hourly temperature variations across variable observation counts:

```sql
SELECT
    c.city_name,
    c.state,
    f.reading_ts,
    f.temperature_c                         AS raw_temperature_c,
    ROUND(
        AVG(f.temperature_c) OVER (
            PARTITION BY f.city_id
            ORDER BY f.reading_ts
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        )::NUMERIC,
        2
    )                                       AS rolling_24h_avg_temp_c,
    COUNT(*) OVER (
        PARTITION BY f.city_id
        ORDER BY f.reading_ts
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
    )                                       AS readings_in_window
FROM fact_weather_reading   f
JOIN dim_city               c  ON f.city_id = c.city_id
WHERE f.reading_ts >= NOW() - INTERVAL '7 days'
ORDER BY c.city_name, f.reading_ts DESC;
```

### Query 2: Sustained Hazardous AQI Streaks (Gaps-and-Islands Pattern)
Identifies consecutive runs of 3 or more readings where AQI exceeded 150 (Unhealthy on the US EPA index):

```sql
WITH aqi_numbered AS (
    SELECT
        city_id,
        reading_ts,
        aqi_value,
        ROW_NUMBER() OVER (
            PARTITION BY city_id
            ORDER BY reading_ts
        ) AS rn,
        ROW_NUMBER() OVER (
            PARTITION BY city_id, (aqi_value > 150)::INT
            ORDER BY reading_ts
        ) AS rn2
    FROM fact_air_quality_reading
    WHERE aqi_value IS NOT NULL
),
unhealthy_only AS (
    SELECT
        city_id,
        reading_ts,
        aqi_value,
        (rn - rn2) AS island_id
    FROM aqi_numbered
    WHERE aqi_value > 150
),
streaks AS (
    SELECT
        city_id,
        island_id,
        MIN(reading_ts)                             AS streak_start,
        MAX(reading_ts)                             AS streak_end,
        COUNT(*)                                    AS consecutive_readings,
        ROUND(AVG(aqi_value)::NUMERIC, 1)          AS avg_aqi_in_streak,
        MAX(aqi_value)                              AS peak_aqi
    FROM unhealthy_only
    GROUP BY city_id, island_id
    HAVING COUNT(*) >= 3
)
SELECT
    c.city_name,
    c.state,
    s.streak_start,
    s.streak_end,
    s.consecutive_readings,
    s.avg_aqi_in_streak,
    s.peak_aqi,
    AGE(s.streak_end, s.streak_start)              AS streak_duration
FROM streaks  s
JOIN dim_city c  ON s.city_id = c.city_id
ORDER BY s.consecutive_readings DESC, s.avg_aqi_in_streak DESC;
```

### Query 3: Weekly City AQI Leaderboard (`RANK()`)
Aggregates readings by ISO calendar week using `dim_date` and ranks cities by average AQI:

```sql
WITH weekly_city_aqi AS (
    SELECT
        f.city_id,
        d.year,
        d.week_of_year,
        MIN(d.full_date)                            AS week_start_date,
        ROUND(AVG(f.aqi_value)::NUMERIC, 1)        AS avg_aqi,
        COUNT(*)                                    AS readings_count
    FROM fact_air_quality_reading   f
    JOIN dim_date                   d  ON f.date_key = d.date_key
    WHERE f.aqi_value IS NOT NULL
    GROUP BY f.city_id, d.year, d.week_of_year
),
ranked AS (
    SELECT
        city_id,
        year,
        week_of_year,
        week_start_date,
        avg_aqi,
        readings_count,
        RANK() OVER (
            PARTITION BY year, week_of_year
            ORDER BY avg_aqi DESC
        )                                           AS rank_worst_aqi
    FROM weekly_city_aqi
)
SELECT
    r.year,
    r.week_of_year,
    r.week_start_date,
    r.rank_worst_aqi,
    c.city_name,
    c.state,
    r.avg_aqi,
    r.readings_count,
    CASE
        WHEN r.avg_aqi <= 50  THEN 'Good'
        WHEN r.avg_aqi <= 100 THEN 'Moderate'
        WHEN r.avg_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN r.avg_aqi <= 200 THEN 'Unhealthy'
        WHEN r.avg_aqi <= 300 THEN 'Very Unhealthy'
        ELSE                       'Hazardous'
    END                                             AS aqi_category
FROM ranked         r
JOIN dim_city       c  ON r.city_id = c.city_id
WHERE r.rank_worst_aqi <= 5
ORDER BY r.year DESC, r.week_of_year DESC, r.rank_worst_aqi;
```
