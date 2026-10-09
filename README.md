# 🌦️ India Weather & AQI ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.14-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248.svg)](https://www.mongodb.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20v2-2496ED.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/Tests-77%20Passed-brightgreen.svg)]()

An end-to-end, production-grade Data Engineering pipeline that ingests, cleans, validates, and models weather and air quality data for Indian cities into a Star Schema data warehouse.

Built with **Apache Airflow**, **MongoDB**, **PostgreSQL**, **Pandas**, and **Docker Compose**.

---

## 🏛️ Architecture Overview

The pipeline implements a **Medallion / Lakehouse architecture**:

```
                       ┌──────────────────────────────────────────────┐
                       │               EXTERNAL APIS                  │
                       │  • Open-Meteo API (Multi-location batched)   │
                       │  • WAQI API (Dynamic task mapping per city)  │
                       └──────────────────────┬───────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ BRONZE ZONE / RAW LAKE (MongoDB: weather_aqi_raw)                                           │
│ • raw_weather_readings   (verbatim JSON payloads, execution audit metadata)                 │
│ • raw_aqi_readings       (verbatim JSON payloads, station metadata)                         │
└─────────────────────────────────────────────┬───────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ SILVER & GOLD ZONE / DATA WAREHOUSE (PostgreSQL: weather_aqi_dwh)                           │
│ • Dimensions : dim_city, dim_city_alias, dim_date (pre-populated 4,000+ dates)              │
│ • Facts      : fact_weather_reading, fact_air_quality_reading                               │
│ • Governance : quarantine_unmatched_city_names, dq_check_results, pipeline_run_log          │
└─────────────────────────────────────────────┬───────────────────────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ QUALITY & ANALYTICS                                                                         │
│ • 10 Automated Data Quality Checks (freshness, completeness, ranges, schema, uniqueness)    │
│ • Advanced Analytical SQL (Rolling 24h averages, Gaps-and-Islands streaks, RANK() leaderboards)│
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Engineering Features

1. **Airflow Dynamic Task Mapping**:
   - The WAQI API only serves single-coordinate queries. Airflow dynamically maps individual tasks per city (`expand(city=...)`), allowing individual retries and isolating partial API failures without halting the pipeline.
2. **Batch Request Optimization**:
   - Open-Meteo weather readings are batched across multi-coordinate URLs, reducing 35+ HTTP requests to just 4 batched network round-trips.
3. **Dirty Data Reconciliation**:
   - Resolves 8 real-world data issues from legacy CSV inputs: trims whitespace, fixes improper casing, handles coordinates/nulls, and canonicalizes historical city names (e.g. *Bombay* → *Mumbai*, *Calcutta* → *Kolkata*, *Madras* → *Chennai*, *Poona* → *Pune*) while seeding `dim_city_alias`.
4. **Idempotent Loading & Star Schema**:
   - Upserts with PostgreSQL `ON CONFLICT (city_id, reading_ts) DO UPDATE` guarantee zero duplication on DAG re-runs.
5. **Automated Data Quality & Audit Logging**:
   - Runs 10 automated pre-analytics checks (Schema Presence, Freshness, Completeness, Null Checks, Temperature Range, Humidity Range, AQI Range, Row Count Sanity, City Resolution, Uniqueness).
   - Audit entries are logged in `dq_check_results` and `pipeline_run_log` within a single atomic transaction.
6. **Advanced SQL Analytics**:
   - Production SQL scripts showcasing `RANGE BETWEEN INTERVAL '24 hours' PRECEDING`, Gaps-and-Islands streak detection for hazardous pollution events, and weekly `RANK()` leaderboards.

---

## 📁 Repository Structure

```
Weather_ETL/
├── dags/
│   ├── dim_city_pipeline.py         # Milestone 2: Cleans CSV & seeds dim_city + aliases
│   └── weather_aqi_pipeline.py      # Milestones 3-6: Extract, Load, DQ checks
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
│   │   ├── warehouse.py             # Star schema PostgreSQL loader (idempotent upserts)
│   │   └── raw_lake.py              # MongoDB upsert helper
│   ├── quality/
│   │   ├── checks.py                # 10 production Data Quality check functions
│   │   └── runner.py                # DQ orchestrator & audit logging
│   └── utils/
│       ├── db.py                    # Connection managers (Postgres & MongoDB)
│       └── retry.py                 # Exponential backoff decorator (Tenacity)
├── data/
│   └── raw/
│       └── indian_cities.csv        # Seed dataset with 8 deliberate dirty data patterns
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
├── requirements.txt                 # Python dependencies
├── .env.example                     # Environment template
└── .gitignore                       # Git ignore rules (protects credentials)
```

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Free WAQI API token: [aqicn.org/data-platform/token](https://aqicn.org/data-platform/token/)

### 2. Configure Environment
```bash
cp .env.example .env
```
Open `.env` and set your token:
```dotenv
WAQI_TOKEN=your_token_here
```

### 3. Start the Platform
```bash
docker compose up --build -d
```

Check that all 7 containers are healthy:
```bash
docker compose ps
```

| Service | Port | Description | Credentials |
|---|---|---|---|
| **Airflow Webserver** | [localhost:8080](http://localhost:8080) | Orchestration UI | `admin` / `admin` |
| **Mongo Express** | [localhost:8081](http://localhost:8081) | Browse Raw JSON Lake | No auth |
| **Adminer** | [localhost:8082](http://localhost:8082) | PostgreSQL Web Client | Server: `warehouse-postgres`, User: `warehouse`, DB: `weather_aqi_dwh` |
| **PostgreSQL DWH** | `localhost:5433` | Star Schema Warehouse | User: `warehouse`, Password: `warehouse_secret` |
| **MongoDB** | `localhost:27017` | Raw Landing Lake | User: `root`, Password: `mongo_secret` |

---

## 🔄 Running the Pipelines

### Step 1: Run the Cities Dimension Pipeline
Unpause and trigger `dim_city_pipeline` in Airflow, or run via CLI:
```bash
docker compose exec airflow-scheduler airflow dags trigger dim_city_pipeline
```
*Loads 34 canonical Indian cities into `dim_city` and seeds 7 historical aliases into `dim_city_alias`.*

### Step 2: Run the Weather & AQI Pipeline
Unpause and trigger `weather_aqi_pipeline`:
```bash
docker compose exec airflow-scheduler airflow dags trigger weather_aqi_pipeline
```
*Fetches live readings from Open-Meteo & WAQI, stages them in MongoDB, transforms & loads to PostgreSQL, and executes all 10 DQ checks.*

---

## 🧪 Testing

### Local Unit Testing (Virtual Environment)
```bash
python -m venv .venv
.\.venv\Scripts\activate  # Or source .venv/bin/activate on Linux/Mac
pip install -r requirements.txt
pytest
```
*77 unit tests pass in under 2 seconds (CSV cleaning, API parsers, DQ checks with mocked connections).*

### End-to-End Container Tests
```bash
docker compose exec airflow-scheduler pytest tests/ -v
```

---

## 📊 Sample Analytical Queries

Run inside Adminer ([http://localhost:8082](http://localhost:8082)) or any PostgreSQL client:

### 1. 24-Hour Rolling Average Temperature (RANGE Frame)
```sql
SELECT
    c.city_name,
    c.state,
    f.reading_ts,
    f.temperature_c,
    ROUND(
        AVG(f.temperature_c) OVER (
            PARTITION BY f.city_id
            ORDER BY f.reading_ts
            RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
        )::NUMERIC, 2
    ) AS rolling_24h_avg_temp_c
FROM fact_weather_reading f
JOIN dim_city c ON f.city_id = c.city_id
ORDER BY c.city_name, f.reading_ts DESC;
```

### 2. Weekly Most Polluted Cities Leaderboard (`RANK()`)
```sql
WITH weekly_city_aqi AS (
    SELECT
        f.city_id,
        d.year,
        d.week_of_year,
        ROUND(AVG(f.aqi_value)::NUMERIC, 1) AS avg_aqi,
        COUNT(*) AS readings_count
    FROM fact_air_quality_reading f
    JOIN dim_date d ON f.date_key = d.date_key
    WHERE f.aqi_value IS NOT NULL
    GROUP BY f.city_id, d.year, d.week_of_year
),
ranked AS (
    SELECT
        city_id, year, week_of_year, avg_aqi, readings_count,
        RANK() OVER (PARTITION BY year, week_of_year ORDER BY avg_aqi DESC) AS rank_worst_aqi
    FROM weekly_city_aqi
)
SELECT
    r.year, r.week_of_year, r.rank_worst_aqi,
    c.city_name, c.state, r.avg_aqi,
    CASE
        WHEN r.avg_aqi <= 50  THEN 'Good'
        WHEN r.avg_aqi <= 100 THEN 'Moderate'
        WHEN r.avg_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN r.avg_aqi <= 200 THEN 'Unhealthy'
        WHEN r.avg_aqi <= 300 THEN 'Very Unhealthy'
        ELSE                       'Hazardous'
    END AS aqi_category
FROM ranked r
JOIN dim_city c ON r.city_id = c.city_id
WHERE r.rank_worst_aqi <= 5
ORDER BY r.year DESC, r.week_of_year DESC, r.rank_worst_aqi;
```
