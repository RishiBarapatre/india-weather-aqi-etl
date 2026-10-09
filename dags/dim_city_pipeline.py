"""
dags/dim_city_pipeline.py
--------------------------
DAG 1 — Slow-Changing Dimension loader for dim_city and dim_city_alias.

Schedule : weekly (every Monday at 01:00 UTC) + manual trigger
Purpose  : Read the messy Indian cities CSV, clean it, and load the results
           into the warehouse dim_city and dim_city_alias tables.

Why "slow-changing"?
  Cities don't appear or rename frequently.  Running this weekly (or manually
  when the CSV is updated) is more than enough — we don't need it every 3 hours
  like the weather/AQI pipeline.

Task graph (linear — each task feeds the next):

  extract_csv  →  transform_cities  →  load_cities  →  seed_aliases

TaskFlow API:
  We use Airflow's @task decorator (TaskFlow API) instead of the older
  PythonOperator.  This means:
    - Return values are automatically XCom-pushed (no manual xcom_push calls)
    - The next task receives the return value directly as a function argument
    - Much cleaner, more Pythonic code
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG default arguments — applied to every task unless overridden
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,          # each run is independent
    "email_on_failure": False,          # set True in production with SMTP config
    "email_on_retry":   False,
    "retries":          2,              # retry up to 2 times on failure
    "retry_delay":      timedelta(minutes=5),
}


@dag(
    dag_id="dim_city_pipeline",
    description="Load and clean the Indian cities CSV into dim_city / dim_city_alias",
    schedule="0 1 * * 1",              # every Monday at 01:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,                     # don't backfill missed weekly runs
    default_args=DEFAULT_ARGS,
    tags=["dimension", "cities", "etl"],
)
def dim_city_pipeline():
    """
    Slow-changing dimension pipeline for Indian city master data.

    Reads the messy CSV → cleans with pandas → upserts into PostgreSQL.
    Fully idempotent: running it multiple times produces the same result.
    """

    # ── Task 1: Extract ────────────────────────────────────────────────────
    @task(task_id="extract_csv")
    def extract_csv() -> str:
        """
        Verify the CSV file exists and return its path as a string.

        Why return a path string instead of the file contents?
        XCom (Airflow's inter-task message passing) is backed by the metadata
        database.  Passing the full CSV content through XCom would bloat the
        DB and slow down the scheduler.  Instead, we pass the lightweight path
        and let the next task read the file directly.
        """
        csv_path = Path("/opt/airflow/data/raw/indian_cities.csv")
        if not csv_path.exists():
            raise FileNotFoundError(f"Cities CSV not found at {csv_path}")

        logger.info("CSV found at %s", csv_path)
        return str(csv_path)

    # ── Task 2: Transform ──────────────────────────────────────────────────
    @task(task_id="transform_cities")
    def transform_cities(csv_path: str) -> dict:
        """
        Clean the raw CSV and return structured data.

        Applies all 8 cleaning steps:
          1. Strip whitespace
          2. Normalise capitalisation
          3. Resolve historical city names (Bombay → Mumbai, etc.)
          4. Coerce numeric columns, set invalid values to NaN
          5. Drop rows with missing/invalid coordinates
          6. Drop duplicate rows (keep last)
          (7 & 8 handled automatically by pandas read_csv)

        Returns a dict with two keys so both lists travel through XCom
        together as one payload:
          {
            "cities":  [ {city_name, state, latitude, ...}, ... ],
            "aliases": [ {raw_alias, canonical, match_method}, ... ]
          }
        """
        from include.transform.cities import clean_cities

        cleaned_cities, alias_seeds = clean_cities(Path(csv_path))

        logger.info(
            "Transform complete — %d cities, %d alias seeds",
            len(cleaned_cities), len(alias_seeds),
        )
        return {"cities": cleaned_cities, "aliases": alias_seeds}

    # ── Task 3: Load dim_city ──────────────────────────────────────────────
    @task(task_id="load_cities")
    def load_cities_task(transformed: dict) -> dict:
        """
        Upsert cleaned cities into dim_city.

        Uses ON CONFLICT (city_name, state) DO UPDATE so re-runs are safe.

        Returns the name → city_id mapping (needed by the next task to link
        alias seeds to the correct city_id foreign key).
        """
        from include.load.dim_city_loader import load_cities

        city_name_to_id = load_cities(transformed["cities"])
        logger.info("Loaded %d cities into dim_city", len(city_name_to_id))

        # Pass both the alias seeds AND the id map to the next task
        return {
            "aliases":         transformed["aliases"],
            "city_name_to_id": city_name_to_id,
        }

    # ── Task 4: Seed dim_city_alias ────────────────────────────────────────
    @task(task_id="seed_aliases")
    def seed_aliases_task(load_result: dict) -> None:
        """
        Pre-seed dim_city_alias with historical name → canonical city maps.

        After this task, future pipeline runs can resolve "Bombay", "Calcutta",
        "Madras" etc. instantly via exact lookup — no fuzzy matching needed.

        ON CONFLICT (raw_alias) DO NOTHING means this is also idempotent:
        re-running never overwrites existing (possibly manually corrected) aliases.
        """
        from include.load.dim_city_loader import load_aliases

        count = load_aliases(
            alias_seeds=load_result["aliases"],
            city_name_to_id=load_result["city_name_to_id"],
        )
        logger.info("Seeded %d aliases into dim_city_alias", count)

    # ── Wire up the task graph ─────────────────────────────────────────────
    # TaskFlow automatically infers dependencies from argument passing.
    # The explicit >> chain is not required here but makes the order crystal
    # clear when reading the code.
    csv_path    = extract_csv()
    transformed = transform_cities(csv_path)
    load_result = load_cities_task(transformed)
    seed_aliases_task(load_result)


# Airflow discovers DAGs by importing the module and looking for a DAG object.
# Calling the decorated function registers it with Airflow's scheduler.
dim_city_pipeline()
