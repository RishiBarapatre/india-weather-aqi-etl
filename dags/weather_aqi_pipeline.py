"""
dags/weather_aqi_pipeline.py
------------------------------
DAG 2 — Main weather & AQI pipeline.

Schedule  : Every 3 hours  (0 */3 * * *)
Purpose   : Extract current weather + AQI readings for all active Indian
            cities, store raw JSON in MongoDB, then transform and load
            curated data into the PostgreSQL warehouse.

Current state (Milestone 4):
  Tasks implemented:
    ✅ get_active_cities     — query dim_city for cities to process
    ✅ extract_weather       — fetch Open-Meteo API → store in MongoDB
    ✅ extract_aqi_city      — fetch WAQI API per city (dynamic task mapping)
  Tasks stubbed (implemented in later milestones):
    ⬜ transform_and_load    — Milestone 5
    ⬜ run_dq_checks         — Milestone 6

Task graph:

  get_active_cities
        │
        ├──► extract_weather ──────────────────────────┐
        │                                              │
        └──► extract_aqi_city[Mumbai] ─────────────────┤
             extract_aqi_city[Delhi]  ─────────────────┤  transform_and_load
             extract_aqi_city[Pune]   ─────────────────┤        │
             ...one task per city...  ─────────────────┘  run_dq_checks

Dynamic Task Mapping:
  extract_aqi_city is a @task decorated function called with .expand(city=cities).
  Airflow creates one task INSTANCE per element in `cities` at runtime.
  Each instance is independent: its own logs, its own retry count, its own
  success/failure status in the UI.

  If Delhi's WAQI station is down, only extract_aqi_city[Delhi] fails and
  retries — every other city's task is completely unaffected.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}


@dag(
    dag_id="weather_aqi_pipeline",
    description="Every-3-hour weather & AQI extraction → MongoDB raw zone → PostgreSQL warehouse",
    schedule="0 */3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    # max_active_tasks: how many tasks can run simultaneously within this DAG.
    # We set 10 so that the ~35 dynamic AQI tasks don't all fire at once and
    # overwhelm the WAQI API (which has a free-tier rate limit).
    max_active_tasks=10,
    tags=["weather", "aqi", "etl", "main"],
)
def weather_aqi_pipeline():

    # ── Task 1: Get active cities ──────────────────────────────────────────
    @task(task_id="get_active_cities")
    def get_active_cities() -> list[dict]:
        """
        Query dim_city for all currently active cities.

        Returns a list of dicts (city_id, city_name, latitude, longitude).
        This list is passed to both extract_weather (as a whole list) and
        to extract_aqi_city.expand() (one element per dynamic task).
        """
        from include.utils.db import get_warehouse_conn, get_warehouse_cursor

        with get_warehouse_conn() as conn:
            with get_warehouse_cursor(conn) as cur:
                cur.execute("""
                    SELECT city_id, city_name, latitude, longitude
                    FROM   dim_city
                    WHERE  is_active = TRUE
                    ORDER  BY city_id
                """)
                cities = [dict(row) for row in cur.fetchall()]

        logger.info("Found %d active cities in dim_city", len(cities))
        if not cities:
            raise ValueError(
                "dim_city has no active cities. "
                "Run dim_city_pipeline first to populate it."
            )
        return cities

    # ── Task 2: Extract weather (one task, batches all cities) ────────────
    @task(task_id="extract_weather")
    def extract_weather(cities: list[dict], **context) -> dict:
        """
        Fetch current weather for ALL cities in a single batched task.
        Open-Meteo supports multi-location requests so we batch them.
        Returns a summary dict for downstream tasks.
        """
        from include.extract.weather import fetch_and_store_weather

        run_id = context["run_id"]
        result = fetch_and_store_weather(cities=cities, airflow_run_id=run_id)

        if result["cities_failed"]:
            logger.warning(
                "Weather extraction partial failures: %s", result["cities_failed"]
            )
        return result

    # ── Task 3: Extract AQI (one task PER city — dynamic task mapping) ────
    @task(task_id="extract_aqi_city")
    def extract_aqi_city(city: dict, **context) -> dict:
        """
        Fetch AQI for a SINGLE city from the WAQI API.

        This function is called with .expand(city=cities) below, which tells
        Airflow to create one task instance per element in `cities`.

        Why single-city here vs batched in extract_weather?
          The WAQI API has no batch endpoint — every city needs its own
          HTTP request.  By mapping each city to its own task we get:
            - Independent retries (one bad station can't block others)
            - Per-city visibility in the Airflow UI
            - Parallelism limited by max_active_tasks=10 (respects rate limits)

        The `city` argument here is ONE dict from the cities list:
          {"city_id": 1, "city_name": "Mumbai", "latitude": 19.076, ...}
        """
        from include.extract.aqi import fetch_and_store_aqi

        run_id = context["run_id"]
        result = fetch_and_store_aqi(city=city, airflow_run_id=run_id)

        logger.info(
            "AQI result for %s: status=%s, stored=%s",
            result["city_name"], result["status"], result["stored"],
        )
        return result

    # ── Task 4: Transform & load ───────────────────────────────────────────
    @task(task_id="transform_and_load")
    def transform_and_load(
        weather_result: dict,
        aqi_results: list[dict],   # list — one dict per dynamic AQI task
        **context,
    ) -> dict:
        """
        Read raw documents from MongoDB for this run, transform them,
        and load clean rows into fact_weather_reading + fact_air_quality_reading.

        Receives:
          weather_result : summary dict from extract_weather
          aqi_results    : list of per-city dicts from extract_aqi_city instances
          context        : Airflow injects run_id here

        Returns a summary dict that run_dq_checks will use.
        """
        from include.transform.weather import transform_weather_run
        from include.transform.aqi import transform_aqi_run
        from include.load.fact_loader import load_weather_facts, load_aqi_facts

        run_id = context["run_id"]

        # ── Transform ─────────────────────────────────────────────────────
        logger.info("Transforming weather documents for run: %s", run_id)
        weather_rows = transform_weather_run(run_id)

        logger.info("Transforming AQI documents for run: %s", run_id)
        aqi_rows = transform_aqi_run(run_id)

        # ── Load ──────────────────────────────────────────────────────────
        weather_load = load_weather_facts(weather_rows)
        aqi_load     = load_aqi_facts(aqi_rows)

        # ── Summarise AQI extraction outcome for logging ──────────────────
        aqi_stored  = sum(1 for r in aqi_results if r.get("stored"))
        aqi_failed  = sum(1 for r in aqi_results if r.get("status") == "error")
        aqi_no_data = sum(1 for r in aqi_results if r.get("status") in ("no_station", "no_data"))

        logger.info(
            "Pipeline run summary — "
            "weather: extracted=%d loaded=%d rejected=%d | "
            "aqi: extracted=%d no_station/data=%d failed=%d loaded=%d rejected=%d",
            weather_result.get("rows_stored", 0),
            weather_load["rows_loaded"], weather_load["rows_rejected"],
            aqi_stored, aqi_no_data, aqi_failed,
            aqi_load["rows_loaded"], aqi_load["rows_rejected"],
        )

        return {
            "weather_loaded":  weather_load["rows_loaded"],
            "weather_rejected": weather_load["rows_rejected"],
            "aqi_loaded":      aqi_load["rows_loaded"],
            "aqi_rejected":    aqi_load["rows_rejected"],
        }

    # ── Task 5: Data quality checks ───────────────────────────────────────
    @task(task_id="run_dq_checks")
    def run_dq_checks(load_result: dict, **context) -> None:
        """
        Run all 10 automated DQ checks and write results to the warehouse.

        Raises RuntimeError (failing the task) if any check returns 'fail'.
        Warnings are logged but do not fail the task.

        Results are permanently stored in dq_check_results and
        pipeline_run_log regardless of pass/fail, so trends are queryable.
        """
        from datetime import date
        from include.quality.runner import run_dq_checks as _run_checks

        _run_checks(
            load_result=load_result,
            dag_id=context["dag"].dag_id,
            task_id=context["task"].task_id,
            logical_date=context["logical_date"].date(),
            airflow_run_id=context["run_id"],
            task_started_at=context["task_instance"].start_date,
        )

    # ── Wire up the task graph ─────────────────────────────────────────────
    cities = get_active_cities()

    # extract_weather receives the whole list — one task handles all cities
    weather_result = extract_weather(cities)

    # extract_aqi_city.expand(city=cities):
    #   Airflow reads the cities list from XCom at runtime and creates
    #   one task instance per element.  `aqi_results` is a list of dicts,
    #   one per city, collected automatically from all instances.
    aqi_results = extract_aqi_city.expand(city=cities)

    # Both extraction results flow into the transform step
    load_result = transform_and_load(weather_result, aqi_results)

    run_dq_checks(load_result)


weather_aqi_pipeline()
