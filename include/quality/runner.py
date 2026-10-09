"""
include/quality/runner.py
--------------------------
Orchestrates all DQ checks for a pipeline run.

Responsibilities:
  1. Open a warehouse connection (shared across all checks — one transaction)
  2. Run every check from checks.ALL_CHECKS
  3. Write results to dq_check_results
  4. Write a summary row to pipeline_run_log
  5. Return the results list and raise an exception if any check failed
     (so the Airflow task is marked red in the UI)

Usage (called from the DAG task):
    from include.quality.runner import run_dq_checks

    run_dq_checks(
        load_result={"weather_loaded": 34, "aqi_loaded": 31, ...},
        dag_id="weather_aqi_pipeline",
        task_id="run_dq_checks",
        logical_date=date(2024, 9, 26),
        airflow_run_id="scheduled__2024-09-26T03:00:00+00:00",
        task_started_at=datetime(...),
    )
"""

import logging
from datetime import date, datetime, timezone
from typing import Any

from include.quality.checks import ALL_CHECKS
from include.utils.db import get_warehouse_conn, get_warehouse_cursor

logger = logging.getLogger(__name__)


def run_dq_checks(
    load_result: dict,
    dag_id: str,
    task_id: str,
    logical_date: date,
    airflow_run_id: str,
    task_started_at: datetime,
) -> list[dict]:
    """
    Run all 10 DQ checks and persist their results to the warehouse.

    Parameters
    ----------
    load_result : dict
        {"weather_loaded": int, "weather_rejected": int,
         "aqi_loaded": int, "aqi_rejected": int}
        Passed from the transform_and_load task via XCom.

    dag_id, task_id, logical_date, airflow_run_id : str / date
        Airflow identifiers for writing to pipeline_run_log.

    task_started_at : datetime
        When the run_dq_checks task started — used to compute duration.

    Returns
    -------
    list[dict] : All check results.

    Raises
    ------
    RuntimeError if any check returned status="fail".
    This causes the Airflow task to turn red and trigger its retry policy.
    """
    with get_warehouse_conn() as conn:

        # ── Run all checks ────────────────────────────────────────────────
        results: list[dict] = []
        for check_fn in ALL_CHECKS:
            try:
                result = check_fn(conn, load_result)
                results.append(result)
            except Exception as exc:
                # A check itself crashing should not abort the others
                logger.error("Check %s raised an exception: %s", check_fn.__name__, exc)
                results.append({
                    "check_name":  check_fn.__name__.replace("check_", ""),
                    "table_name":  None,
                    "status":      "fail",
                    "details":     f"Check crashed: {exc}",
                })

        # ── Persist check results ─────────────────────────────────────────
        run_log_id = _write_pipeline_run_log(
            conn, dag_id, task_id, logical_date, airflow_run_id,
            load_result, results, task_started_at,
        )
        _write_dq_check_results(conn, run_log_id, results)
        conn.commit()

    # ── Summarise ─────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r["status"] == "pass")
    warned = sum(1 for r in results if r["status"] == "warn")
    failed = sum(1 for r in results if r["status"] == "fail")

    logger.info(
        "DQ checks complete — ✅ %d passed | ⚠️  %d warned | ❌ %d failed",
        passed, warned, failed,
    )

    # ── Fail the task if any check failed ─────────────────────────────────
    if failed > 0:
        failed_names = [r["check_name"] for r in results if r["status"] == "fail"]
        raise RuntimeError(
            f"{failed} DQ check(s) FAILED: {failed_names}. "
            "Inspect dq_check_results for details."
        )

    return results


# ── Private helpers ──────────────────────────────────────────────────────────

def _write_pipeline_run_log(
    conn,
    dag_id: str,
    task_id: str,
    logical_date: date,
    airflow_run_id: str,
    load_result: dict,
    dq_results: list[dict],
    started_at: datetime,
) -> int:
    """
    Insert a row into pipeline_run_log and return the generated run_id.

    Status logic:
      'success'         → all checks passed
      'partial_success' → some warns but no fails
      'failure'         → at least one fail
    """
    failed = sum(1 for r in dq_results if r["status"] == "fail")
    warned = sum(1 for r in dq_results if r["status"] == "warn")

    if failed > 0:
        status = "failure"
    elif warned > 0:
        status = "partial_success"
    else:
        status = "success"

    import json
    cities_failed_json = json.dumps(load_result.get("cities_failed", []))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_run_log
                (dag_id, task_id, logical_date, started_at, ended_at,
                 status, rows_extracted, rows_loaded, rows_rejected,
                 cities_failed)
            VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
            RETURNING run_id
            """,
            (
                dag_id,
                task_id,
                logical_date,
                started_at,
                status,
                load_result.get("weather_loaded", 0) + load_result.get("aqi_loaded", 0),
                load_result.get("weather_loaded", 0) + load_result.get("aqi_loaded", 0),
                load_result.get("weather_rejected", 0) + load_result.get("aqi_rejected", 0),
                cities_failed_json,
            ),
        )
        run_id = cur.fetchone()[0]

    logger.info("Written pipeline_run_log row — run_id=%d, status=%s", run_id, status)
    return run_id


def _write_dq_check_results(conn, run_log_id: int, results: list[dict]) -> None:
    """Bulk-insert all check results into dq_check_results."""
    import psycopg2.extras

    rows = [
        {
            "run_id":     run_log_id,
            "check_name": r["check_name"],
            "table_name": r.get("table_name"),
            "status":     r["status"],
            "details":    r.get("details"),
        }
        for r in results
    ]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO dq_check_results
                (run_id, check_name, table_name, status, details)
            VALUES
                (%(run_id)s, %(check_name)s, %(table_name)s,
                 %(status)s, %(details)s)
            """,
            rows,
        )

    logger.info("Written %d rows to dq_check_results", len(rows))
