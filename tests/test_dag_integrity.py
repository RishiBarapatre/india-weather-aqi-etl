"""
tests/test_dag_integrity.py
-----------------------------
DAG integrity tests — verify that all DAGs can be imported, parsed,
and contain the expected tasks without raising any errors.

This is the most important automated check for Airflow projects.
It catches:
  - Syntax errors in DAG files
  - Import errors (missing dependency, typo in module name)
  - Missing task wiring (task referenced but never connected)
  - DAG ID collisions

These tests run WITHOUT a real Airflow database or scheduler.
They only test that the Python code parses correctly.

Run with:  pytest tests/test_dag_integrity.py -v

NOTE: These tests require the `apache-airflow` package to be installed
(it is available inside the Airflow Docker containers, and can also be
installed locally for development).
"""

import pytest

# Guard: skip this entire module if Airflow is not installed locally.
# In CI / Docker, Airflow is always available.
airflow = pytest.importorskip("airflow", reason="apache-airflow not installed")

from airflow.models import DagBag


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def dagbag():
    """
    Load all DAGs from the dags/ folder into a DagBag.

    DagBag is Airflow's DAG loader — it imports every .py file in the
    dags/ directory and registers the DAG objects it finds.

    scope="module" means this runs once per test file, not once per test.
    """
    return DagBag(dag_folder="dags/", include_examples=False)


# ── dim_city_pipeline ─────────────────────────────────────────────────────────

class TestDimCityPipelineIntegrity:

    DAG_ID = "dim_city_pipeline"

    def test_dag_loaded_without_errors(self, dagbag):
        """The DAG file must import without any Python errors."""
        assert dagbag.import_errors == {}, \
            f"DAG import errors: {dagbag.import_errors}"

    def test_dag_exists(self, dagbag):
        assert self.DAG_ID in dagbag.dags, \
            f"'{self.DAG_ID}' not found. Available DAGs: {list(dagbag.dags)}"

    def test_dag_has_correct_schedule(self, dagbag):
        dag = dagbag.dags[self.DAG_ID]
        # Weekly on Monday at 01:00 UTC
        assert dag.schedule_interval == "0 1 * * 1"

    def test_dag_has_expected_tasks(self, dagbag):
        dag = dagbag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        expected = {"extract_csv", "transform_cities", "load_cities", "seed_aliases"}
        assert expected.issubset(task_ids), \
            f"Missing tasks: {expected - task_ids}"

    def test_task_order_is_correct(self, dagbag):
        """
        Verify the dependency chain:
          extract_csv → transform_cities → load_cities → seed_aliases
        """
        dag = dagbag.dags[self.DAG_ID]
        tasks = {t.task_id: t for t in dag.tasks}

        # transform_cities must depend on extract_csv
        assert "extract_csv" in {
            t.task_id for t in tasks["transform_cities"].upstream_list
        }
        # seed_aliases must depend on load_cities
        assert "load_cities" in {
            t.task_id for t in tasks["seed_aliases"].upstream_list
        }

    def test_catchup_is_disabled(self, dagbag):
        dag = dagbag.dags[self.DAG_ID]
        assert dag.catchup is False


# ── weather_aqi_pipeline ──────────────────────────────────────────────────────

class TestWeatherAqiPipelineIntegrity:

    DAG_ID = "weather_aqi_pipeline"

    def test_dag_exists(self, dagbag):
        assert self.DAG_ID in dagbag.dags

    def test_dag_has_correct_schedule(self, dagbag):
        dag = dagbag.dags[self.DAG_ID]
        assert dag.schedule_interval == "0 */3 * * *"

    def test_dag_has_expected_tasks(self, dagbag):
        dag = dagbag.dags[self.DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "get_active_cities",
            "extract_weather",
            "extract_aqi_city",
            "transform_and_load",
            "run_dq_checks",
        }
        assert expected.issubset(task_ids), \
            f"Missing tasks: {expected - task_ids}"

    def test_catchup_is_disabled(self, dagbag):
        dag = dagbag.dags[self.DAG_ID]
        assert dag.catchup is False

    def test_max_active_tasks_is_set(self, dagbag):
        """Concurrency must be limited to avoid WAQI rate-limit issues."""
        dag = dagbag.dags[self.DAG_ID]
        assert dag.max_active_tasks <= 10

    def test_no_dag_import_errors(self, dagbag):
        assert dagbag.import_errors == {}, \
            f"DAG import errors found: {dagbag.import_errors}"
