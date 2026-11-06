"""DagBag import test. Needs apache-airflow (requirements-airflow.txt); skipped without it."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402
from claims_dags.spec import build_daily_pipeline  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_claims_daily_matches_spec(bag):
    dag = bag.dags["claims_daily"]
    spec = {t.task_id: t for t in build_daily_pipeline()}
    assert set(dag.task_ids) == set(spec)
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert dag.sla_miss_callback is not None
    for tid, t in spec.items():
        task = dag.get_task(tid)
        assert set(task.upstream_task_ids) == set(t.upstream)
        assert task.retries == t.retries
        assert task.execution_timeout == t.execution_timeout
        assert task.sla == t.sla
        assert task.on_failure_callback
        assert task.pool == (t.pool or "default_pool")
        assert task.depends_on_past is True
    assert dag.get_task("gold").retry_exponential_backoff is True
