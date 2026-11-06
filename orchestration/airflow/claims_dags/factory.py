"""Turn the pure spec into an Airflow DAG. The only module (with ``dags/``) importing Airflow."""

from __future__ import annotations

from datetime import datetime, timedelta

from . import callbacks
from .spec import PipelineConfig, TaskSpec, build_daily_pipeline

DAG_ID = "claims_daily"


def build_dag(cfg: PipelineConfig | None = None, tasks: list[TaskSpec] | None = None):
    from airflow import DAG
    from airflow.operators.bash import BashOperator

    cfg = cfg or PipelineConfig.from_env()
    tasks = tasks or build_daily_pipeline(cfg)
    dag = DAG(
        dag_id=DAG_ID,
        description="claimsgen -> bronze -> silver -> gold -> dbt -> quality gates (one batch/day)",
        schedule=cfg.schedule,
        start_date=datetime(cfg.start_date.year, cfg.start_date.month, cfg.start_date.day),
        catchup=False,  # backfill explicitly: `airflow dags backfill` (docs/orchestration.md)
        max_active_runs=1,  # layers share Delta tables; two batches must not interleave
        dagrun_timeout=timedelta(hours=6),
        sla_miss_callback=callbacks.on_sla_miss,
        default_args={
            "owner": "data-eng",
            "depends_on_past": True,  # dims arrive with the first snapshot; keep batches in order
            "retries": cfg.retries,
            "retry_delay": cfg.retry_delay,
            "retry_exponential_backoff": True,
            "max_retry_delay": cfg.max_retry_delay,
            "on_failure_callback": callbacks.on_failure,
        },
        tags=["claims", "medallion", "synthetic-data"],
        doc_md=__doc__,
    )
    ops = {}
    for t in tasks:
        ops[t.task_id] = BashOperator(
            task_id=t.task_id,
            dag=dag,
            bash_command=t.command,
            cwd=str(cfg.repo),
            env=t.env or None,
            append_env=True,
            retries=t.retries,
            execution_timeout=t.execution_timeout,
            sla=t.sla,
            pool=t.pool or "default_pool",
        )
    for t in tasks:
        for up in t.upstream:
            ops[up] >> ops[t.task_id]
    return dag
