"""Structural tests on the pure pipeline spec (no Airflow needed)."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from claims_dags.spec import (
    CLI_FLAGS,
    DS,
    SPARK_POOL,
    PipelineConfig,
    batch_key,
    build_daily_pipeline,
    parse_cli,
)

CFG = PipelineConfig(repo=Path("/repo"), data_root=Path("/data/airflow"))
TASKS = {t.task_id: t for t in build_daily_pipeline(CFG)}
LAYER_ROOTS = set(CFG.layer_roots().values())


def _ancestors(tid: str) -> set[str]:
    out: set[str] = set()
    for up in TASKS[tid].upstream:
        out |= {up} | _ancestors(up)
    return out


def test_task_order_is_a_chain_raw_to_gates():
    order = ["land_raw", "bronze", "silver", "gold", "dbt_build", "quality_gates"]
    assert list(TASKS) == order
    for prev, nxt in zip(order, order[1:], strict=False):
        assert TASKS[nxt].upstream == (prev,)
    assert TASKS["land_raw"].upstream == ()
    # transitive: gates depend on every earlier layer
    assert _ancestors("quality_gates") == set(order[:-1])


def test_upstreams_exist_and_no_cycles():
    for t in TASKS.values():
        for up in t.upstream:
            assert up in TASKS
        assert t.task_id not in _ancestors(t.task_id)


def test_retries_timeouts_and_slas_are_set():
    for t in TASKS.values():
        assert t.execution_timeout > timedelta(0), t.task_id
        assert t.sla > timedelta(0), t.task_id
    for tid in ("bronze", "silver", "gold", "dbt_build"):
        assert TASKS[tid].retries >= 2, tid
    # slas are cumulative deadlines: later layers may not have an earlier deadline
    slas = [t.sla for t in TASKS.values()]
    assert slas == sorted(slas)
    # a deterministic quality gate is never retried
    assert TASKS["quality_gates"].retries == 0


def test_spark_layers_share_a_single_slot_pool():
    for tid in ("bronze", "silver", "gold"):
        assert TASKS[tid].pool == SPARK_POOL
    assert SPARK_POOL == "claims_spark"


def test_idempotency_key_is_the_logical_date():
    assert batch_key("2024-01-05") == "b20240105"  # same as lakehouse.bronze batch_id_for
    for tid in ("bronze", "silver", "gold", "quality_gates"):
        mod, flags = parse_cli(TASKS[tid].command)
        assert flags["--batch-date"] == DS == "{{ ds }}", tid
    for t in TASKS.values():
        for forbidden in ("date +", "now()", "$(date", "{{ ts", "data_interval_end"):
            assert forbidden not in t.command, (t.task_id, forbidden)


def test_land_raw_is_guarded_so_replays_do_not_rewrite_landed_files():
    cmd = TASKS["land_raw"].command
    assert cmd.startswith("test -d ") and " || " in cmd
    assert "--seed" in cmd  # deterministic generator


@pytest.mark.parametrize("tid", ["land_raw", "bronze", "silver", "gold"])
def test_no_task_writes_outside_its_declared_layer_root(tid):
    t = TASKS[tid]
    mod, flags = parse_cli(t.command)
    written = []
    for flag, value in flags.items():
        role, layer = CLI_FLAGS[mod].get(flag, ("arg", ""))
        if role == "write":
            assert layer == t.layer, (tid, flag)
            written.append(value)
    assert written, f"{tid} writes nothing?"
    assert set(written) <= set(t.writes) <= LAYER_ROOTS | {str(CFG.root / "dbt")}
    # reads come only from the layer directly upstream
    for flag, value in flags.items():
        if flag in CLI_FLAGS[mod] and CLI_FLAGS[mod][flag][0] == "read":
            assert value in LAYER_ROOTS and value not in t.writes


def test_dbt_and_gates_stay_inside_the_data_root():
    dbt = TASKS["dbt_build"]
    assert all(p.startswith(str(CFG.root)) for p in dbt.writes)
    assert dbt.env["DBT_DUCKDB_PATH"].startswith(str(CFG.root))
    # duckdb will not create the parent directory of its file
    assert f"mkdir -p {CFG.root}/dbt" in dbt.command
    # no claims batch yet (first date is dimensions only): skip, do not fail
    assert f"{CFG.layer_roots()['gold']}/export/fact_claim" in dbt.command
    assert "exit 99" in dbt.command
    assert dbt.env["GOLD_PATH"] == f"{CFG.layer_roots()['gold']}/export"
    _, flags = parse_cli(TASKS["quality_gates"].command)
    assert not [f for f in flags if CLI_FLAGS["claims_dags.gates"].get(f, ("", ""))[0] == "write"]


def test_layer_roots_are_distinct_siblings():
    roots = CFG.layer_roots()
    assert len(set(roots.values())) == 4
    assert all(Path(r).parent == CFG.root for r in roots.values())


def test_config_from_env():
    cfg = PipelineConfig.from_env(
        {"CLAIMS_DATA_ROOT": "/x", "CLAIMS_ROWS": "50", "CLAIMS_ALERT_WEBHOOK_URL": "https://h"}
    )
    assert cfg.root == Path("/x") and cfg.rows == 50 and cfg.alert_webhook_url == "https://h"
    assert "--rows 50" in build_daily_pipeline(cfg)[0].command


def test_parse_cli_ignores_guards():
    mod, flags = parse_cli("test -d /x || PYTHONPATH=src python -m claimsgen --out /x --quiet")
    assert mod == "claimsgen" and flags == {"--out": "/x"}


def test_python_tasks_extend_rather_than_replace_pythonpath():
    for tid in ("land_raw", "bronze", "silver", "gold", "quality_gates"):
        assert "${PYTHONPATH:+:$PYTHONPATH}" in TASKS[tid].command, tid
