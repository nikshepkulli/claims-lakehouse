"""Positive and negative tests for the GE suites.

The clean fixture must pass with zero failures (so a green run means something); every injected
defect must make the matching layer exit non-zero (blocking) or warn (non-blocking). The negative
cases are the ticket's "known injected defects are detected" acceptance test.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from lakehouse.quality.cli import main

from . import fixture as F

ROOT = Path(__file__).resolve().parents[2]
NOW = "2024-06-01T12:00:00"


@pytest.fixture(scope="module")
def clean(tmp_path_factory) -> Path:
    r = tmp_path_factory.mktemp("lake")
    F.build_bronze(r / "bronze")
    F.build_silver(r / "silver")
    F.build_gold(r / "gold")
    return r


@pytest.fixture
def lake(tmp_path, clean) -> Path:
    """A private copy of the clean lakehouse a test may corrupt."""
    import shutil

    dst = tmp_path / "lake"
    shutil.copytree(clean, dst)
    return dst


def run(
    lake: Path, layer: str, tmp_path: Path, *extra: str, docs: bool = False
) -> tuple[int, dict]:
    out = tmp_path / "dq"
    argv = ["run", "--layer", layer, "--root", str(lake / layer), "--out", str(out), "--now", NOW]
    if not docs:
        argv.append("--no-docs")
    code = main([*argv, *extra])
    return code, json.loads((out / f"{layer}-result.json").read_text())


def mutate(lake: Path, layer: str, table: str, fn) -> None:
    """Rewrite a Delta table after ``fn(rows)`` edited its rows (list of dicts), schema kept."""
    path = str(lake / layer / table)
    tbl = DeltaTable(path).to_pyarrow_table()
    rows = tbl.to_pylist()
    edited = fn(rows)
    rows = rows if edited is None else edited
    write_deltalake(path, pa.Table.from_pylist(rows, schema=tbl.schema), mode="overwrite")


def failed(result: dict, blocking: bool = True) -> set[tuple[str, str]]:
    """(table, 'type:column') of failing expectations of the given severity."""
    sev = "blocking" if blocking else "warn"
    return {
        (t, f"{e['type']}:{e['kwargs'].get('column') or e['kwargs'].get('column_list')}")
        for t, tr in result["tables"].items()
        for e in tr["expectations"]
        if not e["success"] and e["severity"] == sev
    }


# ------------------------------------------------------------------ clean fixture passes


@pytest.mark.parametrize("layer", ["bronze", "silver", "gold"])
def test_clean_fixture_passes_with_no_failures(lake, tmp_path, layer):
    code, res = run(lake, layer, tmp_path)
    assert code == 0, failed(res)
    assert res["success"] is True
    assert res["summary"]["failed_blocking"] == 0 and res["summary"]["failed_warn"] == 0
    assert res["summary"]["expectations"] > 20


def test_result_json_is_machine_readable(lake, tmp_path):
    _, res = run(lake, "silver", tmp_path)
    assert res["layer"] == "silver" and res["great_expectations_version"]
    claims = res["tables"]["claims"]
    assert claims["status"] == "validated" and claims["rows"] == 4 and claims["storage"] == "delta"
    e = claims["expectations"][0]
    assert {"type", "kwargs", "severity", "success", "unexpected_count", "samples"} <= set(e)
    json.dumps(res)  # strictly serialisable


def test_data_docs_are_built(lake, tmp_path):
    run(lake, "gold", tmp_path, docs=True)
    index = tmp_path / "dq" / "data_docs" / "index.html"
    assert index.is_file() and "gold" in index.read_text()


# ------------------------------------------------------------------ silver defects (blocking)


def _set(table_rows, i, **kw):
    table_rows[i].update(kw)


@pytest.mark.parametrize(
    ("table", "edit", "expected"),
    [
        (
            "claims",
            lambda r: _set(r, 1, claim_id=r[0]["claim_id"]),
            ("claims", "expect_column_values_to_be_unique:claim_id"),
        ),
        (
            "claims",
            lambda r: _set(r, 0, member_id="M999999999"),
            ("claims", "expect_column_values_to_be_in_set:member_resolves"),
        ),
        (
            "claims",
            lambda r: _set(r, 0, claim_status="MAYBE"),
            ("claims", "expect_column_values_to_be_in_set:claim_status"),
        ),
        (
            "claim_lines",
            lambda r: _set(r, 0, paid_amount=F.money("-5.00")),
            ("claim_lines", "expect_column_values_to_be_between:paid_amount"),
        ),
        (
            "claim_lines",
            lambda r: _set(r, 0, procedure_code="ABCDE"),
            ("claim_lines", "expect_column_values_to_match_regex:procedure_code"),
        ),
        (
            "claim_lines",
            lambda r: _set(r, 0, claim_id="CLM_NOPE"),
            ("claim_lines", "expect_column_values_to_be_in_set:claim_resolves"),
        ),
        (
            "diagnoses",
            lambda r: _set(r, 0, icd10_code="123.45"),
            ("diagnoses", "expect_column_values_to_match_regex:icd10_code"),
        ),
        (
            "diagnoses",
            lambda r: _set(r, 1, dx_seq=1),
            ("diagnoses", "expect_compound_columns_to_be_unique:['claim_id', 'dx_seq']"),
        ),
        (
            "members",
            lambda r: _set(r, 0, sex="X"),
            ("members", "expect_column_values_to_be_in_set:sex"),
        ),
        (
            "members",
            lambda r: _set(r, 0, member_id=None),
            ("members", "expect_column_values_to_not_be_null:member_id"),
        ),
        (
            "silver_reconciliation",
            lambda r: _set(r, 0, silver_rows=r[0]["silver_rows"] + 1),
            ("silver_reconciliation", "expect_column_values_to_be_in_set:identity_recomputed"),
        ),
        (
            "silver_reconciliation",
            lambda r: _set(r, 0, identity_ok=False),
            ("silver_reconciliation", "expect_column_values_to_be_in_set:identity_ok"),
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_silver_injected_defect_is_caught(lake, tmp_path, table, edit, expected, capsys):
    mutate(lake, "silver", table, edit)
    code, res = run(lake, "silver", tmp_path)
    assert code == 1
    assert expected in failed(res), failed(res)
    assert "RESULT: FAILED" in capsys.readouterr().out


def test_silver_unnoticed_row_loss_is_caught_by_recount(lake, tmp_path):
    """A silver row deleted after reconciliation was recorded: recorded count != rows present."""
    mutate(lake, "silver", "claim_lines", lambda rows: rows[:-1])
    code, res = run(lake, "silver", tmp_path)
    assert code == 1
    assert (
        "silver_reconciliation",
        "expect_column_values_to_be_in_set:silver_rows_match_table",
    ) in failed(res)


def test_silver_missing_required_table_is_blocking(lake, tmp_path):
    import shutil

    shutil.rmtree(lake / "silver" / "providers")
    code, res = run(lake, "silver", tmp_path)
    assert code == 1
    assert res["tables"]["providers"]["status"] == "missing"
    assert not res["success"]


def test_silver_empty_table_fails_volume_bound(lake, tmp_path):
    mutate(lake, "silver", "members", lambda rows: [])
    code, res = run(lake, "silver", tmp_path)
    assert code == 1
    assert ("members", "expect_table_row_count_to_be_between:None") in failed(res)


# ------------------------------------------------------------------ gold defects (blocking)


@pytest.mark.parametrize(
    ("table", "edit", "expected"),
    [
        (
            "fact_claim",
            lambda r: _set(r, 0, member_sk="f" * 64),
            ("fact_claim", "expect_column_values_to_be_in_set:member_resolves"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 0, provider_sk="f" * 64),
            ("fact_claim", "expect_column_values_to_be_in_set:provider_resolves"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 0, service_date_key=19990101),
            ("fact_claim", "expect_column_values_to_be_in_set:service_date_resolves"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 1, claim_id=r[0]["claim_id"]),
            ("fact_claim", "expect_column_values_to_be_unique:claim_id"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 0, total_billed_amount=F.money("999.00")),
            ("fact_claim", "expect_column_values_to_be_in_set:header_equals_lines"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 0, line_count=5),
            ("fact_claim", "expect_column_values_to_be_in_set:header_equals_lines"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 0, member_sk=None),
            ("fact_claim", "expect_column_values_to_not_be_null:member_sk"),
        ),
        (
            "fact_claim",
            lambda r: _set(r, 0, claim_status="PENDING"),
            ("fact_claim", "expect_column_values_to_be_in_set:claim_status"),
        ),
        (
            "fact_claim_line",
            lambda r: _set(r, 0, claim_id="CLM_NOPE"),
            ("fact_claim_line", "expect_column_values_to_be_in_set:claim_resolves"),
        ),
        (
            "fact_claim_line",
            lambda r: _set(r, 0, billed_amount=F.money("-1.00")),
            ("fact_claim_line", "expect_column_values_to_be_between:billed_amount"),
        ),
        (
            "dim_member",
            lambda r: _set(r, 3, is_current=False, effective_to=None),
            ("dim_member", "expect_column_values_to_be_in_set:history_ok"),
        ),
        (
            "dim_member",
            lambda r: _set(r, 0, effective_to=None, is_current=True),
            ("dim_member", "expect_column_values_to_be_in_set:history_ok"),
        ),
        (
            "dim_member",
            lambda r: _set(r, 1, member_sk=r[0]["member_sk"]),
            ("dim_member", "expect_column_values_to_be_unique:member_sk"),
        ),
        (
            "dim_provider",
            lambda r: _set(r, 0, cost_tier="PLATINUM"),
            ("dim_provider", "expect_column_values_to_be_in_set:cost_tier"),
        ),
        (
            "gold_reconciliation",
            lambda r: _set(r, 0, gold_rows=r[0]["gold_rows"] + 1),
            ("gold_reconciliation", "expect_column_values_to_be_in_set:identity_recomputed"),
        ),
        (
            "gold_reconciliation",
            lambda r: _set(r, 0, key_violations=3),
            ("gold_reconciliation", "expect_column_values_to_be_between:key_violations"),
        ),
        (
            "gold_exclusions",
            lambda r: _set(r, 0, reason_code="UNRESOLVED_MEMBER"),
            ("gold_exclusions", "expect_column_values_to_not_be_in_set:reason_code"),
        ),
    ],
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_gold_injected_defect_is_caught(lake, tmp_path, table, edit, expected):
    mutate(lake, "gold", table, edit)
    code, res = run(lake, "gold", tmp_path)
    assert code == 1
    assert expected in failed(res), failed(res)


def test_gold_extra_column_breaks_the_contract(lake, tmp_path):
    path = str(lake / "gold" / "dim_diagnosis")
    tbl = DeltaTable(path).to_pyarrow_table().append_column("shadow", pa.array(["x"] * 2))
    write_deltalake(path, tbl, mode="overwrite", schema_mode="overwrite")
    code, res = run(lake, "gold", tmp_path)
    assert code == 1
    failing = [e for e in res["tables"]["dim_diagnosis"]["expectations"] if not e["success"]]
    assert {e["kwargs"]["column"] for e in failing} == {"no_extra_ok", "classified_ok"}


def test_gold_type_drift_breaks_the_contract(lake, tmp_path):
    path = str(lake / "gold" / "dim_date")
    tbl = DeltaTable(path).to_pyarrow_table()
    i = tbl.schema.get_field_index("quarter")
    tbl = tbl.set_column(i, "quarter", tbl.column("quarter").cast(pa.int64()))
    write_deltalake(path, tbl, mode="overwrite", schema_mode="overwrite")
    code, res = run(lake, "gold", tmp_path)
    assert code == 1
    failing = [e for e in res["tables"]["dim_date"]["expectations"] if not e["success"]]
    assert any(e["kwargs"].get("column") == "type_ok" for e in failing)


def test_gold_unclassified_column_is_caught(lake, tmp_path, monkeypatch, tmp_path_factory):
    """A column missing from governance/data_classification.yaml cannot reach gold unnoticed."""
    import yaml

    reg = yaml.safe_load((ROOT / "governance" / "data_classification.yaml").read_text())
    del reg["tables"]["dim_diagnosis"]["columns"]["description"]
    p = tmp_path_factory.mktemp("reg") / "registry.yaml"
    p.write_text(yaml.safe_dump(reg))
    monkeypatch.setenv("LAKEHOUSE_CLASSIFICATION_REGISTRY", str(p))
    code, res = run(lake, "gold", tmp_path)
    assert code == 1
    failing = [e for e in res["tables"]["dim_diagnosis"]["expectations"] if not e["success"]]
    assert [e["kwargs"]["column"] for e in failing] == ["classified_ok"]


# ------------------------------------------------------------------ bronze + severity policy


def test_bronze_defects_warn_but_do_not_block(lake, tmp_path, capsys):
    """Bronze keeps the generator's defects; they are reported as warnings, exit stays 0."""
    mutate(lake, "bronze", "claims", lambda r: _set(r, 1, claim_id=r[0]["claim_id"]))
    mutate(lake, "bronze", "claim_lines", lambda r: _set(r, 0, procedure_code="0000"))
    code, res = run(lake, "bronze", tmp_path)
    assert code == 0 and res["success"] is True
    warned = failed(res, blocking=False)
    assert ("claims", "expect_column_values_to_be_unique:claim_id") in warned
    assert ("claim_lines", "expect_column_values_to_match_regex:procedure_code") in warned
    assert "WARN" in capsys.readouterr().out


def test_fail_on_warn_promotes_warnings(lake, tmp_path):
    mutate(lake, "bronze", "claims", lambda r: _set(r, 1, claim_id=r[0]["claim_id"]))
    code, _ = run(lake, "bronze", tmp_path, "--fail-on-warn")
    assert code == 1


def test_bronze_broken_batch_lineage_blocks(lake, tmp_path):
    mutate(lake, "bronze", "members", lambda r: _set(r, 0, _batch_id="batch-1"))
    code, res = run(lake, "bronze", tmp_path)
    assert code == 1
    assert ("members", "expect_column_values_to_match_regex:_batch_id") in failed(res)


def test_bronze_lost_column_blocks(lake, tmp_path):
    path = str(lake / "bronze" / "providers")
    tbl = DeltaTable(path).to_pyarrow_table().drop_columns(["cost_tier"])
    write_deltalake(path, tbl, mode="overwrite", schema_mode="overwrite")
    code, _ = run(lake, "bronze", tmp_path)
    assert code == 1


def test_freshness_is_a_warning(lake, tmp_path):
    out = tmp_path / "dq"
    code = main(
        [
            "run",
            "--layer",
            "silver",
            "--root",
            str(lake / "silver"),
            "--out",
            str(out),
            "--now",
            "2024-09-01T00:00:00",
            "--no-docs",
        ]
    )
    res = json.loads((out / "silver-result.json").read_text())
    assert code == 0
    assert any(
        e == ("claims", "expect_column_min_to_be_between:silver_age_hours")
        for e in failed(res, blocking=False)
    )


# ------------------------------------------------------------------ CLI contract


def test_cli_subprocess_exit_codes(lake, tmp_path):
    env = {"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"}
    base = [
        sys.executable,
        "-m",
        "lakehouse.quality",
        "run",
        "--layer",
        "silver",
        "--root",
        str(lake / "silver"),
        "--out",
        str(tmp_path / "dq"),
        "--now",
        NOW,
        "--no-docs",
    ]
    ok = subprocess.run(base, capture_output=True, text=True, env=env, cwd=ROOT)
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "RESULT: ok" in ok.stdout
    mutate(lake, "silver", "claims", lambda r: _set(r, 0, claim_status="MAYBE"))
    bad = subprocess.run(base, capture_output=True, text=True, env=env, cwd=ROOT)
    assert bad.returncode == 1
    assert "FAIL claims.claim_status" in bad.stdout


def test_cli_usage_errors_exit_2(tmp_path, capsys):
    assert (
        main(
            [
                "run",
                "--layer",
                "silver",
                "--root",
                str(tmp_path),
                "--out",
                str(tmp_path / "o"),
                "--expectations",
                str(tmp_path),
                "--no-docs",
            ]
        )
        == 2
    )
    assert "no suite file" in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:
        main(["run", "--layer", "platinum", "--root", "x"])
    assert e.value.code == 2


def test_empty_root_is_all_missing_and_blocking(tmp_path):
    code = main(
        [
            "run",
            "--layer",
            "gold",
            "--root",
            str(tmp_path / "nothing"),
            "--out",
            str(tmp_path / "o"),
            "--no-docs",
        ]
    )
    assert code == 1
