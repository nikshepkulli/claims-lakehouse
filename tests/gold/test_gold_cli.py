"""Gold CLI argument handling and repo wiring (plain pytest, no Spark)."""

import re
from pathlib import Path

import pytest

from lakehouse.gold import cli, paths
from lakehouse.gold.schema import MIN_VACUUM_HOURS

ROOT = Path(__file__).resolve().parents[2]


def test_parse_args_defaults():
    a = cli.parse_args(["--silver", "s", "--gold", "g", "--batch-date", "all"])
    assert (a.silver, a.gold, a.batch_date) == ("s", "g", "all")
    assert not (a.no_export or a.optimize or a.vacuum)
    assert a.vacuum_retention_hours == MIN_VACUUM_HOURS == 168


def test_batch_date_is_required():
    with pytest.raises(SystemExit):
        cli.parse_args(["--silver", "s", "--gold", "g"])


def test_bad_batch_date_exits_2(capsys):
    assert cli.main(["--silver", "s", "--gold", "g", "--batch-date", "yesterday"]) == 2
    assert "YYYY-MM-DD" in capsys.readouterr().err


def test_vacuum_below_default_retention_is_refused(capsys):
    rc = cli.main(
        [
            "--silver",
            "s",
            "--gold",
            "g",
            "--batch-date",
            "2024-01-01",
            "--vacuum",
            "--vacuum-retention-hours",
            "1",
        ]
    )
    assert rc == 2
    assert "168" in capsys.readouterr().err


def test_paths_follow_the_bronze_convention():
    assert paths.table_uri("s3a://gold/", "fact_claim") == "s3a://gold/fact_claim"
    assert paths.export_uri("/data/gold", "dim_member") == "/data/gold/export/dim_member"


def test_gold_mk_registers_the_suite_and_the_targets():
    mk = (ROOT / "mk" / "gold.mk").read_text()
    assert "SPARK_TESTS += tests/gold" in mk
    for target in ("gold:", "gold-dbt:", "pipeline-e2e:"):
        assert re.search(rf"^{target}", mk, re.M), target


def test_ci_runs_gold_in_the_spark_matrix_and_the_e2e_job():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert re.search(r"suite:\s*\[[^\]]*\bgold\b", ci)
    assert "pipeline e2e" in ci
