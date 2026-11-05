"""Silver CLI argument handling. Plain pytest: no Spark, no Java."""

from lakehouse.silver import cli


def test_defaults_and_flags():
    a = cli.parse_args(["--bronze", "b", "--silver", "s", "--batch-date", "all", "--optimize"])
    assert (a.bronze, a.silver, a.batch_date, a.optimize) == ("b", "s", "all", True)
    assert "claim_lines" in a.datasets.split(",")


def test_bad_batch_date_and_dataset_are_usage_errors(capsys):
    assert cli.main(["--bronze", "b", "--silver", "s", "--batch-date", "yesterday"]) == 2
    assert (
        cli.main(["--bronze", "b", "--silver", "s", "--batch-date", "all", "--datasets", "x"]) == 2
    )
    assert "must be YYYY-MM-DD" in capsys.readouterr().err


def test_rule_regexes_match_the_generator_docs():
    """The two code patterns are copied from docs/data-generator.md; guard against drift."""
    from pathlib import Path

    doc = (Path(__file__).resolve().parents[2] / "docs" / "data-generator.md").read_text()
    # rules.py imports pyspark, so read the constants textually instead of importing it
    src = (Path(cli.__file__).parent / "rules.py").read_text()
    for pattern in (r"^(\d{5}|[A-V]\d{4})$", r"^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$"):
        assert pattern in src
        assert pattern.replace("|", r"\|") in doc or pattern in doc
