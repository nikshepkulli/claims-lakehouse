"""Unit tests for the suite parser, schema contract frame and derived-column builders."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from lakehouse.gold import schema as G
from lakehouse.quality import derived as D
from lakehouse.quality import suites
from lakehouse.quality.loader import simple_type
from lakehouse.quality.runner import _expected, schema_frame

EXP_DIR = Path(__file__).resolve().parents[2] / "quality" / "expectations"


@pytest.mark.parametrize("layer", suites.LAYERS)
def test_shipped_suites_parse_and_name_real_expectations(layer):
    from great_expectations.expectations.registry import get_expectation_impl

    spec = suites.load_layer(layer, EXP_DIR)
    assert spec.tables
    for t in spec.tables.values():
        assert t.expectations or t.schema_from
        for e in t.expectations:
            assert e.severity in suites.SEVERITIES
            assert get_expectation_impl(e.type) is not None, e.type


def test_gold_suite_covers_every_star_table_and_marks_blocking():
    spec = suites.load_layer("gold", EXP_DIR)
    assert set(G.TABLES) <= set(spec.tables)
    sev = {e.severity for t in spec.tables.values() for e in t.expectations}
    assert "blocking" in sev


def test_parse_rejects_bad_severity_and_layer():
    with pytest.raises(suites.SuiteError, match="severity"):
        suites.parse(
            {
                "layer": "silver",
                "tables": {
                    "t": {
                        "expectations": [
                            {
                                "expectation": "expect_table_row_count_to_be_between",
                                "severity": "fatal",
                            }
                        ]
                    }
                },
            }
        )
    with pytest.raises(suites.SuiteError, match="layer"):
        suites.parse({"layer": "platinum", "tables": {"t": {}}})
    with pytest.raises(suites.SuiteError, match="expectation"):
        suites.parse({"layer": "silver", "tables": {"t": {"expectations": [{"column": "a"}]}}})


def test_gold_contract_columns_come_from_the_ddl_copy():
    t = suites.TableSpec("fact_claim", "fact_claim", schema_from="gold", exact=True)
    exp = _expected(t)
    assert list(exp) == G.column_names("fact_claim")
    assert exp["claim_id"] == ("string", False) and exp["primary_diagnosis_sk"][1] is True


def test_schema_frame_flags_missing_type_and_extra():
    t = suites.TableSpec("x", "x", columns={"a": "int", "b": "string"}, exact=True)
    f = schema_frame(t, {"a": "bigint", "z": "string"}).set_index("column")
    assert not f.loc["a", "type_ok"] and f.loc["a", "present_ok"]
    assert not f.loc["b", "present_ok"]
    assert not f.loc["z", "no_extra_ok"]


def test_simple_type_matches_spark_names():
    import pyarrow as pa

    cases = {
        pa.string(): "string",
        pa.int16(): "smallint",
        pa.int32(): "int",
        pa.int64(): "bigint",
        pa.decimal128(12, 2): "decimal(12,2)",
        pa.date32(): "date",
        pa.bool_(): "boolean",
        pa.timestamp("us"): "timestamp_ntz",
        pa.timestamp("us", tz="UTC"): "timestamp",
    }
    for arrow, spark in cases.items():
        assert simple_type(arrow) == spark


CTX = D.Ctx({"p": pd.DataFrame({"k": ["a", "b"]})}, datetime(2024, 1, 2))


def test_fk_and_allow_null():
    df = pd.DataFrame({"c": ["a", "z", None]})
    assert D.fk(df, {"name": "n", "column": "c", "ref": "p.k"}, CTX).tolist() == [
        True,
        False,
        False,
    ]
    assert D.fk(
        df, {"name": "n", "column": "c", "ref": "p.k", "allow_null": True}, CTX
    ).tolist() == [True, False, True]


def test_eval_where_and_errors():
    df = pd.DataFrame({"a": [1, 2], "b": [1, 3]})
    assert D.eval_(df, {"name": "n", "expr": "a == b"}, CTX).tolist() == [True, False]
    assert D.eval_(df, {"name": "n", "expr": "a == b", "where": "a > 1"}, CTX).tolist() == [
        True,
        False,
    ]
    with pytest.raises(D.DerivedError):
        D.eval_(df, {"name": "n", "expr": "nope == 1"}, CTX)
    with pytest.raises(D.DerivedError):
        D.add_derived(df, [{"name": "n", "kind": "mystery"}], CTX)


def test_age_hours():
    df = pd.DataFrame({"t": [datetime(2024, 1, 1, 0), datetime(2024, 1, 2)]})
    assert D.age_hours(df, {"name": "n", "column": "t"}, CTX).tolist() == [24.0, 0.0]


def test_child_sums_flags_total_and_count_mismatch():
    lines = pd.DataFrame({"claim_id": ["c1", "c1", "c2"], "amt": [1.0, 2.0, 5.0]})
    ctx = D.Ctx({"lines": lines}, datetime(2024, 1, 1))
    heads = pd.DataFrame({"claim_id": ["c1", "c2", "c3"], "total": [3.0, 4.0, 0.0], "n": [2, 1, 0]})
    spec = {
        "name": "n",
        "child": "lines",
        "key": "claim_id",
        "child_key": "claim_id",
        "count": "n",
        "sums": {"total": "amt"},
    }
    assert D.child_sums(heads, spec, ctx).tolist() == [True, False, True]


def test_scd2_detects_gap_overlap_and_double_current():
    t0, t1, t2 = datetime(2020, 1, 1), datetime(2021, 1, 1), datetime(2022, 1, 1)
    spec = {"name": "n", "key": "k", "from_col": "f", "to_col": "t", "current_col": "c"}

    def ok(rows):
        df = pd.DataFrame(rows, columns=["k", "f", "t", "c"])
        return D.scd2(df, spec, CTX).tolist()

    assert ok([("a", t0, t1, False), ("a", t1, None, True)]) == [True, True]
    assert not all(ok([("a", t0, t1, False), ("a", t2, None, True)]))  # gap
    assert not all(ok([("a", t0, t2, False), ("a", t1, None, True)]))  # overlap
    assert not all(ok([("a", t0, None, True), ("a", t1, None, True)]))  # two current
    assert not all(ok([("a", t0, t1, True), ("a", t1, None, True)]))  # flag disagrees
