"""The reviewed column map must stay in lock-step with the gold schema (cannot drift)."""

from pathlib import Path

import pytest
import yaml

from lakehouse.gold import schema as G
from lakehouse.lineage import column_lineage as CL
from lakehouse.lineage.naming import table_ref

ROOT = Path(__file__).resolve().parents[2]


def test_every_gold_table_and_column_is_mapped():
    m = CL.load()
    assert set(m.tables) == set(G.TABLES)
    for t, cols in G.TABLES.items():
        assert set(m.tables[t]) == {c.name for c in cols}, t


def test_problems_is_empty_for_the_committed_mapping():
    assert CL.problems() == []


def test_every_column_is_sourced_xor_derived():
    for t, cols in CL.load().tables.items():
        for c, m in cols.items():
            assert bool(m.sources) != bool(m.derived), f"{t}.{c}"


def test_source_columns_exist_in_the_silver_contract():
    suite = yaml.safe_load((ROOT / "quality/expectations/silver.yaml").read_text())["tables"]
    for ref in CL.load().all_source_refs():
        layer, tbl, col = CL.split_source(ref)
        assert layer == "silver", ref
        assert col in suite[tbl]["columns"], ref


def _write(tmp_path, mutate):
    raw = yaml.safe_load(CL.DEFAULT_PATH.read_text())
    mutate(raw["tables"])
    p = tmp_path / "m.yaml"
    p.write_text(yaml.safe_dump(raw))
    return CL.load(p)


def test_drift_new_gold_column_not_mapped_is_caught(tmp_path):
    m = _write(tmp_path, lambda t: t["fact_claim"].pop("claim_status"))
    assert any("fact_claim.claim_status: gold column not mapped" in p for p in CL.problems(m))


def test_drift_stale_column_is_caught(tmp_path):
    m = _write(tmp_path, lambda t: t["dim_member"].update(ghost={"derived": "x"}))
    assert any("dim_member.ghost: mapped but not a gold column" in p for p in CL.problems(m))


def test_drift_unknown_source_column_is_caught(tmp_path):
    m = _write(
        tmp_path, lambda t: t["fact_claim"]["claim_id"].update(sources=["silver.claims.nope"])
    )
    assert any("silver.claims.nope does not exist" in p for p in CL.problems(m))


def test_drift_missing_table_is_caught(tmp_path):
    m = _write(tmp_path, lambda t: t.pop("dim_date"))
    assert any("dim_date has no column lineage" in p for p in CL.problems(m))


@pytest.mark.parametrize(
    "spec",
    [
        {},
        {"sources": ["silver.claims.claim_id"], "derived": "x"},
        {"sources": ["bad"]},
        {"sources": ["silver.claims.claim_id"], "transformation": "MAGIC"},
    ],
)
def test_malformed_entries_are_rejected(tmp_path, spec):
    p = tmp_path / "m.yaml"
    p.write_text(yaml.safe_dump({"version": 1, "tables": {"t": {"c": spec}}}))
    with pytest.raises(CL.ColumnLineageError):
        CL.load(p)


def test_facet_resolves_sources_to_input_dataset_identities(tmp_path):
    roots = {"silver": str(tmp_path / "silver")}
    f = CL.facet("fact_claim", roots)
    ins = f["fields"]["member_sk"]["inputFields"]
    claims = table_ref(roots["silver"], "claims")
    direct = next(i for i in ins if i["field"] == "member_id" and i["name"] == claims.name)
    assert direct["namespace"] == claims.namespace
    t = direct["transformations"][0]
    assert (t["type"], t["subtype"], t["masking"]) == ("DIRECT", "TRANSFORMATION", True)
    assert any(
        i["field"] == "service_date" and i["transformations"][0]["type"] == "INDIRECT" for i in ins
    )
    assert "loaded_at" not in f["fields"]  # derived columns have no input field
    assert f["fields"]["claim_id"]["inputFields"][0]["transformations"][0]["subtype"] == "IDENTITY"


def test_facet_is_none_for_unmapped_table_and_needs_layer_root():
    assert CL.facet("silver_quarantine", {"silver": "/s"}) is None
    assert CL.facet("dim_date", {"silver": "/s"}) is None  # all columns derived
    with pytest.raises(CL.ColumnLineageError):
        CL.facet("fact_claim", {})
