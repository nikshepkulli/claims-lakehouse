"""The feature registry: complete, consistent with the tables, no PII, wired into docs and make."""

import copy
from pathlib import Path

import pytest
import yaml

from lakehouse.features import registry, store
from lakehouse.features.registry import FeatureRegistryError

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def feats():
    return registry.load()


@pytest.fixture()
def raw(tmp_path):
    d = registry.load_raw()

    def load(mutate):
        d2 = copy.deepcopy(d)
        mutate(d2)
        p = tmp_path / "r.yaml"
        p.write_text(yaml.safe_dump(d2))
        return registry.load(p)

    return load


def test_registry_loads_and_every_feature_has_owner_and_sources(feats):
    assert len(feats) >= 35
    assert all(f.owner and f.description and f.source_columns for f in feats)
    assert {f.entity for f in feats} == {"member", "provider", "claim"}


def test_no_pii_features(feats):
    assert all(f.classification != "PHI_LIKE" and f.identifier != "direct" for f in feats)
    # birth_year and enrollment_date (PHI_LIKE) are only reachable through the generalised band
    sensitive = [f for f in feats if any("birth_year" in s for s in f.source_columns)]
    assert [f.name for f in sensitive] == ["age_band"] and sensitive[0].transform == "generalise"
    assert not any("enrollment_date" in s for f in feats for s in f.source_columns)


def test_registry_matches_the_tables_the_build_writes(gold_export, tmp_path, feats):
    manifest = store.build(gold_export, tmp_path / "s", guard=False)
    con = store.connect()
    for entity, (table, _) in store.TABLES.items():
        store.read_table(con, tmp_path / "s", table)
        cols = [r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()]
        assert cols == registry.KEYS[entity] + registry.columns(entity, feats), table
    assert manifest["tables"]["claim_features"]["rows"] > 0


def test_labels_live_outside_the_feature_store(gold_export, tmp_path):
    store.build(gold_export, tmp_path / "s", guard=False)
    assert any((tmp_path / "s" / "labels" / "claim_denial").rglob("*.parquet"))
    con = store.connect()
    tables = store.TABLES.values()
    for name, _ in tables:
        store.read_table(con, tmp_path / "s", name)
        cols = {r[0] for r in con.execute(f"DESCRIBE {name}").fetchall()}
        assert not cols & {"is_denied", "label_available_at", "claim_status"}


def test_training_set_joins_labels_by_claim_only_here(gold_export, tmp_path):
    store.build(gold_export, tmp_path / "s", guard=False)
    t = store.training_set(tmp_path / "s")
    assert "is_denied" in t.schema.names
    assert t.num_rows > 1000
    assert 0.02 < sum(t.column("is_denied").to_pylist()) / t.num_rows < 0.2


@pytest.mark.parametrize(
    ("mutate", "msg"),
    [
        (lambda d: d["features"][0].update(classification="PHI_LIKE"), "PII features"),
        (lambda d: d["features"][0].update(identifier="direct"), "PII features"),
        (lambda d: d["features"][0].update(owner=""), "owner"),
        (lambda d: d["features"][0].update(transform="magic"), "transform"),
        (
            lambda d: d["features"][0].update(source_columns=["fact_claim.nope"]),
            "not in the classification",
        ),
        (
            lambda d: d["features"][0].update(source_columns=["dim_member.member_id"]),
            "direct identifier",
        ),
        (
            lambda d: d["features"][0].update(
                source_columns=["dim_member.birth_year"], transform="passthrough"
            ),
            "must be derived",
        ),
        (lambda d: d["features"].append(copy.deepcopy(d["features"][0])), "duplicate"),
        (lambda d: d["features"][0].pop("window"), "missing fields"),
    ],
)
def test_registry_rejects_violations(raw, mutate, msg):
    with pytest.raises(FeatureRegistryError, match=msg):
        raw(mutate)


def test_quasi_identifier_features_are_justified(feats):
    quasi = [f for f in feats if f.identifier == "quasi"]
    assert {f.name for f in quasi} == {"state", "age_band"}
    assert all(f.justification for f in quasi)


def test_outcome_dependent_features_are_flagged(feats):
    flagged = {f.name for f in feats if f.outcome_dependent}
    assert {"paid_sum_365d", "denied_count_365d", "denial_rate_365d"} <= flagged
    for f in feats:
        uses_outcome = any(
            s.endswith(("claim_status", "total_paid_amount")) for s in f.source_columns
        )
        assert uses_outcome == f.outcome_dependent, f.name


def test_docs_catalogue_and_make_wiring(feats):
    docs = (ROOT / "docs" / "features.md").read_text()
    for f in feats:
        assert f"`{f.name}`" in docs, f"docs/features.md does not list {f.entity}.{f.name}"
    mk = (ROOT / "mk" / "features.mk").read_text()
    for target in ("features:", "features-demo:", "features-verify:"):
        assert target in mk
    assert "duckdb" in (ROOT / "requirements-dev.txt").read_text()
