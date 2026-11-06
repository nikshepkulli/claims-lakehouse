"""The catalog is derived from the classification registry; PII never reaches the prompt."""

import pytest

from lakehouse.genai.catalog import build_catalog, column_denial
from lakehouse.genai.prompt import build_system_prompt
from lakehouse.governance import registry

DIRECT = [
    (t.name, c.name)
    for t in registry.load().values()
    for c in t.columns.values()
    if c.identifier == "direct"
]


def test_registry_has_direct_identifiers():
    assert ("dim_member", "member_id") in DIRECT and ("dim_provider", "provider_id") in DIRECT


@pytest.mark.parametrize("role", ["public", "analyst"])
def test_direct_and_masked_columns_never_exposed(role):
    cat = build_catalog(role, layers=("gold", "mart"))
    reg = registry.load()
    for tname, view in cat.tables.items():
        for cname in view.allowed:
            col = reg[tname].columns[cname]
            assert col.identifier != "direct"
            assert col.handling not in {"hash", "mask", "generalise", "drop"}


@pytest.mark.parametrize("role", ["public", "analyst"])
def test_prompt_never_mentions_denied_columns(role):
    cat = build_catalog(role)
    prompt = build_system_prompt(cat, {}, 100)
    for tname, view in cat.tables.items():
        for denied in view.denied:
            # a denied name may legitimately equal an allowed column elsewhere; check by table line
            line = next(x for x in prompt.splitlines() if x.startswith(f"- {tname}("))
            assert f" {denied} " not in f" {line.replace(',', ' ').replace('(', ' ')} "
    assert "member_id" not in prompt and "birth_year" not in prompt and "provider_id" not in prompt


def test_public_role_is_stricter_than_analyst():
    pub, ana = build_catalog("public"), build_catalog("analyst")
    for t in pub.tables:
        assert set(pub.tables[t].allowed) <= set(ana.tables[t].allowed)
    assert "total_paid_amount" not in pub.tables["fact_claim"].allowed
    assert "total_paid_amount" in ana.tables["fact_claim"].allowed


def test_unknown_role_rejected():
    with pytest.raises(ValueError):
        build_catalog("admin")
    col = next(iter(registry.load()["dim_date"].columns.values()))
    assert "unknown role" in column_denial(col, "root")


def test_marts_catalog_includes_semantic_metrics_and_no_ids():
    cat = build_catalog("analyst", layers=("mart",))
    assert "mart_claims_monthly" in cat.tables
    assert "provider_id" not in cat.tables["mart_provider_performance"].allowed
    assert "member_id" not in cat.tables["mart_member_risk"].allowed
    assert any(n == "denial_rate_metric" for n, _ in cat.metrics)
    assert "denial_rate_metric" in build_system_prompt(cat, {}, 10)
