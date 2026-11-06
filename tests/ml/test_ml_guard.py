import dataclasses

import pytest

from lakehouse.features import registry
from lakehouse.ml import guard
from lakehouse.ml.guard import FeatureGuardError, allow_list, assert_safe


def _feats():
    return registry.load()


def test_default_allow_list_is_clean_registry_claim_columns():
    feats = _feats()
    specs = allow_list(feats)
    names = [s.name for s in specs]
    assert names and len(names) == len(set(names))
    assert not set(names) & set(guard.KEY_COLUMNS) and guard.LABEL not in names
    cat = guard.catalogue(feats)
    for s in specs:
        f = cat[s.name][0]
        assert f.classification != "PHI_LIKE" and f.identifier not in {"direct", "quasi"}
    # every name exists as a column of the real claim table
    assert set(names) <= set(registry.columns("claim", feats))


def test_quasi_identifiers_are_opt_in():
    base = {s.name for s in allow_list()}
    with_q = {s.name for s in allow_list(include_quasi=True)}
    assert {"m_state", "m_age_band"} <= with_q - base


@pytest.mark.parametrize("bad", ["is_denied", "claim_id", "member_id", "provider_id", "as_of"])
def test_labels_and_keys_are_rejected(bad):
    with pytest.raises(FeatureGuardError):
        assert_safe(["billed_amount", bad])


def test_unknown_and_empty_and_duplicate_rejected():
    for bad in (["birth_year"], ["billed_amount", "billed_amount"], [], ["ssn"]):
        with pytest.raises(FeatureGuardError):
            assert_safe(bad)


def test_registry_pii_feature_cannot_enter_the_list():
    feats = _feats()
    i = next(i for i, f in enumerate(feats) if f.entity == "claim" and f.name == "billed_amount")
    for tweak in ({"classification": "PHI_LIKE"}, {"identifier": "direct"}):
        poisoned = list(feats)
        poisoned[i] = dataclasses.replace(feats[i], **tweak)
        with pytest.raises(FeatureGuardError, match="PII"):
            allow_list(poisoned)  # the whole list refuses to build
        with pytest.raises(FeatureGuardError, match="PII"):
            assert_safe(["billed_amount"], poisoned)  # and so does an explicit override


def test_override_is_still_guarded():
    assert [s.name for s in allow_list(override=["billed_amount", "line_count"])] == [
        "billed_amount",
        "line_count",
    ]
    with pytest.raises(FeatureGuardError):
        allow_list(override=["billed_amount", "member_id"])


def test_kinds_follow_registry_dtypes():
    kinds = {s.name: s.kind for s in allow_list(include_quasi=True)}
    assert kinds["claim_type"] == "categorical" and kinds["billed_amount"] == "numeric"
    assert kinds["m_has_diabetes"] == "numeric" and kinds["p_specialty"] == "categorical"
