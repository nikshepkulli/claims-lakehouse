"""Leakage- and privacy-aware feature allow-list taken from ``governance/feature_registry.yaml``.

The model may read only registry features of the claim table (own, ``m_`` member and ``p_``
provider columns). Never allowed: the label, join keys (``claim_id``, ``member_id``,
``provider_id``), ``as_of``, any PHI_LIKE or direct-identifier feature, and any column the
registry does not list. Quasi-identifier features (state, age band) are excluded by default and
need an explicit opt-in (``include_quasi``).
"""

from __future__ import annotations

from dataclasses import dataclass

from lakehouse.features import registry

LABEL = "is_denied"
KEY_COLUMNS = ("claim_id", "member_id", "provider_id", "as_of")
PREFIX = {"member": "m_", "provider": "p_", "claim": ""}


class FeatureGuardError(ValueError):
    """A forbidden column (PII, key, label, unknown) was about to enter the model."""


@dataclass(frozen=True)
class FeatureSpec:
    name: str  # column name in claim_features / the training set
    kind: str  # "numeric" | "categorical"
    entity: str
    source: str  # registry feature name
    outcome_dependent: bool
    quasi: bool


def _kind(dtype: str) -> str:
    return "categorical" if dtype == "string" else "numeric"


def catalogue(
    feats: list[registry.Feature] | None = None,
) -> dict[str, tuple[registry.Feature, str]]:
    """Column name in the claim table -> (registry feature, entity)."""
    feats = feats if feats is not None else registry.load()
    return {PREFIX[f.entity] + f.name: (f, f.entity) for f in feats}


def _is_pii(f: registry.Feature) -> bool:
    return f.classification == "PHI_LIKE" or f.identifier == "direct" or f.handling in {"drop"}


def allow_list(
    feats: list[registry.Feature] | None = None,
    *,
    include_quasi: bool = False,
    override: list[str] | None = None,
) -> list[FeatureSpec]:
    """The model's feature list, ordered as the registry (stable). ``override`` narrows it.

    Raises ``FeatureGuardError`` if the registry itself contains a PII feature (fail loud, do not
    silently drop) or ``override`` names a forbidden column.
    """
    cat = catalogue(feats)
    for col, (f, _) in cat.items():
        if _is_pii(f):
            raise FeatureGuardError(
                f"registry feature {col} is PII ({f.classification}/{f.identifier})"
            )
    names = (
        list(override)
        if override
        else [c for c, (f, _) in cat.items() if include_quasi or f.identifier != "quasi"]
    )
    assert_safe(names, feats)
    return [
        FeatureSpec(
            name=c,
            kind=_kind(cat[c][0].dtype),
            entity=cat[c][1],
            source=cat[c][0].name,
            outcome_dependent=cat[c][0].outcome_dependent,
            quasi=cat[c][0].identifier == "quasi",
        )
        for c in names
    ]


def assert_safe(names: list[str], feats: list[registry.Feature] | None = None) -> None:
    """Raise ``FeatureGuardError`` unless every name is a non-PII registry claim-table feature."""
    cat = catalogue(feats)
    bad: list[str] = []
    if len(set(names)) != len(names):
        bad.append("duplicate feature names")
    for n in names:
        if n == LABEL or n.endswith("is_denied"):
            bad.append(f"{n}: label column")
        elif n in KEY_COLUMNS:
            bad.append(f"{n}: join key / timestamp, not a model input")
        elif n not in cat:
            bad.append(f"{n}: not in the feature registry")
        elif _is_pii(cat[n][0]):
            bad.append(f"{n}: PII feature ({cat[n][0].classification}/{cat[n][0].identifier})")
    if not names:
        bad.append("empty feature list")
    if bad:
        raise FeatureGuardError("; ".join(bad))
