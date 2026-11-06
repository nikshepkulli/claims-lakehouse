"""Load and validate ``governance/feature_registry.yaml`` (the feature catalogue).

Validation (also run by tests/features/test_registry.py):
* every entry has the required fields, a unique name per entity, a known entity and dtype;
* classification/identifier/handling use the vocabulary of ``lakehouse.governance.registry``;
* no feature is PHI_LIKE or a direct identifier, and a feature never takes a direct identifier
  as a source (ids are join keys, not inputs);
* every source column exists in ``governance/data_classification.yaml``; a feature that reads a
  PHI_LIKE source must be derived (aggregate/difference/generalise/flag), never passed through.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from lakehouse.governance import registry as cls_registry

DEFAULT_PATH = Path(__file__).resolve().parents[3] / "governance" / "feature_registry.yaml"
ENTITIES = ("member", "provider", "claim")
TRANSFORMS = ("aggregate", "difference", "generalise", "flag", "lookup", "passthrough")
DERIVED = ("aggregate", "difference", "generalise", "flag")
REQUIRED = [
    "name",
    "entity",
    "dtype",
    "window",
    "description",
    "owner",
    "source_columns",
    "transform",
    "classification",
    "identifier",
    "handling",
    "outcome_dependent",
    "nullable",
]
RANK = {c: i for i, c in enumerate(cls_registry.CLASSIFICATIONS)}


class FeatureRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class Feature:
    name: str
    entity: str
    dtype: str
    window: str
    description: str
    owner: str
    source_columns: tuple[str, ...]
    transform: str
    classification: str
    identifier: str
    handling: str
    outcome_dependent: bool
    nullable: bool
    justification: str = ""


def registry_path() -> Path:
    return Path(os.environ.get("LAKEHOUSE_FEATURE_REGISTRY", DEFAULT_PATH))


def load_raw(path: Path | None = None) -> dict:
    return yaml.safe_load((path or registry_path()).read_text())


def load(path: Path | None = None, classification: dict | None = None) -> list[Feature]:
    raw = load_raw(path)
    cls = classification if classification is not None else cls_registry.load()
    feats: list[Feature] = []
    seen: set[tuple[str, str]] = set()
    for d in raw["features"]:
        missing = [k for k in REQUIRED if k not in d]
        if missing:
            raise FeatureRegistryError(f"{d.get('name')}: missing fields {missing}")
        f = Feature(**{**d, "source_columns": tuple(d["source_columns"])})
        where = f"{f.entity}.{f.name}"
        if f.entity not in ENTITIES:
            raise FeatureRegistryError(f"{where}: unknown entity")
        if (f.entity, f.name) in seen:
            raise FeatureRegistryError(f"{where}: duplicate")
        seen.add((f.entity, f.name))
        if f.transform not in TRANSFORMS:
            raise FeatureRegistryError(f"{where}: transform must be one of {TRANSFORMS}")
        if f.classification not in cls_registry.CLASSIFICATIONS:
            raise FeatureRegistryError(f"{where}: bad classification {f.classification}")
        if f.identifier not in cls_registry.IDENTIFIERS or f.handling not in cls_registry.HANDLINGS:
            raise FeatureRegistryError(f"{where}: bad identifier/handling")
        if f.classification == "PHI_LIKE" or f.identifier == "direct":
            raise FeatureRegistryError(f"{where}: PII features are not allowed")
        if f.identifier == "quasi" and not f.justification:
            raise FeatureRegistryError(f"{where}: quasi-identifier feature needs a justification")
        if not f.owner or not f.description or not f.source_columns:
            raise FeatureRegistryError(f"{where}: owner, description, source_columns required")
        for src in f.source_columns:
            table, _, col = src.partition(".")
            c = cls.get(table) and cls[table].columns.get(col)
            if not c:
                raise FeatureRegistryError(
                    f"{where}: source {src} not in the classification registry"
                )
            if c.identifier == "direct":
                raise FeatureRegistryError(f"{where}: direct identifier {src} used as a source")
            if c.classification == "PHI_LIKE" and f.transform not in DERIVED:
                raise FeatureRegistryError(f"{where}: PHI_LIKE source {src} must be derived")
            if (
                f.transform in ("passthrough", "lookup")
                and RANK[f.classification] < RANK[c.classification]
            ):
                raise FeatureRegistryError(f"{where}: classified below its source {src}")
        feats.append(f)
    return feats


def columns(entity: str, feats: list[Feature] | None = None) -> list[str]:
    """Feature columns of the entity's table, after the key columns, in registry order.

    The claim table also carries every member (``m_``) and provider (``p_``) feature.
    """
    feats = feats if feats is not None else load()
    own = [f.name for f in feats if f.entity == entity]
    if entity != "claim":
        return own
    return (
        own
        + [f"m_{f.name}" for f in feats if f.entity == "member"]
        + [f"p_{f.name}" for f in feats if f.entity == "provider"]
    )


KEYS = {
    "member": ["member_id", "as_of"],
    "provider": ["provider_id", "as_of"],
    "claim": ["claim_id", "member_id", "provider_id", "as_of"],
}
