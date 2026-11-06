"""Load and validate the data-classification registry (governance/data_classification.yaml)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

CLASSIFICATIONS = ("PUBLIC", "INTERNAL", "CONFIDENTIAL", "PHI_LIKE")
IDENTIFIERS = ("direct", "quasi", "none")
HANDLINGS = ("none", "token", "mask", "generalise", "drop")
LAYERS = ("gold", "mart")

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PATH = REPO_ROOT / "governance" / "data_classification.yaml"


class RegistryError(ValueError):
    """The registry file is malformed or violates a handling rule."""


@dataclass(frozen=True)
class Column:
    table: str
    name: str
    classification: str
    identifier: str
    handling: str
    justification: str = ""

    @property
    def sensitive(self) -> bool:
        """Needs an explicit decision: identifies a person, alone or in combination."""
        return self.identifier != "none" or self.classification == "PHI_LIKE"


@dataclass(frozen=True)
class Table:
    name: str
    layer: str
    columns: dict[str, Column]


def registry_path() -> Path:
    return Path(os.environ.get("LAKEHOUSE_CLASSIFICATION_REGISTRY", DEFAULT_PATH))


def load(path: Path | None = None) -> dict[str, Table]:
    """Parse and validate the registry. Raises RegistryError on any rule violation."""
    raw = yaml.safe_load((path or registry_path()).read_text())
    if not isinstance(raw, dict) or raw.get("version") != 1 or "tables" not in raw:
        raise RegistryError("registry must be a mapping with version: 1 and tables:")
    tables: dict[str, Table] = {}
    for tname, tdef in raw["tables"].items():
        if tdef.get("layer") not in LAYERS:
            raise RegistryError(f"{tname}: layer must be one of {LAYERS}")
        cols: dict[str, Column] = {}
        for cname, cdef in (tdef.get("columns") or {}).items():
            col = Column(
                tname,
                cname,
                cdef.get("classification", ""),
                cdef.get("identifier", ""),
                cdef.get("handling", ""),
                (cdef.get("justification") or "").strip(),
            )
            _validate(col)
            cols[cname] = col
        if not cols:
            raise RegistryError(f"{tname}: no columns")
        tables[tname] = Table(tname, tdef["layer"], cols)
    return tables


def _validate(col: Column) -> None:
    where = f"{col.table}.{col.name}"
    if col.classification not in CLASSIFICATIONS:
        raise RegistryError(
            f"{where}: classification {col.classification!r} not in {CLASSIFICATIONS}"
        )
    if col.identifier not in IDENTIFIERS:
        raise RegistryError(f"{where}: identifier {col.identifier!r} not in {IDENTIFIERS}")
    if col.handling not in HANDLINGS:
        raise RegistryError(f"{where}: handling {col.handling!r} not in {HANDLINGS}")
    if col.classification == "PUBLIC" and col.identifier != "none":
        raise RegistryError(f"{where}: a PUBLIC column cannot be an identifier")
    if col.identifier == "direct" and col.handling == "none":
        raise RegistryError(f"{where}: a direct identifier must be tokenised, masked or dropped")
    if col.handling == "token" and not col.justification:
        raise RegistryError(f"{where}: handling token needs a justification (which key, where)")
    if col.sensitive and col.handling == "none" and not col.justification:
        raise RegistryError(f"{where}: sensitive column with handling none needs a justification")
