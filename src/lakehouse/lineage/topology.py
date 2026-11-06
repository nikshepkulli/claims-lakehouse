"""Which datasets each layer job reads and writes (used by the wrapper defaults and the ledger
emitter). Pure data + URI joins; the table lists are pinned to the code by tests/lineage."""

from __future__ import annotations

from dataclasses import dataclass

from lakehouse.gold import schema as G

from .naming import DatasetRef, table_ref

RAW_DATASETS = ("members", "providers", "claims", "claim_lines", "diagnoses", "notes", "events")
BRONZE_TABLES = RAW_DATASETS
SILVER_TABLES = RAW_DATASETS
SILVER_OPS = ("silver_quarantine", "silver_reconciliation")
GOLD_INPUTS = ("members", "providers", "claims", "claim_lines", "diagnoses")
GOLD_OPS = ("gold_reconciliation", "gold_exclusions")
GOLD_TABLES = tuple(G.TABLES)


@dataclass(frozen=True)
class Roots:
    """Layer root URIs (``landing`` is the claimsgen output directory)."""

    landing: str | None = None
    bronze: str | None = None
    silver: str | None = None
    gold: str | None = None

    def layer_roots(self) -> dict[str, str]:
        return {
            k: v
            for k, v in (("bronze", self.bronze), ("silver", self.silver), ("gold", self.gold))
            if v
        }


@dataclass(frozen=True)
class IO:
    """A dataset plus the layer it belongs to (drives the schema facet)."""

    ref: DatasetRef
    layer: str
    table: str
    uri: str = ""  # the storage URI, for local schema inference


def _ios(root: str | None, layer: str, tables: tuple[str, ...]) -> list[IO]:
    return (
        [IO(table_ref(root, t), layer, t, f"{root.rstrip('/')}/{t}") for t in tables]
        if root
        else []
    )


def layer_io(layer: str, roots: Roots) -> tuple[list[IO], list[IO]]:
    """(inputs, outputs) of the batch job of ``layer``; ``([], [])`` for layers without a map."""
    if layer == "bronze":
        return _ios(roots.landing, "landing", RAW_DATASETS), _ios(
            roots.bronze, "bronze", BRONZE_TABLES
        )
    if layer == "silver":
        return _ios(roots.bronze, "bronze", BRONZE_TABLES), _ios(
            roots.silver, "silver", (*SILVER_TABLES, *SILVER_OPS)
        )
    if layer == "gold":
        return _ios(roots.silver, "silver", GOLD_INPUTS), _ios(
            roots.gold, "gold", (*GOLD_TABLES, *GOLD_OPS)
        )
    return [], []


def layer_tables(layer: str, roots: Roots) -> list[IO]:
    """Every dataset of a layer (the inputs of its data-quality run)."""
    root = {"bronze": roots.bronze, "silver": roots.silver, "gold": roots.gold}.get(layer)
    tables = {
        "bronze": BRONZE_TABLES,
        "silver": (*SILVER_TABLES, *SILVER_OPS),
        "gold": (*GOLD_TABLES, *GOLD_OPS),
    }.get(layer, ())
    return _ios(root, layer, tables)
