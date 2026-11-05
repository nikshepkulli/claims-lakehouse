"""Generator configuration (the only place defaults live)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np

# Fixed stream ids so each concern has an independent, reproducible RNG.
STREAM_DIMS, STREAM_VOLUME, STREAM_DRIFT, STREAM_CHUNK = 1, 2, 3, 10


def rng_for(seed: int, stream: int, index: int = 0) -> np.random.Generator:
    """Independent RNG per (seed, stream, index): output never depends on run order."""
    return np.random.default_rng(np.random.SeedSequence([seed, stream, index]))


@dataclass(frozen=True)
class DefectRates:
    """Probability of each injected defect. Claim-level unless noted."""

    duplicate_claim: float = 0.005
    late_arrival: float = 0.02
    orphan_member: float = 0.003
    null_procedure_code: float = 0.004  # per claim line
    invalid_procedure_code: float = 0.004  # per claim line
    negative_amount: float = 0.003  # per claim line
    null_icd10: float = 0.004  # per diagnosis row
    invalid_icd10: float = 0.004  # per diagnosis row


@dataclass(frozen=True)
class GenConfig:
    rows: int  # number of distinct claims (headers); lines/diagnoses scale ~2.4x / ~2x
    out: Path
    seed: int = 42
    chunk_size: int = 250_000  # claims per chunk; bounds memory
    start_date: date = date(2024, 1, 1)
    days: int = 366
    formats: tuple[str, ...] = ("parquet", "csv")
    notes_rate: float = 0.25  # share of claims that get a clinical note
    overwrite: bool = False
    rates: DefectRates = field(default_factory=DefectRates)

    def __post_init__(self) -> None:
        if self.rows < 1000:
            raise ValueError("rows must be >= 1000")
        if self.chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        if not set(self.formats) <= {"parquet", "csv"} or not self.formats:
            raise ValueError("formats must be a non-empty subset of {parquet, csv}")

    @property
    def n_members(self) -> int:
        return int(min(max(self.rows // 6, 1_000), 5_000_000))

    @property
    def n_providers(self) -> int:
        return int(min(max(self.rows // 400, 200), 100_000))
