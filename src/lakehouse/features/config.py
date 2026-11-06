"""Feature-build configuration and the constants every implementation must share."""

from __future__ import annotations

from dataclasses import asdict, dataclass

WINDOWS = (30, 90, 365)  # trailing windows in days: (as_of - w, as_of]
ADJUDICATED = ("PAID", "DENIED")  # claim_status values that carry an outcome
DENIED = "DENIED"
# A claim received on day D is visible from the END of D (event_ts = D 23:59:59), so an as_of at
# midnight of D never sees it, and a claim never sees itself or same-day siblings.
EVENT_TIME_OF_DAY_SECONDS = 86399
SCHEMA_VERSION = "features-v1"


@dataclass(frozen=True)
class FeatureConfig:
    """Knobs of the feature set. Part of the manifest, so a change is visible in the hash.

    outcome_lag_days: gold has no adjudication date, so the outcome of a claim (status, paid
        amount) is treated as known only ``outcome_lag_days`` after it was received. Outcome-
        based features only use claims whose outcome is known by ``as_of``; volume/billed
        features use receipt time. This is an assumption, not a measured property of the data.
    min_peer_claims: a provider needs this many claims in 365d to enter the peer distribution.
    min_peers: the peer group (same specialty) needs this many providers for a z-score.
    """

    outcome_lag_days: int = 30
    min_peer_claims: int = 5
    min_peers: int = 3

    def __post_init__(self) -> None:
        for k, v in asdict(self).items():
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise ValueError(f"{k} must be a non-negative int, got {v!r}")

    def as_dict(self) -> dict[str, int]:
        return asdict(self)
