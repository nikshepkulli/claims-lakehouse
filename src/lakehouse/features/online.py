"""Row-at-a-time ("online") feature computation in plain Python.

This is the serving-side reference: given a key and ``as_of`` it recomputes a feature vector
from the raw visible rows with ordinary loops, sharing NO SQL with the batch engine. The parity
test compares it to the stored batch features for sampled keys (training/serving parity); a
real low-latency service would hold the same logic against an online store (docs/features.md).
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal

from lakehouse.governance.generalise import age_band

from .chronic import CONDITIONS
from .config import ADJUDICATED, DENIED, WINDOWS, FeatureConfig

_CENT = Decimal("0.01")


def _q(x: Decimal) -> Decimal:
    return x.quantize(_CENT)


class OnlineFeatures:
    def __init__(
        self,
        claims: list[dict],
        dim_member: list[dict],
        dim_provider: list[dict],
        cfg: FeatureConfig,
    ):
        self.cfg = cfg
        self.by_member: dict[str, list[dict]] = defaultdict(list)
        self.by_provider: dict[str, list[dict]] = defaultdict(list)
        self.by_id: dict[str, dict] = {}
        for c in claims:
            self.by_member[c["member_id"]].append(c)
            self.by_provider[c["provider_id"]].append(c)
            self.by_id[c["claim_id"]] = c
        self.dim_member: dict[str, list[dict]] = defaultdict(list)
        self.dim_provider: dict[str, list[dict]] = defaultdict(list)
        for d in dim_member:
            self.dim_member[d["member_id"]].append(d)
        for d in dim_provider:
            self.dim_provider[d["provider_id"]].append(d)
        self._cond = {k: re.compile(v) for k, v in CONDITIONS.items()}

    @classmethod
    def from_connection(cls, con, cfg: FeatureConfig) -> OnlineFeatures:
        def rows(t: str) -> list[dict]:
            cur = con.execute(f"SELECT * FROM {t}")
            names = [d[0] for d in cur.description]
            return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]

        return cls(rows("src_claims"), rows("src_dim_member"), rows("src_dim_provider"), cfg)

    # -- helpers ------------------------------------------------------------------------------
    @staticmethod
    def _version(versions: list[dict], as_of: datetime) -> dict | None:
        hits = [
            v
            for v in versions
            if v["effective_from"] <= as_of
            and (v["effective_to"] is None or as_of < v["effective_to"])
        ]
        if len(hits) > 1:
            raise ValueError("overlapping SCD2 versions")
        return hits[0] if hits else None

    @staticmethod
    def _known(c: dict, as_of: datetime) -> bool:
        return c["outcome_ts"] <= as_of and c["status"] in ADJUDICATED

    # -- member -------------------------------------------------------------------------------
    def member(self, member_id: str, as_of: datetime) -> dict:
        hist = [c for c in self.by_member.get(member_id, []) if c["event_ts"] <= as_of]

        def win(days: int) -> list[dict]:
            return [c for c in hist if c["event_ts"] > as_of - timedelta(days=days)]

        w30, w90, w365 = (win(d) for d in WINDOWS)
        out365 = [c for c in w365 if self._known(c, as_of)]
        denied = sum(1 for c in out365 if c["status"] == DENIED)
        out = {
            "claims_30d": len(w30),
            "claims_90d": len(w90),
            "claims_365d": len(w365),
            "billed_sum_90d": _q(sum((c["billed"] for c in w90), Decimal(0))),
            "billed_sum_365d": _q(sum((c["billed"] for c in w365), Decimal(0))),
            "paid_sum_365d": _q(sum((c["paid"] for c in out365), Decimal(0))),
            "denied_count_365d": denied,
            "denial_rate_365d": round(denied / len(out365), 6) if out365 else None,
            "distinct_providers_365d": len({c["provider_id"] for c in w365}),
            "days_since_last_claim": (
                (as_of.date() - max(c["received_date"] for c in hist)).days if hist else None
            ),
        }
        flags = {}
        for name, rx in self._cond.items():
            flags[f"has_{name}"] = any(
                c["icd10"] is not None and rx.search(c["icd10"]) for c in hist
            )
        out.update(flags)
        out["chronic_condition_count"] = sum(flags.values())
        v = self._version(self.dim_member.get(member_id, []), as_of)
        age = None if v is None or v["birth_year"] is None else max(as_of.year - v["birth_year"], 0)
        out["plan_type"] = v["plan_type"] if v else None
        out["state"] = v["state"] if v else None
        out["age_band"] = age_band(age)
        return out

    # -- provider -----------------------------------------------------------------------------
    def _mean_c(self, provider_id: str, as_of: datetime) -> tuple[int, int] | None:
        """(claims in 365d, rounded-half-up mean billed in cents) or None."""
        w = [
            c
            for c in self.by_provider.get(provider_id, [])
            if as_of - timedelta(days=365) < c["event_ts"] <= as_of
        ]
        if not w:
            return None
        s = sum(c["billed_c"] for c in w)
        return len(w), (2 * s + len(w)) // (2 * len(w))

    def provider(self, provider_id: str, as_of: datetime) -> dict:
        cfg = self.cfg
        w365 = [
            c
            for c in self.by_provider.get(provider_id, [])
            if as_of - timedelta(days=365) < c["event_ts"] <= as_of
        ]
        known = [c for c in w365 if self._known(c, as_of)]
        den = sum(1 for c in known if c["status"] == DENIED)
        v = self._version(self.dim_provider.get(provider_id, []), as_of)
        own = self._mean_c(provider_id, as_of)
        z = None
        if v is not None and own is not None and own[0] >= cfg.min_peer_claims:
            means = []
            for q in self.dim_provider:  # ALL providers: the peer universe
                if not any(True for _ in self.by_provider.get(q, [])):
                    continue
                qv = self._version(self.dim_provider[q], as_of)
                m = self._mean_c(q, as_of)
                if qv and m and m[0] >= cfg.min_peer_claims and qv["specialty"] == v["specialty"]:
                    means.append(m[1])
            k = len(means)
            if k >= cfg.min_peers:
                s1, s2 = sum(means), sum(m * m for m in means)
                num = k * s2 - s1 * s1
                if num > 0:
                    z = round((own[1] - s1 / k) / math.sqrt(num / (k * (k - 1))), 6)
        return {
            "claims_90d": sum(1 for c in w365 if c["event_ts"] > as_of - timedelta(days=90)),
            "claims_365d": len(w365),
            "distinct_members_365d": len({c["member_id"] for c in w365}),
            "denial_rate_365d": round(den / len(known), 6) if known else None,
            "avg_billed_365d": round(own[1] / 100.0, 2) if own else None,
            "cost_z_365d": z,
            "specialty": v["specialty"] if v else None,
            "network_status": v["network_status"] if v else None,
            "cost_tier": v["cost_tier"] if v else None,
        }

    # -- claim --------------------------------------------------------------------------------
    def claim(self, claim_id: str) -> dict:
        c = self.by_id[claim_id]
        as_of = datetime(c["received_date"].year, c["received_date"].month, c["received_date"].day)
        out = {
            "billed_amount": c["billed"],
            "line_count": c["line_count"],
            "claim_type": c["claim_type"],
            "place_of_service": c["place_of_service"],
            "service_to_received_lag_days": (c["received_date"] - c["service_date"]).days,
            "primary_dx_chapter": c["icd10"][0] if c["icd10"] else None,
        }
        out.update({f"m_{k}": v for k, v in self.member(c["member_id"], as_of).items()})
        out.update({f"p_{k}": v for k, v in self.provider(c["provider_id"], as_of).items()})
        return out
