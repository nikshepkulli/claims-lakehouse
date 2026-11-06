"""Test helpers: a random gold-shaped dataset and a single-moment feature computation."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from decimal import Decimal

from lakehouse.features import sql, store
from lakehouse.features.config import FeatureConfig
from lakehouse.features.fixture import EPOCH, write_gold_export

ICD = [None, "E11.9", "I10", "I50.9", "J45.909", "N18.3", "F32.9", "M54.5", "Z00.00", "J44.1"]


def random_dataset(seed: int, n_claims: int = 160):
    rng = random.Random(seed)
    members, providers, claims = [], [], []
    for i in range(12):
        base = {
            "member_id": f"M{i:03d}",
            "birth_year": rng.randint(1940, 2010),
            "sex": rng.choice("FM"),
            "state": rng.choice(["GA", "MA", "TX"]),
            "plan_type": rng.choice(["HMO", "PPO"]),
            "enrollment_date": date(2020, 1, 1),
        }
        if rng.random() < 0.5:
            cut = datetime(2024, 1, 1) + timedelta(days=rng.randint(10, 340))
            members.append({**base, "effective_from": EPOCH, "effective_to": cut})
            alt = {**base, "plan_type": "PPO" if base["plan_type"] == "HMO" else "HMO"}
            members.append({**alt, "effective_from": cut, "effective_to": None})
        else:
            members.append({**base, "effective_from": EPOCH, "effective_to": None})
    for i in range(9):
        base = {
            "provider_id": f"P{i:03d}",
            "specialty": "Cardiology" if i < 5 else "Pediatrics",
            "state": "GA",
            "network_status": "IN_NETWORK",
            "cost_tier": rng.choice(["STANDARD", "HIGH"]),
        }
        if rng.random() < 0.4:
            cut = datetime(2024, 1, 1) + timedelta(days=rng.randint(10, 340))
            providers.append({**base, "effective_from": EPOCH, "effective_to": cut})
            providers.append(
                {**base, "cost_tier": "HIGH", "effective_from": cut, "effective_to": None}
            )
        else:
            providers.append({**base, "effective_from": EPOCH, "effective_to": None})
    for i in range(n_claims):
        recv = date(2024, 1, 1) + timedelta(days=rng.randint(0, 364))
        billed = Decimal(rng.randint(5000, 200000)) / 100
        status = "DENIED" if rng.random() < 0.2 else "PAID"
        claims.append(
            {
                "claim_id": f"C{i:05d}",
                "member_id": f"M{rng.randrange(12):03d}",
                "provider_id": f"P{rng.randrange(9):03d}",
                "service_date": recv - timedelta(days=rng.randint(0, 10)),
                "received_date": recv,
                "claim_status": status,
                "billed": billed,
                "paid": Decimal(0)
                if status == "DENIED"
                else (billed * Decimal("0.8")).quantize(Decimal("0.01")),
                "icd10": rng.choice(ICD),
                "line_count": rng.randint(1, 4),
                "claim_type": rng.choice(["PROFESSIONAL", "INSTITUTIONAL"]),
            }
        )
    return members, providers, claims


def features_at(export, as_of: datetime, cfg: FeatureConfig | None = None):
    """Member, provider and claim feature rows at ONE moment (claims received that day)."""
    cfg = cfg or FeatureConfig()
    con = store.connect()
    sql.prepare_sources(con, str(export), cfg)
    ts = f"TIMESTAMP '{as_of:%Y-%m-%d %H:%M:%S}'"
    con.execute(f"CREATE TABLE tm AS SELECT DISTINCT member_id, {ts} AS as_of FROM src_dim_member")
    con.execute(
        f"CREATE TABLE tp AS SELECT DISTINCT provider_id, {ts} AS as_of FROM src_dim_provider"
    )
    n = sql.Names()
    sql.build_member_features(con, n, "tm", "fm", cfg)
    sql.build_provider_features(con, n, "tp", "fp", cfg)
    sql.build_claim_features(
        con, n, "fc", cfg, as_of_filter=f"CAST(received_date AS TIMESTAMP) = {ts}"
    )

    def rows(t: str, key: str) -> dict:
        cur = con.execute(f"SELECT * FROM {t}")
        names = [d[0] for d in cur.description]
        return {r[names.index(key)]: dict(zip(names, r, strict=True)) for r in cur.fetchall()}

    out = rows("fm", "member_id"), rows("fp", "provider_id"), rows("fc", "claim_id")
    con.close()
    return out


def _variant(kind: str, rng: random.Random, members, providers, claims, as_of: datetime):
    """Return perturbed copies of the inputs; only information after ``as_of`` is touched."""
    day = as_of.date()
    members, providers, claims = ([dict(x) for x in xs] for xs in (members, providers, claims))
    lag = timedelta(days=30)  # outcome lag of the default config
    if kind == "shuffle":
        for xs in (members, providers, claims):
            rng.shuffle(xs)
    elif kind == "drop_future":
        # claims received after the as_of day are gone; same-day claims stay (they are the spine)
        claims = [c for c in claims if c["received_date"] <= day]
        members = [m for m in members if m["effective_from"] <= as_of]
        providers = [p for p in providers if p["effective_from"] <= as_of]
    elif kind == "rewrite_future":
        ids = [m["member_id"] for m in members]
        for c in claims:
            if c["received_date"] > day:
                c.update(
                    member_id=rng.choice(ids),
                    billed=Decimal(rng.randint(1, 9_999_999)) / 100,
                    claim_status=rng.choice(["PAID", "DENIED"]),
                    icd10=rng.choice(ICD),
                )
                c["paid"] = c["billed"]
            elif c["received_date"] + lag >= day or c["received_date"] == day:
                # received by as_of but outcome not yet known (and own claims): outcomes are free
                c.update(claim_status=rng.choice(["PAID", "DENIED"]), paid=Decimal("9999.99"))
        for m in members:
            if m["effective_from"] > as_of:
                m.update(plan_type="LEAK", state="ZZ", birth_year=1900)
        for p in providers:
            if p["effective_from"] > as_of:
                p.update(specialty="LEAK", cost_tier="LEAK")
    return members, providers, claims


def future_invariance_violations(seed: int, tmp_path) -> list[str]:
    """Features at a random as_of must be identical when only post-as_of information changes.
    Returns a list of human-readable violations (empty = no leakage)."""
    rng = random.Random(seed)
    members, providers, claims = random_dataset(seed)
    as_of = datetime(2024, 1, 1) + timedelta(days=rng.randint(40, 330))
    base = features_at(write_gold_export(tmp_path / "base", members, providers, claims), as_of)
    assert any(r["claims_365d"] for r in base[0].values()), "dataset must have history"
    bad = []
    for kind in ("shuffle", "drop_future", "rewrite_future"):
        m, p, c = _variant(kind, random.Random(seed * 7 + 1), members, providers, claims, as_of)
        got = features_at(write_gold_export(tmp_path / kind, m, p, c), as_of)
        if got[0] != base[0]:
            bad.append(f"seed {seed} {kind}: member features changed")
        if got[1] != base[1]:
            bad.append(f"seed {seed} {kind}: provider features changed")
        for cid in (k for k, r in base[2].items() if r["as_of"] == as_of):
            if got[2].get(cid) != base[2][cid]:
                bad.append(f"seed {seed} {kind}: claim {cid} features changed")
    return bad
