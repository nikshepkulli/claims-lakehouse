"""Build a gold-shaped Parquet export WITHOUT Spark, for tests and the local demo.

``write_gold_export`` writes the tables the feature job reads (fact_claim, dim_member,
dim_provider, dim_diagnosis) with the gold column names and types from docs/gold.md, deriving
surrogate keys with the documented sha256 scheme (a ``hashlib`` mirror of gold/keys.py, which
needs PySpark and so is not imported here). ``from_claimsgen`` converts a raw claimsgen run into
that shape with a minimal clean-up and a *synthetic* second dimension version for some entities,
so the as-of join is exercised on generated data too. This is a stand-in for the real Spark gold
job (#8), not a replacement: use the real ``<gold>/export`` in a deployment.
"""

# ruff: noqa: E501
from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

EPOCH = datetime(1900, 1, 1)
_VALID_ICD = re.compile(r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$")


def derive_sk(kind: str, *parts) -> str:
    """Python mirror of ``lakehouse.gold.keys.derive_sk`` (scheme v1)."""
    txt = "|".join(
        [
            f"{kind}:v1",
            *[
                p.strftime("%Y-%m-%d %H:%M:%S") if isinstance(p, datetime) else str(p)
                for p in parts
            ],
        ]
    )
    return hashlib.sha256(txt.encode()).hexdigest()


def _key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def _write(out: Path, table: str, t: pa.Table) -> None:
    d = out / table
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(t, d / "part-00000.parquet")


def write_gold_export(
    out: str | Path, members: list[dict], providers: list[dict], claims: list[dict]
) -> Path:
    """members/providers: one dict per SCD2 *version* (id, attributes, effective_from,
    effective_to). claims: one dict per claim (ids, dates, status, billed, paid, icd10)."""
    out = Path(out)
    ts = pa.timestamp("us")
    mver = {}
    for m in members:
        m = {**m, "member_sk": derive_sk("member", m["member_id"], m["effective_from"])}
        mver.setdefault(m["member_id"], []).append(m)
    pver = {}
    for p in providers:
        p = {**p, "provider_sk": derive_sk("provider", p["provider_id"], p["effective_from"])}
        pver.setdefault(p["provider_id"], []).append(p)

    def at(versions: list[dict], day: date) -> dict:
        t = datetime(day.year, day.month, day.day)
        for v in versions:
            if v["effective_from"] <= t and (v["effective_to"] is None or t < v["effective_to"]):
                return v
        raise ValueError(f"no dimension version covers {day}")

    codes = sorted({c["icd10"] for c in claims if c.get("icd10")})
    dx_sk = {c: derive_sk("diagnosis", c) for c in codes}
    fact = []
    for c in claims:
        fact.append(
            {
                "claim_id": c["claim_id"],
                "member_sk": at(mver[c["member_id"]], c["service_date"])["member_sk"],
                "provider_sk": at(pver[c["provider_id"]], c["service_date"])["provider_sk"],
                "primary_diagnosis_sk": dx_sk.get(c.get("icd10")),
                "service_date_key": _key(c["service_date"]),
                "received_date_key": _key(c["received_date"]),
                "claim_type": c.get("claim_type", "PROFESSIONAL"),
                "place_of_service": c.get("place_of_service", "11"),
                "claim_status": c["claim_status"],
                "line_count": c.get("line_count", 1),
                "total_billed_amount": Decimal(str(c["billed"])),
                "total_allowed_amount": Decimal(str(c.get("allowed", c["billed"]))),
                "total_paid_amount": Decimal(str(c["paid"])),
            }
        )
    dec = pa.decimal128(12, 2)
    _write(
        out,
        "fact_claim",
        pa.Table.from_pylist(
            fact,
            schema=pa.schema(
                [
                    ("claim_id", pa.string()),
                    ("member_sk", pa.string()),
                    ("provider_sk", pa.string()),
                    ("primary_diagnosis_sk", pa.string()),
                    ("service_date_key", pa.int32()),
                    ("received_date_key", pa.int32()),
                    ("claim_type", pa.string()),
                    ("place_of_service", pa.string()),
                    ("claim_status", pa.string()),
                    ("line_count", pa.int16()),
                    ("total_billed_amount", dec),
                    ("total_allowed_amount", dec),
                    ("total_paid_amount", dec),
                ]
            ),
        ),
    )
    mrows = [v for vs in mver.values() for v in vs]
    _write(
        out,
        "dim_member",
        pa.Table.from_pylist(
            mrows,
            schema=pa.schema(
                [
                    ("member_sk", pa.string()),
                    ("member_id", pa.string()),
                    ("birth_year", pa.int16()),
                    ("sex", pa.string()),
                    ("state", pa.string()),
                    ("plan_type", pa.string()),
                    ("enrollment_date", pa.date32()),
                    ("effective_from", ts),
                    ("effective_to", ts),
                ]
            ),
        ),
    )
    prows = [v for vs in pver.values() for v in vs]
    _write(
        out,
        "dim_provider",
        pa.Table.from_pylist(
            prows,
            schema=pa.schema(
                [
                    ("provider_sk", pa.string()),
                    ("provider_id", pa.string()),
                    ("specialty", pa.string()),
                    ("state", pa.string()),
                    ("network_status", pa.string()),
                    ("cost_tier", pa.string()),
                    ("effective_from", ts),
                    ("effective_to", ts),
                ]
            ),
        ),
    )
    _write(
        out,
        "dim_diagnosis",
        pa.table(
            {
                "diagnosis_sk": [dx_sk[c] for c in codes],
                "icd10_code": codes,
                "icd10_chapter": [c[0] for c in codes],
            }
        ),
    )
    return out


def _bucket(key: str, seed: int, mod: int) -> int:
    return int(hashlib.sha256(f"{seed}:{key}".encode()).hexdigest(), 16) % mod


def from_claimsgen(raw: str | Path, out: str | Path, seed: int = 7, change_pct: int = 10) -> Path:
    """Convert a claimsgen run into a gold-shaped export (SYNTHETIC; see module docstring).

    Minimal clean-up: first claim per id, non-negative amounts, PAID/DENIED status, known
    member/provider, well-formed ICD-10 else NULL. ``change_pct`` percent of members change
    plan_type and of providers change cost_tier on 2024-07-01, giving a second SCD2 version.
    """
    import duckdb

    raw = Path(raw)
    con = duckdb.connect()
    rd = lambda t: f"read_parquet('{raw}/{t}/*/*.parquet')"  # noqa: E731
    mem = con.execute(
        f"""SELECT member_id, birth_year, sex, state, plan_type, enrollment_date FROM {rd("members")}
            WHERE member_id IS NOT NULL QUALIFY row_number() OVER
            (PARTITION BY member_id ORDER BY snapshot_date DESC) = 1 ORDER BY member_id"""
    ).fetchall()
    prv = con.execute(
        f"""SELECT provider_id, specialty, state, network_status, cost_tier FROM {rd("providers")}
            WHERE provider_id IS NOT NULL QUALIFY row_number() OVER
            (PARTITION BY provider_id ORDER BY snapshot_date DESC) = 1 ORDER BY provider_id"""
    ).fetchall()
    change = datetime(2024, 7, 1)
    members, providers = [], []
    for r in mem:
        base = dict(
            zip(
                ("member_id", "birth_year", "sex", "state", "plan_type", "enrollment_date"),
                r,
                strict=True,
            )
        )
        if _bucket(r[0], seed, 100) < change_pct:
            members.append({**base, "effective_from": EPOCH, "effective_to": change})
            alt = "PPO" if base["plan_type"] != "PPO" else "HMO"
            members.append(
                {**base, "plan_type": alt, "effective_from": change, "effective_to": None}
            )
        else:
            members.append({**base, "effective_from": EPOCH, "effective_to": None})
    for r in prv:
        base = dict(
            zip(
                ("provider_id", "specialty", "state", "network_status", "cost_tier"), r, strict=True
            )
        )
        if _bucket(r[0], seed, 100) < change_pct:
            providers.append({**base, "effective_from": EPOCH, "effective_to": change})
            alt = "HIGH" if base["cost_tier"] != "HIGH" else "STANDARD"
            providers.append(
                {**base, "cost_tier": alt, "effective_from": change, "effective_to": None}
            )
        else:
            providers.append({**base, "effective_from": EPOCH, "effective_to": None})
    mids, pids = {m["member_id"] for m in members}, {p["provider_id"] for p in providers}
    rows = con.execute(
        f"""SELECT c.claim_id, c.member_id, c.provider_id, c.service_date, c.received_date,
                   c.claim_type, c.place_of_service, c.claim_status, c.line_count,
                   c.total_billed_amount, c.total_paid_amount, dx.icd10_code
            FROM {rd("claims")} c LEFT JOIN {rd("diagnoses")} dx
              ON dx.claim_id = c.claim_id AND dx.dx_seq = 1
            WHERE c.claim_id IS NOT NULL AND c.service_date IS NOT NULL
              AND c.received_date IS NOT NULL AND c.claim_status IN ('PAID', 'DENIED')
              AND c.total_billed_amount >= 0 AND c.total_paid_amount >= 0
            QUALIFY row_number() OVER (PARTITION BY c.claim_id ORDER BY c.ingest_date) = 1
            ORDER BY c.claim_id"""
    ).fetchall()
    claims = []
    for r in rows:
        if r[1] not in mids or r[2] not in pids or r[3] > r[4] or r[8] is None or r[9] is None:
            continue
        claims.append(
            {
                "claim_id": r[0],
                "member_id": r[1],
                "provider_id": r[2],
                "service_date": r[3],
                "received_date": r[4],
                "claim_type": r[5],
                "place_of_service": r[6],
                "claim_status": r[7],
                "line_count": r[8],
                "billed": round(r[9], 2),
                "paid": round(r[10], 2),
                "icd10": r[11] if r[11] and _VALID_ICD.match(r[11]) else None,
            }
        )
    return write_gold_export(out, members, providers, claims)
