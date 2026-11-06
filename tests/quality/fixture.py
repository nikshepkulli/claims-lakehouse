"""A tiny, internally consistent three-layer lakehouse written as real Delta tables (no Spark).

The shapes (column names, Arrow types that Spark/Delta produce, partition-free layout) follow what
the Spark jobs write, so the quality suites run against the same kind of input as in CI. Every
reconciliation count here is true, so the clean fixture must pass with zero failures.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake

from lakehouse.gold import schema as G

NOW = datetime(2024, 6, 1, 12, 0, 0)
A = {  # Spark simpleString -> Arrow
    "string": pa.string(),
    "int": pa.int32(),
    "smallint": pa.int16(),
    "bigint": pa.int64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "timestamp_ntz": pa.timestamp("us"),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "decimal(12,2)": pa.decimal128(12, 2),
    "decimal(22,2)": pa.decimal128(22, 2),
}


def put(root: Path, name: str, types: dict[str, str], rows: list[dict]) -> None:
    schema = pa.schema([(c, A[t]) for c, t in types.items()])
    cols = {c: [r.get(c) for r in rows] for c in types}
    write_deltalake(str(root / name), pa.table(cols, schema=schema), mode="overwrite")


def d(s: str) -> date:
    return date.fromisoformat(s)


def money(x: str) -> Decimal:
    return Decimal(x)


BATCHES = [("b20240102", d("2024-01-02")), ("b20240505", d("2024-05-05"))]
MEMBERS = [f"M00000000{i}" for i in range(3)]
PROVIDERS = [f"PRV000000{i}" for i in range(2)]


def claims_rows():
    rows = []
    for i in range(4):
        b, bd = BATCHES[i % 2]
        rows.append(
            {
                "claim_id": f"CLM00000000000{i}",
                "member_id": MEMBERS[i % 3],
                "provider_id": PROVIDERS[i % 2],
                "claim_type": "PROFESSIONAL",
                "place_of_service": "11",
                "service_date": bd,
                "received_date": bd,
                "claim_status": "PAID" if i else "DENIED",
                "line_count": 2,
                "total_billed_amount": money("200.00"),
                "total_allowed_amount": money("150.00"),
                "total_paid_amount": money("0.00") if i == 0 else money("120.00"),
                "_batch_id": b,
            }
        )
    return rows


def lines_rows():
    rows = []
    for c in claims_rows():
        for n in (1, 2):
            rows.append(
                {
                    "line_id": f"{c['claim_id']}-0{n}",
                    "claim_id": c["claim_id"],
                    "line_number": n,
                    "procedure_code": "99213",
                    "units": 1,
                    "service_date": c["service_date"],
                    "billed_amount": money("100.00"),
                    "allowed_amount": money("75.00"),
                    "paid_amount": money("0.00")
                    if c["claim_status"] == "DENIED"
                    else money("60.00"),
                    "_batch_id": c["_batch_id"],
                }
            )
    return rows


def dx_rows():
    return [
        {
            "claim_id": c["claim_id"],
            "dx_seq": s,
            "icd10_code": "I10" if s == 1 else "E11.9",
            "diagnosis_type": "PRIMARY" if s == 1 else "SECONDARY",
            "_batch_id": c["_batch_id"],
        }
        for c in claims_rows()
        for s in (1, 2)
    ]


def members_rows():
    return [
        {
            "member_id": m,
            "birth_year": 1960 + i,
            "sex": "F" if i % 2 else "M",
            "state": "CA",
            "plan_type": "PPO",
            "enrollment_date": d("2023-01-01"),
            "_batch_id": BATCHES[0][0],
        }
        for i, m in enumerate(MEMBERS)
    ]


def providers_rows():
    return [
        {
            "provider_id": p,
            "specialty": "Cardiology",
            "state": "TX",
            "network_status": "IN_NETWORK",
            "cost_tier": "STANDARD",
            "_batch_id": BATCHES[0][0],
        }
        for p in PROVIDERS
    ]


BRONZE_META = {
    "_ingest_ts": "timestamp",
    "_source_file": "string",
    "_batch_id": "string",
    "_ingest_date": "date",
}
SILVER_META = {
    "_row_hash": "string",
    "_source_file": "string",
    "_batch_id": "string",
    "_ingest_date": "date",
    "_bronze_ingest_ts": "timestamp",
    "_silver_ts": "timestamp",
}


def _meta(rows, meta):
    ts = datetime(2024, 6, 1, 11, 0, 0)
    out = []
    for r in rows:
        bid = r["_batch_id"]
        bd = dict(BATCHES)[bid]
        m = {
            "_ingest_ts": ts,
            "_source_file": "x.parquet",
            "_ingest_date": bd,
            "_row_hash": "h",
            "_bronze_ingest_ts": ts,
            "_silver_ts": ts,
        }
        out.append({**r, **m})
    return out


DATA = {
    "claims": (
        {
            "claim_id": "string",
            "member_id": "string",
            "provider_id": "string",
            "claim_type": "string",
            "place_of_service": "string",
            "service_date": "date",
            "received_date": "date",
            "claim_status": "string",
            "line_count": "int",
            "total_billed_amount": "decimal(12,2)",
            "total_allowed_amount": "decimal(12,2)",
            "total_paid_amount": "decimal(12,2)",
        },
        claims_rows,
    ),
    "claim_lines": (
        {
            "line_id": "string",
            "claim_id": "string",
            "line_number": "int",
            "procedure_code": "string",
            "units": "int",
            "service_date": "date",
            "billed_amount": "decimal(12,2)",
            "allowed_amount": "decimal(12,2)",
            "paid_amount": "decimal(12,2)",
        },
        lines_rows,
    ),
    "diagnoses": (
        {"claim_id": "string", "dx_seq": "int", "icd10_code": "string", "diagnosis_type": "string"},
        dx_rows,
    ),
    "members": (
        {
            "member_id": "string",
            "birth_year": "int",
            "sex": "string",
            "state": "string",
            "plan_type": "string",
            "enrollment_date": "date",
        },
        members_rows,
    ),
    "providers": (
        {
            "provider_id": "string",
            "specialty": "string",
            "state": "string",
            "network_status": "string",
            "cost_tier": "string",
        },
        providers_rows,
    ),
}


def build_bronze(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    for name, (types, rows) in DATA.items():
        put(root, name, {**types, **BRONZE_META}, _meta(rows(), BRONZE_META))
    return root


def build_silver(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, (types, rows) in DATA.items():
        data = _meta(rows(), SILVER_META)
        put(root, name, {**types, **SILVER_META}, data)
        by = {}
        for r in data:
            by[r["_batch_id"]] = by.get(r["_batch_id"], 0) + 1
        counts[name] = by
    q = {
        c: "string"
        for c in (
            "dataset",
            "business_key",
            "reason_code",
            "reason_detail",
            "all_reason_codes",
            "source_batch",
            "source_file",
            "payload",
        )
    }
    q |= {"_ingest_date": "date", "_quarantined_at": "timestamp"}
    put(
        root,
        "silver_quarantine",
        q,
        [
            {
                "dataset": "claims",
                "business_key": "CLM9",
                "reason_code": "ORPHAN_MEMBER_ID",
                "reason_detail": "member_id=M999",
                "all_reason_codes": "ORPHAN_MEMBER_ID",
                "source_batch": BATCHES[0][0],
                "source_file": "x",
                "payload": "{}",
                "_ingest_date": BATCHES[0][1],
                "_quarantined_at": NOW,
            }
        ],
    )
    rt = {
        "dataset": "string",
        "batch_id": "string",
        "batch_date": "date",
        "bronze_rows": "bigint",
        "silver_rows": "bigint",
        "quarantined_rows": "bigint",
        "deduped_rows": "bigint",
        "identity_ok": "boolean",
        "_run_ts": "timestamp",
    }
    rec = []
    for name, by in counts.items():
        for b, n in by.items():
            rec.append(
                {
                    "dataset": name,
                    "batch_id": b,
                    "batch_date": dict(BATCHES)[b],
                    "bronze_rows": n,
                    "silver_rows": n,
                    "quarantined_rows": 0,
                    "deduped_rows": 0,
                    "identity_ok": True,
                    "_run_ts": NOW,
                }
            )
    put(root, "silver_reconciliation", rt, rec)
    return root


def _sk(prefix: str, i: int) -> str:
    return (
        f"{prefix}{i:03d}".ljust(64, "0")[:64].replace("m", "a").replace("p", "b").replace("g", "c")
    )


def build_gold(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    ts = datetime(2024, 6, 1, 11, 0, 0)
    ctypes = {c.name: c.type for c in G.TABLES["dim_date"]}
    days = []
    cur = d("2024-01-01")
    while cur.year == 2024:
        wd = cur.isoweekday()
        days.append(
            {
                "date_key": cur.year * 10000 + cur.month * 100 + cur.day,
                "full_date": cur,
                "year": cur.year,
                "quarter": (cur.month - 1) // 3 + 1,
                "month": cur.month,
                "month_name": cur.strftime("%B"),
                "day_of_month": cur.day,
                "day_of_week": wd,
                "is_weekend": wd >= 6,
            }
        )
        cur = date.fromordinal(cur.toordinal() + 1)
    put(root, "dim_date", ctypes, days)

    epoch = datetime(1900, 1, 1)
    mv = []
    for i, m in enumerate(MEMBERS):
        mv.append(
            {
                "member_sk": _sk("a", i),
                "member_id": m,
                "birth_year": 1960 + i,
                "sex": "F",
                "state": "CA",
                "plan_type": "PPO",
                "enrollment_date": d("2023-01-01"),
                "effective_from": epoch,
                "effective_to": None,
                "is_current": True,
                "record_hash": "h",
                "load_batch_id": BATCHES[0][0],
                "loaded_at": ts,
            }
        )
    # member 0 changed plan: two versions, history closed and continuous
    mv[0].update(effective_to=datetime(2024, 3, 1), is_current=False)
    mv.append(
        {
            **mv[0],
            "member_sk": _sk("a", 9),
            "plan_type": "HMO",
            "effective_from": datetime(2024, 3, 1),
            "effective_to": None,
            "is_current": True,
        }
    )
    put(root, "dim_member", {c.name: c.type for c in G.TABLES["dim_member"]}, mv)
    pv = [
        {
            "provider_sk": _sk("b", i),
            "provider_id": p,
            "specialty": "Cardiology",
            "state": "TX",
            "network_status": "IN_NETWORK",
            "cost_tier": "STANDARD",
            "effective_from": epoch,
            "effective_to": None,
            "is_current": True,
            "record_hash": "h",
            "load_batch_id": BATCHES[0][0],
            "loaded_at": ts,
        }
        for i, p in enumerate(PROVIDERS)
    ]
    put(root, "dim_provider", {c.name: c.type for c in G.TABLES["dim_provider"]}, pv)
    dx = [
        {
            "diagnosis_sk": _sk("c", i),
            "icd10_code": code,
            "icd10_chapter": code[0],
            "description": "Synthetic ICD-10 code",
            "load_batch_id": BATCHES[0][0],
            "loaded_at": ts,
        }
        for i, code in enumerate(("I10", "E11.9"))
    ]
    put(root, "dim_diagnosis", {c.name: c.type for c in G.TABLES["dim_diagnosis"]}, dx)

    fc, fl = [], []
    for c in claims_rows():
        bid = c["_batch_id"]
        key = c["service_date"].year * 10000 + c["service_date"].month * 100 + c["service_date"].day
        fc.append(
            {
                "claim_id": c["claim_id"],
                "member_sk": _sk("a", 1 + int(c["claim_id"][-1]) % 2),
                "provider_sk": _sk("b", int(c["claim_id"][-1]) % 2),
                "primary_diagnosis_sk": _sk("c", 0),
                "service_date_key": key,
                "received_date_key": key,
                "claim_type": c["claim_type"],
                "place_of_service": "11",
                "claim_status": c["claim_status"],
                "line_count": 2,
                "total_billed_amount": c["total_billed_amount"],
                "total_allowed_amount": c["total_allowed_amount"],
                "total_paid_amount": c["total_paid_amount"],
                "load_batch_id": bid,
                "loaded_at": ts,
            }
        )
    for r in lines_rows():
        key = r["service_date"].year * 10000 + r["service_date"].month * 100 + r["service_date"].day
        fl.append(
            {
                "line_id": r["line_id"],
                "claim_id": r["claim_id"],
                "line_number": r["line_number"],
                "procedure_code": r["procedure_code"],
                "units": r["units"],
                "service_date_key": key,
                "billed_amount": r["billed_amount"],
                "allowed_amount": r["allowed_amount"],
                "paid_amount": r["paid_amount"],
                "load_batch_id": r["_batch_id"],
                "loaded_at": ts,
            }
        )
    put(root, "fact_claim", {c.name: c.type for c in G.TABLES["fact_claim"]}, fc)
    put(root, "fact_claim_line", {c.name: c.type for c in G.TABLES["fact_claim_line"]}, fl)

    rec = []
    for name, rows in (("fact_claim", fc), ("fact_claim_line", fl)):
        for b, bd in BATCHES:
            n = sum(r["load_batch_id"] == b for r in rows)
            z = Decimal("0.00")
            rec.append(
                {
                    "gold_table": name,
                    "batch_id": b,
                    "batch_date": bd,
                    "silver_rows": n,
                    "gold_rows": n,
                    "excluded_rows": 0,
                    "silver_billed": z,
                    "gold_billed": z,
                    "excluded_billed": z,
                    "silver_allowed": z,
                    "gold_allowed": z,
                    "excluded_allowed": z,
                    "silver_paid": z,
                    "gold_paid": z,
                    "excluded_paid": z,
                    "key_violations": 0,
                    "identity_ok": True,
                    "detail": None,
                    "_run_ts": NOW,
                }
            )
    put(root, "gold_reconciliation", {c.name: c.type for c in G.RECONCILIATION_COLS}, rec)
    put(
        root,
        "gold_exclusions",
        {c.name: c.type for c in G.EXCLUSION_COLS},
        [
            {
                "dataset": "claims",
                "business_key": "CLM9",
                "reason_code": "INCOMPLETE_LINES",
                "reason_detail": None,
                "batch_id": BATCHES[0][0],
                "_run_ts": NOW,
            }
        ],
    )
    return root
