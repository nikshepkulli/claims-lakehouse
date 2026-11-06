"""Data-quality scorecard inputs for the dashboard.

Three sources, all optional except the gold export itself:

1. **Gold invariants** (always computed): independent DuckDB checks on the gold Parquet
   export. Each is a rule a defect would break; the expected number of violations is 0.
2. **Defect manifest** (``--raw``): ``defects_manifest.json`` written by ``claimsgen`` is the
   ground truth of what was injected. Together with the invariants it gives, per defect, how
   many injected records are still visible in gold. "Caught" means *not present in gold*;
   it does not say which layer (silver quarantine, gold exclusion, dedupe) removed it.
3. **Reconciliation / quality results** (``--reconciliation``, ``--quality-json``): extension
   points. ``--reconciliation`` takes the Spark ``gold_reconciliation`` table exported as
   JSON/Parquet; ``--quality-json`` takes the data-quality framework's summary (ticket #22),
   shape documented in ``docs/dashboard.md``. When absent the dashboard says so; it never
   fabricates numbers.

All data is synthetic.
"""

from __future__ import annotations

import json
from pathlib import Path

ICD_RE = r"^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$"
PROC_RE = r"^(\d{5}|[A-V]\d{4})$"

# Which defects the claims-level gold can be checked for, and the invariant that detects each.
DEFECT_CHECKS: dict[str, str] = {
    "duplicate_claim": "no_duplicate_claim_ids",
    "orphan_member_id": "no_orphan_member_keys",
    "null_procedure_code": "no_invalid_procedure_codes",
    "invalid_procedure_code": "no_invalid_procedure_codes",
    "negative_amount": "no_negative_amounts",
    "null_icd10": "no_invalid_icd10_codes",
    "invalid_icd10": "no_invalid_icd10_codes",
}
# Defects that are retained or out of scope by design, with the reason shown in the scorecard.
DEFECT_NOTES: dict[str, str] = {
    "late_arrival": "Kept on purpose: late claims are real claims, bucketed by service month.",
    "duplicate_event": "Streaming event defect; handled by the streaming job, not batch gold.",
    "out_of_order_event": "Streaming event defect; handled by the streaming job, not batch gold.",
}


def _scalar(con, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def gold_invariants(con, gold: Path) -> list[dict]:
    """Run independent integrity checks on the gold Parquet export.

    ``con`` is a DuckDB connection (any database). Returns one dict per check:
    ``id``, ``label``, ``checked`` (rows examined), ``violations`` and ``status`` (pass/fail).
    """

    def rd(table: str) -> str:
        return f"read_parquet('{(gold / table).as_posix()}/*.parquet')"

    fc, fl, dm, dp, dd = (
        rd("fact_claim"),
        rd("fact_claim_line"),
        rd("dim_member"),
        rd("dim_provider"),
        rd("dim_diagnosis"),
    )
    checks = [
        (
            "no_duplicate_claim_ids",
            "Each claim_id appears once",
            f"SELECT count(*) FROM {fc}",
            f"SELECT count(*) - count(DISTINCT claim_id) FROM {fc}",
        ),
        (
            "no_duplicate_line_ids",
            "Each line_id appears once",
            f"SELECT count(*) FROM {fl}",
            f"SELECT count(*) - count(DISTINCT line_id) FROM {fl}",
        ),
        (
            "no_orphan_member_keys",
            "Every claim resolves to a member version",
            f"SELECT count(*) FROM {fc}",
            f"SELECT count(*) FROM {fc} WHERE member_sk NOT IN (SELECT member_sk FROM {dm})",
        ),
        (
            "no_orphan_provider_keys",
            "Every claim resolves to a provider version",
            f"SELECT count(*) FROM {fc}",
            f"SELECT count(*) FROM {fc} WHERE provider_sk NOT IN (SELECT provider_sk FROM {dp})",
        ),
        (
            "no_orphan_lines",
            "Every line belongs to a claim in gold",
            f"SELECT count(*) FROM {fl}",
            f"SELECT count(*) FROM {fl} WHERE claim_id NOT IN (SELECT claim_id FROM {fc})",
        ),
        (
            "no_invalid_procedure_codes",
            "Procedure codes present and well-formed",
            f"SELECT count(*) FROM {fl}",
            f"SELECT count(*) FROM {fl} WHERE procedure_code IS NULL "
            f"OR NOT regexp_matches(procedure_code, '{PROC_RE}')",
        ),
        (
            "no_negative_amounts",
            "No negative billed, allowed or paid amounts",
            f"SELECT count(*) FROM {fl}",
            f"SELECT count(*) FROM {fl} "
            "WHERE billed_amount < 0 OR allowed_amount < 0 OR paid_amount < 0",
        ),
        (
            "no_invalid_icd10_codes",
            "Diagnosis dimension holds only valid ICD-10 codes",
            f"SELECT count(*) FROM {dd}",
            f"SELECT count(*) FROM {dd} WHERE icd10_code IS NULL "
            f"OR NOT regexp_matches(icd10_code, '{ICD_RE}')",
        ),
        (
            "header_equals_lines",
            "Claim header paid equals the sum of its lines",
            f"SELECT count(*) FROM {fc}",
            f"""SELECT count(*) FROM {fc} c
                JOIN (SELECT claim_id, sum(paid_amount) AS p, count(*) AS n FROM {fl}
                      GROUP BY claim_id) l USING (claim_id)
                WHERE c.total_paid_amount <> l.p OR c.line_count <> l.n""",
        ),
        (
            "denied_claims_paid_zero",
            "Denied claims have paid = 0",
            f"SELECT count(*) FROM {fc} WHERE claim_status = 'DENIED'",
            f"SELECT count(*) FROM {fc} WHERE claim_status = 'DENIED' AND total_paid_amount <> 0",
        ),
        (
            "scd2_one_current_member",
            "Exactly one current version per member",
            f"SELECT count(DISTINCT member_id) FROM {dm}",
            f"""SELECT count(*) FROM (SELECT member_id FROM {dm} GROUP BY member_id
                HAVING count(*) FILTER (WHERE is_current) <> 1)""",
        ),
        (
            "scd2_one_current_provider",
            "Exactly one current version per provider",
            f"SELECT count(DISTINCT provider_id) FROM {dp}",
            f"""SELECT count(*) FROM (SELECT provider_id FROM {dp} GROUP BY provider_id
                HAVING count(*) FILTER (WHERE is_current) <> 1)""",
        ),
    ]
    out = []
    for cid, label, checked_sql, viol_sql in checks:
        violations = _scalar(con, viol_sql)
        out.append(
            {
                "id": cid,
                "label": label,
                "checked": _scalar(con, checked_sql),
                "violations": violations,
                "status": "pass" if violations == 0 else "fail",
            }
        )
    return out


def load_manifest(raw: Path) -> dict | None:
    """Read ``defects_manifest.json`` from a claimsgen output root, or None when absent."""
    path = raw / "defects_manifest.json"
    if not path.is_file():
        return None
    return json.loads(path.read_text())


def defect_catch_table(manifest: dict, invariants: list[dict]) -> list[dict]:
    """One row per injected defect: injected, still in gold, catch rate, and a note.

    ``still_in_gold`` is the violation count of the invariant that detects the defect; defects
    sharing one invariant (e.g. null and invalid procedure code) share its count, so the
    combined rows are marked ``shared_check``. Defects without a gold-level check are listed
    with ``catch_rate`` None and the reason.
    """
    by_id = {i["id"]: i for i in invariants}
    shared: dict[str, int] = {}
    for check in DEFECT_CHECKS.values():
        shared[check] = shared.get(check, 0) + 1
    rows = []
    for name, info in sorted(manifest["defects"].items()):
        injected = int(info["count"])
        check = DEFECT_CHECKS.get(name)
        row = {
            "defect": name,
            "table": info.get("table", ""),
            "injected": injected,
            "check": check,
            "shared_check": bool(check) and shared[check] > 1,
            "still_in_gold": None,
            "catch_rate": None,
            "note": DEFECT_NOTES.get(name, ""),
        }
        if check and check in by_id:
            leaked = by_id[check]["violations"]
            row["still_in_gold"] = leaked
            row["catch_rate"] = None if injected == 0 else max(0.0, 1 - leaked / injected)
        rows.append(row)
    return rows


def raw_to_gold_counts(con, raw: Path, gold: Path) -> list[dict]:
    """Row counts from the generator's raw output to gold, per fact table.

    ``raw_rows`` includes re-sent duplicates; ``raw_distinct`` is distinct ids; the difference
    to ``gold_rows`` is what dedupe plus quarantine/exclusion removed.
    """
    spec = [("claims", "claim_id", "fact_claim"), ("claim_lines", "line_id", "fact_claim_line")]
    out = []
    for raw_table, key, gold_table in spec:
        g = f"read_parquet('{(raw / raw_table).as_posix()}/*/*.parquet', union_by_name = true)"
        raw_rows = _scalar(con, f"SELECT count(*) FROM {g}")
        distinct = _scalar(con, f"SELECT count(DISTINCT {key}) FROM {g}")
        gold_rows = _scalar(
            con, f"SELECT count(*) FROM read_parquet('{(gold / gold_table).as_posix()}/*.parquet')"
        )
        out.append(
            {
                "table": gold_table,
                "raw_rows": raw_rows,
                "raw_distinct": distinct,
                "duplicates_removed": raw_rows - distinct,
                "quarantined_or_excluded": distinct - gold_rows,
                "gold_rows": gold_rows,
            }
        )
    return out


def load_reconciliation(path: Path | None) -> list[dict] | None:
    """Load an exported ``gold_reconciliation`` table (JSON list of objects or Parquet).

    Extension point for the Spark gold job's own reconciliation (Delta only today; export it
    with ``spark.table(...).toPandas().to_json(orient="records")`` or write Parquet). Only the
    documented columns are kept; anything else is ignored.
    """
    if path is None:
        return None
    keep = ["gold_table", "batch_id", "silver_rows", "gold_rows", "excluded_rows", "identity_ok"]
    if path.suffix == ".json":
        rows = json.loads(path.read_text())
    else:
        import duckdb

        rows = (
            duckdb.connect()
            .execute(f"SELECT * FROM read_parquet('{path.as_posix()}')")
            .fetch_arrow_table()
            .to_pylist()
        )
    return [{k: r.get(k) for k in keep} for r in rows]


def _suites_from_runner_result(doc: dict, path: Path) -> list[dict]:
    """Convert ``python -m lakehouse.quality run`` output (``<layer>-result.json``, ticket #22).

    One suite per table. A required table that is absent is reported with ``status: missing``
    and zero expectations evaluated: it is neither a pass nor hidden.
    """
    layer = doc.get("layer", "unknown")
    suites = []
    for table, t in doc["tables"].items():
        if not isinstance(t, dict) or "expectations" not in t:
            raise ValueError(f"{path}: table {table!r} has no 'expectations' list")
        exps = t["expectations"]
        passed = sum(1 for e in exps if e.get("success"))
        suites.append(
            {
                "name": f"{layer}.{table}",
                "layer": layer,
                "evaluated": len(exps),
                "passed": passed,
                "failed": len(exps) - passed,
                "status": t.get("status", "validated"),
                "problem": t.get("problem"),
            }
        )
    return suites


def load_quality_json(path: Path | None) -> dict | None:
    """Load the data-quality framework result (ticket #22), validating its shape.

    Two shapes are accepted. The runner's own ``<layer>-result.json`` (has a ``tables`` mapping;
    converted to one suite per table, see ``_suites_from_runner_result``), or the minimal summary
    ``{"generated_by": str, "suites": [{"name", "layer", "evaluated", "passed", "failed"}]}``
    (optional per-suite ``status`` and ``problem``). Raises ``ValueError`` on a malformed file so
    a broken contract fails the build instead of rendering nonsense.
    """
    if path is None:
        return None
    doc = json.loads(path.read_text())
    if isinstance(doc.get("tables"), dict) and doc["tables"]:
        ge = doc.get("great_expectations_version")
        by = f"great_expectations {ge} via lakehouse.quality" if ge else "lakehouse.quality"
        return {
            "generated_by": by,
            "suites": _suites_from_runner_result(doc, path),
        }
    suites = doc.get("suites")
    if not isinstance(suites, list) or not suites:
        raise ValueError(f"{path}: expected a non-empty 'suites' list")
    for s in suites:
        missing = {"name", "layer", "evaluated", "passed", "failed"} - set(s)
        if missing:
            raise ValueError(f"{path}: suite {s.get('name')!r} missing {sorted(missing)}")
    return {"generated_by": doc.get("generated_by", "unknown"), "suites": suites}
