"""Validity rules and reason codes. The catalogue in docs/silver.md is generated from this."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F

# Exact patterns from docs/data-generator.md (defect catalogue). Python ``re`` semantics.
PROCEDURE_CODE_RE = r"^(\d{5}|[A-V]\d{4})$"
ICD10_RE = r"^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$"

# Normal receipt lag is 0-2 days; the generator's late arrivals are 31-90 days.
LATE_ARRIVAL_DAYS = 30

CLAIM_TYPES = ("PROFESSIONAL", "INSTITUTIONAL", "PHARMACY")
CLAIM_STATUSES = ("PAID", "DENIED")
SEXES = ("F", "M", "U")
COST_TIERS = ("STANDARD", "ELEVATED", "HIGH_COST")
DIAGNOSIS_TYPES = ("PRIMARY", "SECONDARY")
EVENT_TYPES = ("claim_submitted", "claim_paid", "claim_denied")

# Reason codes (the full list, with examples, lives in docs/silver.md).
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"


def java_regex(pattern: str) -> str:
    """Make a Python ``^...$`` pattern safe for Spark's ``rlike`` (Java ``$`` also matches before
    a trailing newline; ``\\z`` does not)."""
    assert pattern.startswith("^") and pattern.endswith("$")
    return pattern[:-1] + r"\z"


@dataclass(frozen=True)
class Rule:
    code: str
    fail: Column  # true when the row violates the rule (null counts as "not violated")
    detail: Column  # human readable string describing the offending value


def value(col: str) -> Column:
    return F.concat(F.lit(f"{col}="), F.coalesce(F.col(col).cast("string"), F.lit("NULL")))


def null_rule(code: str, col: str) -> Rule:
    """``col`` is null. A value that existed but failed its cast is TYPE_CAST_FAILURE instead."""
    return Rule(code, F.col(col).isNull() & ~F.col(f"_cf_{col}"), F.lit(f"{col} is null"))


def domain_rule(code: str, col: str, allowed: tuple[str, ...]) -> Rule:
    return Rule(code, F.col(col).isNotNull() & ~F.col(col).isin(*allowed), value(col))


def regex_rule(code: str, col: str, pattern: str) -> Rule:
    return Rule(
        code,
        F.col(col).isNotNull() & ~F.col(col).rlike(java_regex(pattern)),
        F.concat(value(col), F.lit(f" does not match {pattern}")),
    )


def cast_rule(cols: list[str]) -> Rule:
    failed = F.array_join(F.array(*[F.when(F.col(f"_cf_{c}"), F.lit(c)) for c in cols]), ",")
    any_fail = F.lit(False)
    for c in cols:
        any_fail = any_fail | F.col(f"_cf_{c}")
    return Rule("TYPE_CAST_FAILURE", any_fail, F.concat(F.lit("cannot cast: "), failed))


def key_rule(col: str = "_key") -> Rule:
    return Rule("NULL_KEY", F.col(col).isNull(), F.lit("business key is null"))


def orphan_rule(code: str, col: str, ref: str) -> Rule:
    return Rule(
        code,
        F.col(col).isNotNull() & ~F.col(f"_ref_{ref}"),
        F.concat(value(col), F.lit(f" not found in {ref}")),
    )
