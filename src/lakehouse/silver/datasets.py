"""Per-dataset silver specification: casts, standardisation, business key, rules, references."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import Column
from pyspark.sql import functions as F

from . import rules as R

AMOUNT = "decimal(12,2)"


@dataclass(frozen=True)
class Spec:
    name: str
    key: Callable[[], Column]  # business key over the standardised columns
    key_desc: str
    casts: dict[str, str]  # column -> type; a failed cast quarantines (TYPE_CAST_FAILURE)
    rules: Callable[[], list[R.Rule]]
    upper: tuple[str, ...] = ()  # enum-like string columns folded to upper case
    soft_casts: dict[str, str] = field(default_factory=dict)  # cast, null on failure, no quarantine
    optional: dict[str, str] = field(default_factory=dict)  # added as NULL when bronze lacks them
    refs: tuple[tuple[str, str], ...] = ()  # (column, reference-set name) -> `_ref_<name>`
    # Columns the rules read. One absent from bronze is treated as all-NULL (the NULL_* rules then
    # quarantine every row) instead of crashing the job on an unresolved column.
    columns: tuple[str, ...] = ()


def _c(name: str) -> Column:
    return F.col(name)


def _members_rules() -> list[R.Rule]:
    return [
        R.key_rule(),
        R.cast_rule(["birth_year", "enrollment_date"]),
        R.domain_rule("INVALID_SEX", "sex", R.SEXES),
        R.Rule(
            "INVALID_BIRTH_YEAR",
            ~_c("birth_year").between(1900, 2100),
            R.value("birth_year"),
        ),
    ]


def _providers_rules() -> list[R.Rule]:
    return [R.key_rule(), R.domain_rule("INVALID_COST_TIER", "cost_tier", R.COST_TIERS)]


def _claims_rules() -> list[R.Rule]:
    return [
        R.key_rule(),
        R.cast_rule(
            [
                "service_date",
                "received_date",
                "line_count",
                "total_billed_amount",
                "total_allowed_amount",
                "total_paid_amount",
            ]
        ),
        R.null_rule("NULL_MEMBER_ID", "member_id"),
        R.orphan_rule("ORPHAN_MEMBER_ID", "member_id", "members"),
        R.orphan_rule("ORPHAN_PROVIDER_ID", "provider_id", "providers"),
        R.null_rule("NULL_SERVICE_DATE", "service_date"),
        R.null_rule("NULL_RECEIVED_DATE", "received_date"),
        R.Rule(
            "RECEIVED_BEFORE_SERVICE",
            _c("received_date") < _c("service_date"),
            F.concat(R.value("received_date"), F.lit(", "), R.value("service_date")),
        ),
        R.domain_rule("INVALID_CLAIM_TYPE", "claim_type", R.CLAIM_TYPES),
        R.domain_rule("INVALID_CLAIM_STATUS", "claim_status", R.CLAIM_STATUSES),
    ]


def _lines_rules() -> list[R.Rule]:
    amounts = ["billed_amount", "allowed_amount", "paid_amount"]
    negative = F.lit(False)
    for a in amounts:
        negative = negative | (_c(a) < 0)
    return [
        R.key_rule(),
        R.cast_rule(["line_number", "units", "service_date", *amounts]),
        R.null_rule("NULL_CLAIM_ID", "claim_id"),
        R.orphan_rule("ORPHAN_CLAIM_ID", "claim_id", "claims_in_batch"),
        R.Rule(
            "PARENT_CLAIM_QUARANTINED",
            _c("claim_id").isNotNull() & _c("_ref_claims_in_batch") & ~_c("_ref_claims_valid"),
            F.concat(R.value("claim_id"), F.lit(" was quarantined")),
        ),
        R.null_rule("NULL_PROCEDURE_CODE", "procedure_code"),
        R.regex_rule("INVALID_PROCEDURE_CODE", "procedure_code", R.PROCEDURE_CODE_RE),
        R.Rule(
            "NULL_AMOUNT",
            (_c("billed_amount").isNull() & ~_c("_cf_billed_amount"))
            | (_c("allowed_amount").isNull() & ~_c("_cf_allowed_amount"))
            | (_c("paid_amount").isNull() & ~_c("_cf_paid_amount")),
            F.lit("billed/allowed/paid amount is null"),
        ),
        R.Rule(
            "NEGATIVE_AMOUNT",
            negative,
            F.concat(*[F.concat(R.value(a), F.lit(" ")) for a in amounts]),
        ),
        R.Rule("INVALID_UNITS", _c("units") < 1, R.value("units")),
    ]


def _diagnoses_rules() -> list[R.Rule]:
    return [
        R.key_rule(),
        R.cast_rule(["dx_seq"]),
        R.null_rule("NULL_CLAIM_ID", "claim_id"),
        R.orphan_rule("ORPHAN_CLAIM_ID", "claim_id", "claims_in_batch"),
        R.Rule(
            "PARENT_CLAIM_QUARANTINED",
            _c("claim_id").isNotNull() & _c("_ref_claims_in_batch") & ~_c("_ref_claims_valid"),
            F.concat(R.value("claim_id"), F.lit(" was quarantined")),
        ),
        R.null_rule("NULL_ICD10", "icd10_code"),
        R.regex_rule("INVALID_ICD10", "icd10_code", R.ICD10_RE),
        R.domain_rule("INVALID_DIAGNOSIS_TYPE", "diagnosis_type", R.DIAGNOSIS_TYPES),
    ]


def _notes_rules() -> list[R.Rule]:
    return [
        R.key_rule(),
        R.cast_rule(["note_date"]),
        R.null_rule("NULL_CLAIM_ID", "claim_id"),
        R.null_rule("NULL_NOTE_TEXT", "text"),
    ]


def _events_rules() -> list[R.Rule]:
    return [
        R.key_rule(),
        R.cast_rule(["event_ts", "amount"]),
        R.null_rule("NULL_EVENT_TS", "event_ts"),
        R.null_rule("NULL_CLAIM_ID", "claim_id"),
        R.domain_rule("INVALID_EVENT_TYPE", "event_type", R.EVENT_TYPES),
    ]


def _dx_key() -> Column:
    return F.when(
        _c("claim_id").isNotNull() & _c("dx_seq").isNotNull(),
        F.concat_ws("-", _c("claim_id"), _c("dx_seq").cast("string")),
    )


SPECS: dict[str, Spec] = {
    s.name: s
    for s in (
        Spec(
            "members",
            lambda: _c("member_id"),
            "member_id",
            {"birth_year": "int", "enrollment_date": "date"},
            _members_rules,
            upper=("sex", "state", "plan_type"),
            columns=("member_id", "birth_year", "enrollment_date", "sex"),
        ),
        Spec(
            "providers",
            lambda: _c("provider_id"),
            "provider_id",
            {},
            _providers_rules,
            upper=("network_status", "cost_tier", "state"),
            columns=("provider_id", "cost_tier"),
        ),
        Spec(
            "claims",
            lambda: _c("claim_id"),
            "claim_id",
            {
                "service_date": "date",
                "received_date": "date",
                "line_count": "int",
                "total_billed_amount": AMOUNT,
                "total_allowed_amount": AMOUNT,
                "total_paid_amount": AMOUNT,
            },
            _claims_rules,
            upper=("claim_type", "claim_status"),
            soft_casts={"prior_auth_flag": "boolean"},
            optional={"prior_auth_flag": "boolean"},
            refs=(("member_id", "members"), ("provider_id", "providers")),
            columns=(
                "claim_id",
                "member_id",
                "provider_id",
                "claim_type",
                "claim_status",
                "service_date",
                "received_date",
                "line_count",
                "total_billed_amount",
                "total_allowed_amount",
                "total_paid_amount",
            ),
        ),
        Spec(
            "claim_lines",
            lambda: _c("line_id"),
            "line_id",
            {
                "line_number": "int",
                "units": "int",
                "service_date": "date",
                "billed_amount": AMOUNT,
                "allowed_amount": AMOUNT,
                "paid_amount": AMOUNT,
            },
            _lines_rules,
            refs=(("claim_id", "claims_in_batch"), ("claim_id", "claims_valid")),
            columns=(
                "line_id",
                "claim_id",
                "line_number",
                "procedure_code",
                "units",
                "service_date",
                "billed_amount",
                "allowed_amount",
                "paid_amount",
            ),
        ),
        Spec(
            "diagnoses",
            _dx_key,
            "claim_id-dx_seq",
            {"dx_seq": "int"},
            _diagnoses_rules,
            upper=("diagnosis_type",),
            refs=(("claim_id", "claims_in_batch"), ("claim_id", "claims_valid")),
            columns=("claim_id", "dx_seq", "icd10_code", "diagnosis_type"),
        ),
        Spec(
            "notes",
            lambda: _c("note_id"),
            "note_id",
            {"note_date": "date"},
            _notes_rules,
            columns=("note_id", "claim_id", "text", "note_date"),
        ),
        Spec(
            "events",
            lambda: _c("event_id"),
            "event_id",
            {"event_ts": "timestamp", "amount": AMOUNT},
            _events_rules,
            columns=("event_id", "event_type", "event_ts", "claim_id", "amount"),
        ),
    )
}
