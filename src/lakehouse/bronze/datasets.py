"""What each raw dataset looks like on disk (``claimsgen`` output, docs/data-generator.md)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Dataset:
    name: str
    kind: str  # "parquet" | "jsonl"
    partition_key: str | None  # hive key whose value is the batch date; None for a flat file
    glob_root: str  # directory under the input root
    # JSONL only: DDL of the fields parsed out of each line (the raw line is always kept too).
    json_ddl: str | None = None


NOTES_DDL = "note_id STRING, claim_id STRING, member_id STRING, note_date STRING, text STRING"
EVENTS_DDL = (
    "event_id STRING, event_type STRING, event_ts STRING, claim_id STRING, member_id STRING, "
    "provider_id STRING, claim_type STRING, amount DOUBLE"
)

DATASETS: dict[str, Dataset] = {
    d.name: d
    for d in (
        Dataset("members", "parquet", "snapshot_date", "members"),
        Dataset("providers", "parquet", "snapshot_date", "providers"),
        Dataset("claims", "parquet", "ingest_date", "claims"),
        Dataset("claim_lines", "parquet", "ingest_date", "claim_lines"),
        Dataset("diagnoses", "parquet", "ingest_date", "diagnoses"),
        Dataset("notes", "jsonl", "ingest_date", "notes", NOTES_DDL),
        Dataset("events", "jsonl", None, "events", EVENTS_DDL),
    )
}
