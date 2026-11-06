"""Build OpenLineage ``RunEvent`` dicts (spec 2-0-2). Pure functions: no I/O, no clocks.

Everything that varies (event time, run id, counts) is passed in, so rebuilding the events of a
finished run yields byte-identical JSON. That is what makes post-hoc re-emit idempotent.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import column_lineage as CL
from .naming import PRODUCER, job_namespace
from .schemas import fields_for, schema_facet_fields
from .topology import IO

SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
FACET_URLS = {
    "schema": "https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
    "dataSource": "https://openlineage.io/spec/facets/1-0-1/DatasourceDatasetFacet.json#/$defs/DatasourceDatasetFacet",
    "outputStatistics": "https://openlineage.io/spec/facets/1-0-2/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet",
    "errorMessage": "https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet",
    "jobType": "https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json#/$defs/JobTypeJobFacet",
}
# custom facet: schema lives in this repo (docs/lineage.md); only the BaseFacet contract applies
RUN_FACET_URL = "https://github.com/nikshepkulli/claims-lakehouse/blob/main/docs/lineage.md#run-facet-lakehouse_run"

EVENT_TYPES = ("START", "COMPLETE", "FAIL")


def run_uuid(run_id: str) -> str:
    """Ledger ids are uuid4 hex; OpenLineage wants a UUID. Other ids map deterministically."""
    try:
        return str(uuid.UUID(hex=run_id))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"claims-lakehouse:run:{run_id}"))


def iso(t: datetime) -> str:
    """RFC 3339 UTC with milliseconds (naive datetimes are taken as UTC)."""
    if t.tzinfo is None:
        t = t.replace(tzinfo=UTC)
    return t.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _facet(url_key: str, **body: Any) -> dict[str, Any]:
    return {"_producer": PRODUCER, "_schemaURL": FACET_URLS[url_key], **body}


@dataclass
class Stats:
    rows: int | None = None
    bytes: int | None = None


@dataclass
class RunSpec:
    """Everything needed to render the events of one run."""

    run_id: str
    job: str
    inputs: list[IO] = field(default_factory=list)
    outputs: list[IO] = field(default_factory=list)
    layer_roots: dict[str, str] = field(default_factory=dict)  # for column-lineage resolution
    stats: dict[str, Stats] = field(default_factory=dict)  # output table -> counts (COMPLETE only)
    input_facets: dict[str, dict[str, Any]] = field(default_factory=dict)  # input table -> facets
    run_info: dict[str, Any] = field(default_factory=dict)  # custom `lakehouse_run` facet body
    layer: str | None = None
    mapping: CL.ColumnLineage | None = None


def _dataset(io: IO, spec: RunSpec, role: str, with_stats: bool) -> dict[str, Any]:
    facets: dict[str, Any] = {}
    local = Path(io.uri) if io.ref.namespace.startswith("file://") and io.uri else None
    fields = fields_for(io.table, None if io.layer in ("landing", "") else io.layer, local)
    if fields:
        facets["schema"] = _facet("schema", fields=schema_facet_fields(fields))
    facets["dataSource"] = _facet("dataSource", name=io.ref.namespace, uri=io.ref.namespace)
    d: dict[str, Any] = {"namespace": io.ref.namespace, "name": io.ref.name, "facets": facets}
    if role == "input":
        if io.table in spec.input_facets:
            d["inputFacets"] = spec.input_facets[io.table]
        return d
    if io.layer == "gold" and spec.layer_roots:
        cl = CL.facet(io.table, spec.layer_roots, spec.mapping)
        if cl:
            facets["columnLineage"] = cl
    st = spec.stats.get(io.table)
    if with_stats and st and (st.rows is not None or st.bytes is not None):
        body: dict[str, Any] = {}
        if st.rows is not None:
            body["rowCount"] = int(st.rows)
        if st.bytes is not None:
            body["size"] = int(st.bytes)
        d["outputFacets"] = {"outputStatistics": _facet("outputStatistics", **body)}
    return d


def build_event(
    spec: RunSpec, event_type: str, event_time: datetime, *, error: str | None = None
) -> dict[str, Any]:
    if event_type not in EVENT_TYPES:
        raise ValueError(f"event_type must be one of {EVENT_TYPES}")
    complete = event_type == "COMPLETE"
    run_facets: dict[str, Any] = {}
    if spec.run_info:
        run_facets["lakehouse_run"] = {
            "_producer": PRODUCER,
            "_schemaURL": RUN_FACET_URL,
            **{k: v for k, v in spec.run_info.items() if v is not None},
        }
    if event_type == "FAIL":
        run_facets["errorMessage"] = _facet(
            "errorMessage", message=(error or "run failed")[:2000], programmingLanguage="PYTHON"
        )
    return {
        "eventType": event_type,
        "eventTime": iso(event_time),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": {"runId": run_uuid(spec.run_id), "facets": run_facets},
        "job": {
            "namespace": job_namespace(),
            "name": spec.job,
            "facets": {
                "jobType": _facet(
                    "jobType", processingType="BATCH", integration="CLAIMS_LAKEHOUSE", jobType="JOB"
                )
            },
        },
        "inputs": [_dataset(i, spec, "input", False) for i in spec.inputs],
        "outputs": [_dataset(o, spec, "output", complete) for o in spec.outputs],
    }
