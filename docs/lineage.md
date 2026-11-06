# Lineage and metadata (ticket #24)

OpenLineage events for the batch pipeline, sent to Marquez (or a JSON-lines file), with **column-level
lineage for gold**. Package: `src/lakehouse/lineage/` (stdlib + PyYAML + pyarrow; no Spark, no
`openlineage-python`). Column map: `lineage/column_lineage.yaml`. Targets: `mk/lineage.mk`.

**Status: built and tested without Docker. Not yet seen in a live Marquez.** See [Verified and not verified](#verified-and-not-verified).

## How it fits

```
layer job ──(python -m lakehouse.lineage wrap … -- <cmd>)──► START … COMPLETE|FAIL ─┐
run ledger (#23) ──(lineage emit)──────────────────────────► START + COMPLETE|FAIL ─┼─► transports
DQ result JSON (#22) ──(lineage quality)───────────────────► quality.<layer> run   ─┘   file | http | console
lineage/column_lineage.yaml ──► columnLineage facet on every gold output
```

The layer jobs are **not modified**. Two ways to produce events, both give the same run ids:

```sh
# 1. live: wrap the command (exit code passes through; START before, COMPLETE/FAIL after)
PYTHONPATH=src python -m lakehouse.lineage wrap --job silver_batch --layer silver \
  --bronze s3a://claims-bronze --silver s3a://claims-silver \
  --transport http --transport file --marquez-url http://localhost:5002 \
  -- python -m lakehouse.silver --bronze s3a://claims-bronze --silver s3a://claims-silver --batch-date all

# explicit datasets instead of --layer:  --inputs silver=/d/silver/claims … --outputs /d/gold/fact_claim
# 2. post-hoc: replay the run ledger and the DQ results (idempotent, safe to repeat)
make -f mk/lineage.mk lineage-emit lineage-quality LIN_BASE=data/e2e TRANSPORT="--transport http"
```

Nesting with the run ledger: put the observability wrap **outside** (`observability wrap -- lineage wrap
-- <cmd>`): the ledger's `LAKEHOUSE_RUN_ID` is reused, the child's `LAKEHOUSE_RUN_METRICS_FILE` (row counts)
is read for the output statistics, and the lineage run id equals the ledger `run_id` as a UUID. The other order
also works (lineage generates the id, passes it down; the ledger row carries it; counts are read back with
`--ops-root`). Both orders are tested.

## Naming convention

Datasets follow the [OpenLineage naming spec](https://openlineage.io/docs/spec/naming). Namespace = storage
system, name = object. Both come from the storage URI alone (`lakehouse_common.storage` produces these URIs), so
a table has one identity no matter which job reads or writes it, and `s3a://` and `s3://` spellings agree.

| Storage | Namespace | Name | Example name |
|---|---|---|---|
| local path / `file://` | `file://localhost` (`LAKEHOUSE_LINEAGE_FILE_HOST`) | absolute path (relative paths are resolved) | `/data/e2e/silver/claims` |
| `s3a://` / `s3://` | `s3://<bucket>` | object key, no leading `/` | `claims/claims` |
| `abfss://` | `abfss://<container>@<account>.dfs.core.windows.net` | `/<path>` | `/lakehouse/claims` |
| `gs://` | `gs://<bucket>` | object key | |

A table is `<layer root>/<table>` (`silver/claims`, `gold/fact_claim`). Anything else (`ftp://`, a host in a
`file://` URI, `abfss` without `container@account.dfs.core.windows.net`) is rejected.
**Jobs:** namespace `claims-lakehouse` (`LAKEHOUSE_LINEAGE_NAMESPACE`, e.g. one per environment), name = the job
name given to `wrap` / recorded in the ledger (`bronze`, `silver_batch`, `gold_batch`, `quality.silver`).
**Run id:** the ledger `run_id` (uuid4 hex) rendered as a UUID; non-UUID ids map to a UUIDv5, so replays agree.

Layer maps (`topology.py`, pinned to the code by tests): bronze reads the claimsgen landing directory
(`members, providers, claims, claim_lines, diagnoses, notes, events`) and writes the same names; silver
reads bronze and writes those plus `silver_quarantine`, `silver_reconciliation`; gold reads silver
(`members, providers, claims, claim_lines, diagnoses`) and writes the six star tables plus `gold_reconciliation`,
`gold_exclusions`. Other layers (streaming) get job events without datasets until mapped.

## Event example

`COMPLETE` of a silver run replayed from the ledger (trimmed to one input and one output; the real event lists
every table). Schema validity of every emitted event is tested against the vendored OpenLineage 2-0-2 schemas.

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2024-01-05T06:13:00.000Z",
  "producer": "https://github.com/nikshepkulli/claims-lakehouse/tree/main/src/lakehouse/lineage",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
  "run": {
    "runId": "22222222-2222-2222-2222-222222222222",
    "facets": {"lakehouse_run": {"layer": "silver", "rows_in": 100, "rows_out": 97, "rows_quarantined": 3, "tables": ["silver"], "ledger_rows": 1, "...": "..."}}
  },
  "job": {"namespace": "claims-lakehouse", "name": "silver_batch", "facets": {"jobType": {"processingType": "BATCH", "integration": "CLAIMS_LAKEHOUSE", "jobType": "JOB"}}},
  "inputs": [{
    "namespace": "s3://claims-bronze", "name": "claims",
    "facets": {"dataSource": {"name": "s3://claims-bronze", "uri": "s3://claims-bronze"},
               "schema": {"fields": [{"name": "claim_id", "type": "string", "ordinal_position": 1}, "..."]}}
  }],
  "outputs": [{
    "namespace": "s3://claims-silver", "name": "claims",
    "facets": {"schema": {"fields": ["..."]}},
    "outputFacets": {"outputStatistics": {"rowCount": 97, "size": 2048}}
  }]
}
```

(Each facet also carries `_producer` and `_schemaURL`; omitted above for width.)

| Facet | On | From |
|---|---|---|
| `schema` | every input and output | gold: `lakehouse.gold.schema`; bronze/silver: `quality/expectations/<layer>.yaml`; otherwise first local Parquet file + Hive partition keys; none if unknown |
| `dataSource` | every dataset | namespace |
| `outputStatistics` (`rowCount`, `size`) | outputs of `COMPLETE` | ledger rows whose `table_name` equals the table (summed over batches), or `--rows-out/--bytes-out`, or the child's metrics file (single-output jobs only) |
| `dataQualityAssertions`, `dataQualityMetrics` | inputs of `quality.<layer>` runs (and `wrap --dq-result`) | #22 result JSON: one assertion per expectation (`blocking`→`error`, `warn`→`warn`), `rowCount`, null counts from not-null expectations |
| `columnLineage` | gold star-table outputs | `lineage/column_lineage.yaml` |
| `errorMessage` | run of `FAIL` | ledger `error` / exit code |
| `lakehouse_run` (custom) <a id="run-facet-lakehouse_run"></a> | run | ledger counts for single-row runs: `layer, batch_ids, tables, ledger_rows, rows_in, rows_out, rows_quarantined, bytes_out`; DQ summary |
| `jobType` | job | constant |

`START` carries datasets and column lineage but no statistics (unknown yet). A failed run has no output
statistics. The ledger's per-table rows only populate `outputStatistics` when `table_name` names an output
table (gold reconciliation does; whole-layer rows land in `lakehouse_run`), rather than guessing a table.

## Column-level lineage (gold)

`lineage/column_lineage.yaml` is hand-written and reviewed. Per gold column it gives either source columns
(`silver.<table>.<column>`), a transformation (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`), `masking` and
`indirect` (join/filter) inputs, or `derived: "why"` for columns with no source column.

| Gold table | columns | derived (no source) | masked (keyed token) |
|---|---|---|---|
| `dim_member` | 13 | 5 (SCD2 dates, `is_current`, batch id, `loaded_at`) | `member_sk`, `member_id` |
| `dim_provider` | 12 | 5 | `provider_sk`, `provider_id` |
| `dim_diagnosis` | 6 | 3 (`description` is a reference lookup, batch id, `loaded_at`) | |
| `fact_claim` | 15 | 2 | `member_sk`, `provider_sk` |
| `fact_claim_line` | 11 | 2 | |
| `dim_date` | 9 | 9 (calendar generated by the job) | |

Derived columns are not in the emitted facet (OpenLineage has no "no source" value); `dim_date` therefore has no
`columnLineage` facet at all. Highlights: `fact_claim.member_sk` ← `silver.claims.member_id` (DIRECT,
TRANSFORMATION, masking) plus `silver.claims.service_date` (INDIRECT, the as-of join); `primary_diagnosis_sk` ←
`silver.diagnoses.icd10_code`; header totals ← `silver.claims.total_*` with the line amounts as indirect inputs
(the reconciliation rule).

**It cannot drift silently.** `tests/lineage/test_lineage_column_mapping.py` and
`make -f mk/lineage.mk lineage-check` fail when: a gold table or column is missing from the map, the map names a
column gold no longer has, or a source column is not in `quality/expectations/silver.yaml`. Change gold
(`schema.py`) and you must touch the map in the same PR. What the tests cannot check is that a mapping is
*semantically* right (that `record_hash` really hashes those five columns): that is what review of the YAML is for.
It is static: it describes the code, it is not derived from a Spark plan.

## Transports and failure behaviour

| `--transport` | Behaviour |
|---|---|
| `file` (default) | append JSON lines to `--file` (`data/lineage/events.jsonl`, `LAKEHOUSE_LINEAGE_FILE`); an identical event already in the file is skipped, so re-emitting a run adds nothing |
| `http` | `POST <url>/api/v1/lineage` (`--marquez-url`, `MARQUEZ_URL`, default `http://localhost:5002`); retries connection errors and 5xx twice with backoff, 4xx fails at once; optional bearer token from `MARQUEZ_API_KEY`; http(s) only |
| `console` | JSON line on stderr |

Repeat `--transport` to fan out. **A transport failure never fails the pipeline**: `wrap` returns the child's exit
code whatever happens; an event that cannot be sent is logged as `lineage_emit_failed` (transport, run id, job,
error) on the same JSON-lines log as the ledger, and counted. Every command ends with a `lineage_summary` log
line `{attempted, delivered, skipped_duplicates, failed, invalid, by_transport, last_error}`; alert on
`failed > 0` there. `emit` / `quality` only exit 1 on failures with `--fail-on-transport-error`. A bad dataset
URI in `wrap` logs `lineage_disabled` and still runs the job. `--validate` checks every event against the
OpenLineage schema first and refuses to send an invalid one (counted in `invalid`).

Idempotency: events are a pure function of the run (ledger `run_id`, its `started_at` / `ended_at`, counts), so
re-emitting gives byte-identical events. The file transport skips them; Marquez upserts by run id and event time
(backend behaviour, not exercised here). A `wrap` run uses wall-clock event times, so it is replayed from the
ledger, not by running `wrap` again.

## View it in Marquez

```sh
make up LINEAGE=1            # Marquez API :5002, UI :3002 (docs/local-stack.md)
make -f mk/lineage.mk lineage-emit lineage-quality LIN_BASE=data/e2e TRANSPORT="--transport http"
open http://localhost:3002   # namespace claims-lakehouse -> job silver_batch -> graph
```

To answer "click a gold column, see its sources": open the dataset `fact_claim` (namespace of your gold root) in
Marquez, switch to the column-level view and pick `member_sk`. Marquez reads this from the `columnLineage` facet.
The exact UI path depends on the Marquez version (the stack pins 0.50.0); it was not clicked through for this doc.
API check: `curl 'localhost:5002/api/v1/lineage?nodeId=dataset:<namespace>:<name>'`.

## Verified and not verified

**Verified locally (no Docker), in `tests/lineage` (87 tests, about 20 s, in the `pytest` CI job):**
event shape and schema validity of START/COMPLETE/FAIL, DQ and column-lineage facets against the vendored
OpenLineage JSON Schemas (the validator is itself tested to reject bad events); START before COMPLETE/FAIL,
one run id across them, shared with `LAKEHOUSE_RUN_ID` and the ledger in both nesting orders; dataset naming
for file / s3a / abfss; the column map against the gold schema and silver contract (including drift cases);
the hand-kept table lists against the bronze/silver/gold code; transport failure tolerance (dead HTTP endpoint,
5xx retry, 4xx, unwritable file, one dead transport does not stop another; exit code still the child's);
idempotent re-emit; all through the real CLI in subprocesses. HTTP is exercised against a **loopback stand-in
that only records POSTs** (`FakeMarquez` in `tests/lineage/conftest.py`), not against Marquez.

**Not verified:**

- **Marquez itself.** Docker was unhealthy on the build machine, so nothing was sent to a real Marquez and there is
  **no lineage-graph screenshot** (`docs/img/` has none; the ticket's README screenshot is therefore open).
  Unknowns: Marquez 0.50 accepting these namespaces/names and the custom `lakehouse_run` facet, and how its UI
  renders the column-level view.
- **Spark native OpenLineage** (`openlineage-spark` listener) is not configured. The ticket asks that "Spark and
  Airflow emit OpenLineage"; today both are covered only through the wrapper, with dataset-level counts. For
  Spark's own plan-derived column lineage later, add `spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`
  and `spark.openlineage.transport.type=http` + `spark.openlineage.transport.url=http://marquez:5000`
  (untested here; keep the same job namespace and URIs so both producers agree on dataset names).
- **Airflow.** The DAG (#9) is unchanged. To get events now, prefix a task command with `python -m lakehouse.lineage
  wrap --job <task> --layer <layer> … --`. Future: the `apache-airflow-providers-openlineage` provider
  (`[openlineage] transport` in `airflow.cfg`) emits DAG/task events automatically; it needs `LAKEHOUSE_RUN_ID`
  or a parent-run facet to join the two. Not built or tested.
- Real cloud storage: `s3a` and `abfss` naming is covered by unit tests of the URI mapping only; no event was
  produced from a run on AWS or Azure.
- dbt docs → catalog descriptions (ticket checklist item) are not done here.
- The `bronze`/`silver` jobs were not run under `wrap` in Docker/Spark; the wrapper and the job CLIs were only
  exercised separately (wrapper with stand-in commands, replay with synthetic ledger rows).

## Schema pinning and licensing

The OpenLineage JSON Schemas (core 2-0-2 and the facets used) are vendored **unmodified** in
`src/lakehouse/lineage/spec/` (Apache-2.0, license and source commit in that directory). This was chosen over
depending on `openlineage-python`: the client is a large dependency tree (and is itself Apache-2.0) while we
only build plain dicts and POST JSON; the schemas give the same correctness guarantee in tests. Validation needs
`jsonschema` (`requirements-lineage.txt`, already pulled in by the quality stack); runtime emission does not.
Facet versions come from OpenLineage `main` at fetch time (2026-11-06); a newer Marquez ignores unknown facet
versions, an older one may ignore newer facets rather than reject them (not verified).

## Limits

Dataset-level lineage for tables outside the layer maps needs explicit `--inputs/--outputs`. One ledger run id
maps to one OpenLineage run, so a ledger row set with several jobs under one id is reported under the first
job. Gold column lineage is static and describes the intended transformation, not each run's actual plan.
