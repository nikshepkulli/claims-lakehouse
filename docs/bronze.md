# Bronze ingestion

Ticket #6. Lands the raw `claimsgen` files into **append-only Delta tables** with ingestion metadata, schema capture and idempotent re-runs. Code: `src/lakehouse/bronze/`. Design context: [ADR-0001](adr/0001-medallion-layout.md), [ADR-0002](adr/0002-delta-vs-iceberg.md), [ADR-0003](adr/0003-cloud-agnostic-boundary.md).

## What it does

```
data/raw (claimsgen)                       <bronze root URI>
  claims/ingest_date=D/*.parquet    ->       claims/        Delta, partitioned by _ingest_date
  claim_lines/, diagnoses/          ->       claim_lines/, diagnoses/
  members|providers/snapshot_date=D ->       members/, providers/
  notes/ingest_date=D/*.jsonl       ->       notes/         parsed fields + `_raw` (verbatim line)
  events/claim_events.jsonl         ->       events/        parsed fields + `_raw`, routed by event_ts date
                                             _meta/schema_log    one row per dataset per batch
                                             _meta/quarantine    unreadable files / bad lines
```

A **batch** is one calendar date `D`. Running batch `D` ingests, for every dataset, the partition `*=D` (events: the rows whose `event_ts` falls on `D`). `batch_id` is `bYYYYMMDD`.

Bronze keeps data source-shaped: no casting, no dedupe, no cleansing. Parquet keeps its types; CSV (only used if a partition has no Parquet copy) lands as strings. The injected defects (duplicate claims, bad codes, orphans, late arrivals) are deliberately still present for [silver](silver.md) (#7) to catch.

### Metadata columns (last four columns of every table)

| Column | Meaning |
|---|---|
| `_ingest_ts` | timestamp of the run that wrote the row (UTC) |
| `_source_file` | file path relative to the input root, e.g. `claims/ingest_date=2024-01-03/part-00000.parquet` |
| `_batch_id` | `bYYYYMMDD` |
| `_ingest_date` | the batch date (not wall-clock); the Delta partition column |

ADR-0001 originally sketched `_ingested_at`; the implemented name is `_ingest_ts` (the ADR carries an amendment note).

## Idempotency: `replaceWhere` on the batch

Every batch write is `mode("overwrite")` + `replaceWhere = "_ingest_date = D AND _batch_id = 'bYYYYMMDD'"`, one atomic Delta commit.

- Re-running batch `D` replaces exactly the rows of batch `D` with the freshly read ones: same source, same row count, new `_ingest_ts`. Nothing is duplicated.
- Other batches are never read or rewritten, so replaying day 40 cannot disturb day 41.
- If the upstream file was corrected, the replay picks up the correction (rows are replaced, not merged by key).
- "Append-only" is logical: a batch is only ever added or atomically swapped as a unit; the Delta log keeps every prior table version, so the previous load stays readable with `versionAsOf` (replay / audit).

Why not `MERGE` on `(_source_file, row)`: raw rows have no stable key (duplicate claims are byte-identical by design), so a key-based merge would either collapse legitimate duplicates or need a row hash; batch replacement is simpler and exact. Why not plain append plus an "already ingested" ledger: it needs a second write that can disagree with the data write; `replaceWhere` is a single transaction.

Limits: if a re-run finds **no** input files for a batch, it does nothing (it will not delete a previous load). Events are re-read in full for each batch date (the stream file is one flat file); fine at this scale, partition the file upstream if it grows.

## Schema capture and drift

Per run, per dataset, `_meta/schema_log` records the observed schema (Spark schema JSON), observed columns, `columns_added`, `drift_detected`, file and row counts (one row per `(dataset, batch_id)`, replaced on re-run).

- Writes use `mergeSchema=true`, so a new column **evolves the table** instead of failing the job; earlier rows read it as `NULL`.
- **Drift** = a column in this batch that never appeared in any *earlier* batch date of that dataset (the first batch has no baseline, so it is never drift). It logs `SCHEMA_DRIFT dataset=... new_columns=[...]` at WARNING and sets `drift_detected` in the log. Because the baseline is "earlier batch dates", replaying a batch reproduces the same drift record; ingest in date order (`--batch-date all` does) so the baseline is complete.
- The injected `prior_auth_flag` column on one `claims` partition is the test case.
- **Type conflict**: a file whose existing column has a different type than the bronze table (for example CSV strings against a typed table) is quarantined rather than coerced.

## Quarantine

`_meta/quarantine` (replaced per `(dataset, batch)`, so replays do not duplicate):

- an unreadable structured file (corrupt Parquet footer, unreadable CSV) or a type-conflicting file is quarantined as a whole (`payload` is null) and the rest of the batch continues;
- a JSONL line that is not valid JSON, has a field of the wrong type, or (events) lacks a parseable `event_ts` is quarantined with the raw line in `payload`; good lines continue. Malformed events lines cannot be assigned to a date, so they are recorded under every events batch run: dedupe on `(source_file, payload)`.

### Failure modes

| Failure | Detected | Outcome | `reason` prefix |
|---|---|---|---|
| Parquet footer/magic corrupt, unreadable CSV | file open / schema read | file quarantined, rest of batch lands | `unreadable file:` |
| Column type differs from the bronze table | schema compare | file quarantined | `incompatible column types:` |
| Parquet footer valid, **a data page corrupt** (#55) | at write time (see below) | file quarantined, rest of batch lands | `CORRUPT_DATA_PAGE:` |
| Bad JSONL line / missing `event_ts` | per-line parse | line quarantined (raw line in `payload`) | `malformed JSON line...` |
| Anything else that fails the write (permissions, storage, schema merge) | write | batch fails, table untouched | n/a |

**Data-page corruption, how it is detected.** Reading is lazy, so a page-corrupt file passes the footer check and only fails when the batch write decodes it. A Delta `replaceWhere` overwrite commits nothing when it fails, so the table is untouched. On that failure only, bronze probes each Parquet file of the dataset on its own (a Spark job with the `noop` sink, which decodes every column without writing), quarantines the files that fail, and re-runs the write with the rest. If no file fails the probe, the original error is re-raised (it is not a page problem, so it is not masked). Cost: **zero extra reads on the happy path**; a failing batch pays one failed write plus one probe job per Parquet file of that dataset batch. Measured locally (2000-claim fixture, a 5-row `claims` day with 2 files, one page-corrupt, local[2]): clean batch 6.8 s, batch with a corrupt file 9.4-10.1 s, i.e. about +3 s, dominated by Spark job start-up on such tiny files; not a benchmark at scale. The alternative (probe every file up front) would add a full extra decode pass to every run; `ignoreCorruptFiles` was rejected because it silently drops or half-keeps rows with no accounting.

**Reconciliation.** Per dataset and batch: `source rows = rows_written + quarantined_file_rows + quarantined JSONL lines`. A whole-file quarantine has no decoded rows, so it is accounted at file granularity: `DatasetResult.quarantined_file_rows` (and the quarantine `reason`) carry the row count declared by the file's footer, which is readable even when a page is not. `rows_quarantined` counts quarantine *records* (one per quarantined file or line), not source rows. Footer-corrupt files have no known row count and are counted as a file only.

**Re-runs.** Quarantine rows and data rows are both replaced per `(dataset, batch)`, so re-running a batch with the same corrupt file gives identical tables; once the file is fixed, a re-run lands it and clears its quarantine row.

**Still unhandled.**

- Corruption that still decodes (flipped bits inside a value, or bytes that stay valid under the codec) is not detected: Parquet page CRCs are not verified here.
- A file corrupt in a way that fails the schema read (before the write) is the old `unreadable file:` path, with no known row count.
- Only Parquet is probed. A CSV or JSONL file broken mid-file is handled by line-level rules, not by this mechanism.
- The probe is one Spark job per file, so a batch with thousands of files and a failure pays thousands of small jobs; fine at this scale.
- Verified locally only (Spark 3.5.9 local, Delta 3.3.3, synthetic data); not run on S3A, ADLS or a cloud cluster.

## Run

Needs no local Java: Spark runs in the digest-pinned `apache/spark` image from `docker-compose.yml`; pip deps (pinned in `requirements-spark.txt`: pyspark 3.5.5, delta-spark 3.3.2) are cached in a Docker volume.

```bash
PYTHONPATH=src python -m claimsgen --rows 100000 --seed 42 --out data/raw   # needs requirements-claimsgen.txt
make bronze                                   # all dates -> data/bronze
make bronze BATCH_DATE=2024-01-03 BRONZE=/work/data/bronze   # one day (path is inside the container)
make test-spark                               # Spark test suite in Docker
```

With a local Java 17 and `pip install -r requirements-spark.txt` you can skip Docker:

```bash
PYTHONPATH=src python -m lakehouse.bronze --input data/raw --bronze data/bronze --batch-date 2024-01-03
```

### Against MinIO (S3A)

```bash
make up
# in the compose network; credentials fall back to MINIO_ROOT_USER / MINIO_ROOT_PASSWORD
LAKEHOUSE_S3_ENDPOINT=http://minio:9000 MINIO_ROOT_USER=... MINIO_ROOT_PASSWORD=... \
  python -m lakehouse.bronze --input data/raw --bronze s3a://bronze/ --batch-date 2024-01-03
```

`s3a://` roots add `hadoop-aws:3.3.4` (downloaded on first run) and the S3A settings. Verified manually on 2026-11-05 against the compose MinIO (two runs of one batch, same counts; `_delta_log` and partitions visible in the `bronze` bucket). This is **not** exercised in CI. ADLS (`abfss://`) is untested; the bronze root is a plain URI string today and is built in one function (`paths.table_uri`) until the storage abstraction (#20) replaces it.

## Tests

`tests/bronze/test_bronze_discover.py` (plain pytest, no Spark): paths, file discovery, CLI. `tests/bronze/test_bronze_spark.py` (`-m spark`, one shared SparkSession, 1000-claim fixture, about one minute): metadata columns, all datasets incl. notes and events, snapshot dims, event routing, idempotent re-run and batch isolation, replay of a corrected file plus time travel, schema drift merge + log, quarantine of corrupt files and bad lines, page-corrupt Parquet quarantine with idempotent re-run, CSV fallback and type-conflict quarantine, no-op on missing batch.

CI: `make -f mk/quality.mk test` runs `pytest -m "not spark"`; the separate `spark-tests` job (Temurin 17) runs `pytest -m spark`.
