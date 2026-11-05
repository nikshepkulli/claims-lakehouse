# Silver layer

Ticket #7. Turns [bronze](bronze.md) Delta tables into **trusted, typed, deduplicated** silver Delta tables, and routes every row that cannot be trusted to a **quarantine table** with a reason code instead of dropping it. Code: `src/lakehouse/silver/`. Design context: [ADR-0001](adr/0001-medallion-layout.md) (silver = typed, deduped, validated, quarantine-not-drop), [ADR-0002](adr/0002-delta-vs-iceberg.md), [ADR-0003](adr/0003-cloud-agnostic-boundary.md).

## What it does

```
<bronze root>                         <silver root>
  claims, claim_lines, diagnoses  ->    claims, claim_lines, diagnoses   typed, deduped, valid rows
  members, providers              ->    members, providers
  notes, events                   ->    notes, events                    (`_raw` dropped; bronze keeps it)
                                        silver_quarantine                one row per row that failed
                                        silver_reconciliation            one row per dataset per batch
```

A **batch** is a bronze `_ingest_date` D (same `bYYYYMMDD` id as bronze). Silver batch D reads bronze rows `_ingest_date = D` and writes silver rows for D. `--batch-date all` walks every bronze batch date in order.

Every bronze row ends in exactly one bucket:

| Bucket | Meaning |
|---|---|
| `silver` | the dedupe winner, and it passed every rule |
| `quarantined` | a winner that broke at least one rule, **or** a conflicting duplicate |
| `deduped` | a loser that is identical to its winner (nothing is lost) |

## Rules

Reason codes are the stable contract with quality tooling (#22, #13). A row that breaks several rules gets **one** quarantine row: `reason_code` is the first rule in the order below, `all_reason_codes` lists every rule it broke (so "how many lines have an invalid procedure code" is always answerable, even for lines also quarantined because their claim was).

| Rule order | Reason code | Datasets | Example (from claimsgen) | How detected |
|---|---|---|---|---|
| dedupe | `CONFLICTING_DUPLICATE` | all | same `claim_id` re-sent with a different amount | same business key, different row hash, not the winner |
| 1 | `NULL_KEY` | all | `claim_id` NULL | business key is null |
| 2 | `TYPE_CAST_FAILURE` | all typed columns | `billed_amount = 'abc'` (CSV fallback in bronze lands strings) | value present but `cast` yields NULL |
| 3 | `NULL_MEMBER_ID`, `NULL_CLAIM_ID`, `NULL_SERVICE_DATE`, `NULL_RECEIVED_DATE`, `NULL_EVENT_TS`, `NULL_NOTE_TEXT`, `NULL_AMOUNT` | claims, lines, diagnoses, notes, events | required field missing | `IS NULL` (and not a cast failure) |
| 4 | `ORPHAN_MEMBER_ID` | claims | `member_id` absent from members (defect `orphan_member_id`) | left-join to the members that themselves pass silver rules |
| 4 | `ORPHAN_PROVIDER_ID` | claims | provider absent from providers | same, against providers |
| 4 | `ORPHAN_CLAIM_ID` | claim_lines, diagnoses | `claim_id` not in this batch's claims | left-join to the batch's claims |
| 4 | `PARENT_CLAIM_QUARANTINED` | claim_lines, diagnoses | child of an orphan-member claim | claim exists in the batch but did not reach silver |
| 5 | `NULL_PROCEDURE_CODE` | claim_lines | NULL (defect `null_procedure_code`) | `IS NULL` after trim/blank->NULL |
| 5 | `INVALID_PROCEDURE_CODE` | claim_lines | `ABCDE`, `99-99`, `0000` (defect `invalid_procedure_code`) | not `^(\d{5}\|[A-V]\d{4})$` |
| 5 | `NULL_ICD10` | diagnoses | NULL (defect `null_icd10`) | `IS NULL` |
| 5 | `INVALID_ICD10` | diagnoses | `123.45`, `??.1`, `i1o` (defect `invalid_icd10`) | not `^[A-TV-Z]\d[0-9A-Z](\.[0-9A-Z]{1,4})?$` |
| 5 | `NEGATIVE_AMOUNT` | claim_lines | billed/allowed/paid negated (defect `negative_amount`) | any of the three `< 0` |
| 5 | `INVALID_UNITS` | claim_lines | `units = 0` | `units < 1` |
| 5 | `RECEIVED_BEFORE_SERVICE` | claims | `received_date` earlier than `service_date` | date comparison |
| 5 | `INVALID_CLAIM_TYPE`, `INVALID_CLAIM_STATUS`, `INVALID_SEX`, `INVALID_COST_TIER`, `INVALID_DIAGNOSIS_TYPE`, `INVALID_EVENT_TYPE`, `INVALID_BIRTH_YEAR` | per dataset | `claim_type = 'DENTAL'` | not in the documented domain |

Notes on specific choices:

- **Standardisation.** Strings are trimmed and blank becomes NULL; enum-like columns (`claim_type`, `claim_status`, `sex`, `plan_type`, `cost_tier`, ...) are upper-cased. **Procedure and ICD-10 codes are trimmed but never case-folded**: the validity regexes are case-sensitive and the generator's `i1o` is only invalid because it is lower case; "repairing" it would hide a real defect.
- **Types.** Amounts become `DECIMAL(12,2)`, dates `DATE`, counts `INT`, `event_ts` `TIMESTAMP` (UTC). Bronze Parquet is already typed; the casts matter for the CSV fallback, where bronze lands strings.
- **Regexes** are the exact ones in [data-generator.md](data-generator.md), evaluated with `\z` instead of `$` (Java's `$` accepts a trailing newline).
- **Header totals** on `claims` are not sign-checked: they are derived from the lines (the generator negates lines and header totals follow). Instead silver `claims` carries `lines_in_silver` and `is_line_complete` (`lines_in_silver = line_count`), so a consumer can tell that a claim lost a quarantined line and its header total no longer matches the silver lines. Quarantining the header would drop the good sibling lines too.
- **Event `amount`** is cast but not sign-checked: claimsgen derives event amounts from header totals, which can be negative. The authoritative amounts are in `claim_lines`.
- **Referential reference sets.** Members/providers are checked against the bronze dimension rows (all snapshots) that pass the dimension's own silver rules, not against the silver dimension tables, so a claim batch does not depend on the dimension batch having run first. If bronze has no `members`/`providers` table at all the job fails (otherwise every claim would be an orphan).
- **Out-of-order events** are not a defect for batch silver: ordering is carried by `event_ts`; they are kept. Watermarking is the streaming job's concern (#12).
- **Schema drift.** `claims.prior_auth_flag` (boolean, cast leniently: unparseable -> NULL) is always present in silver `claims`, NULL for every partition that did not carry it. Drift is already recorded by bronze (`_meta/schema_log`); silver never fails or quarantines because of it.

### Late arrivals

A claim whose `received_date - service_date` exceeds `LATE_ARRIVAL_DAYS = 30` (claimsgen's normal lag is 0-2 days, injected late arrivals 31-90) is **kept and flagged**: `is_late_arrival = true` and `arrival_lag_days` on `claims`. Late claims are valid claims that landed in a far-later batch; nothing is discarded and no reason code is raised. Only the header is flagged (lines and diagnoses carry no `received_date`; join on `claim_id`).

## Dedupe

Business keys: `claim_id`; `line_id`; `claim_id` + `dx_seq`; `member_id`; `provider_id`; `note_id`; `event_id`.

**Winner rule**, within one batch, per key: greatest `_ingest_ts`, then greatest `_source_file` (string order), then greatest `_row_hash` (SHA-256 of all source columns, sorted by name, nulls ignored). The rule is total and independent of read order, so the same bronze always yields the same winner (tested by presenting the same rows in reverse order). "Latest wins" follows ADR-0001's latest-adjustment-wins.

Among the non-winners:

- **Exact duplicate** (same row hash as the winner): dropped and counted in `deduped_rows`. This is the generator's `duplicate_claim` / `duplicate_event` (byte-identical re-sends).
- **Conflicting duplicate** (same key, different content): **quarantined** as `CONFLICTING_DUPLICATE` with the full row in `payload`, never silently dropped. The generator does not inject these; they are covered by hand-built tests.

The winner is then validated like any other row, so a duplicated orphan claim is quarantined once, not twice.

Scope limit: dedupe is **within a batch**. Re-sends of the same key in a *different* batch (a true cross-batch adjustment) are two silver rows until a later ticket adds cross-batch resolution. claimsgen never does this: duplicates land in the same `ingest_date`, and duplicate events are byte-identical so they carry the same `event_ts` and therefore the same event-date batch.

## Quarantine table: `silver_quarantine`

| Column | Meaning |
|---|---|
| `dataset` | `claims`, `claim_lines`, ... |
| `business_key` | key of the rejected row (`line_id`, `claim_id-dx_seq`, ...); NULL when the key itself is null |
| `reason_code` | first broken rule (table above) |
| `reason_detail` | human readable: every broken rule with the offending value |
| `all_reason_codes` | every rule the row broke (array) |
| `source_batch` | bronze `_batch_id` |
| `source_file` | bronze `_source_file` |
| `payload` | the bronze row as JSON (nulls kept), enough to replay or fix it |
| `_ingest_date` | batch date; the Delta partition column |
| `_quarantined_at` | run timestamp |

Nothing reads quarantine back into silver automatically: fix the upstream data (or the rule), re-run the batch, and the row moves.

## Reconciliation: `silver_reconciliation`

One row per `(dataset, batch_id)`: `bronze_rows`, `silver_rows`, `quarantined_rows`, `deduped_rows`, `identity_ok`, `_run_ts`.

The identity `bronze_rows = silver_rows + quarantined_rows + deduped_rows` is **asserted in code**. `bronze_rows` is an independent count of the bronze batch; `silver_rows` and `quarantined_rows` are re-read from the Delta tables after the write (not taken from in-memory counters), so a lost or duplicated row anywhere in the job shows up. On a mismatch the numbers are still written (with `identity_ok = false`) and the job raises `ReconciliationError`; the CLI exits 1 after finishing the other batches. The test suite breaks the identity on purpose to prove this.

Quarantine and silver are written as separate Delta commits, so a crash between them leaves a batch half-written; re-running the batch repairs it (see below), and a failed batch has no reconciliation row marked ok.

## Idempotency

Same approach as bronze: every table is written with `mode("overwrite")` + `replaceWhere` on exactly the batch (`_ingest_date = D AND _batch_id = 'bYYYYMMDD'`; quarantine: `dataset = X AND _ingest_date = D`; reconciliation: `dataset = X AND batch_id = ...`). Re-running batch D atomically swaps D's rows, leaves every other batch alone and never duplicates. Silver is a pure function of the bronze batch plus the dimension reference sets, so a re-run produces the same rows and the same `_row_hash`es; only `_silver_ts` / `_quarantined_at` change. If a rule change makes fewer rows invalid, the re-run clears the old quarantine rows for that dataset and batch (an empty replacement is written too). Time travel keeps the previous version.

If bronze has no rows for a batch, silver does nothing for that dataset (it will not delete a previous load).

## Partitioning and file size

- **Partition column: `_ingest_date`** (the batch date) on every silver table and on the quarantine. It matches the `replaceWhere` unit, so a batch rewrite touches only its own partition, and it is the column downstream incremental readers (gold #8, Airflow backfills #9) filter on. Business dates (`service_date`) are *not* partition columns: late arrivals would scatter one batch over many partitions, and a replay would have to rewrite them all.
- **Files per batch.** A naive write produces one file per shuffle partition (200 by default), so a day of a few thousand rows would become 200 tiny files. Each batch write is `repartition(ceil(rows / ROWS_PER_FILE))` with `ROWS_PER_FILE = 1_000_000` (about 100-200 MB of Parquet for these narrow tables), i.e. **one file per partition** until a day exceeds that. A test asserts one file per batch partition.
- **Daily partitions are still small at this scale.** claimsgen at 100k claims/year is a few hundred claims per day, so each partition file is small by design (~370 partitions, one file each). Real volume is the cure; until then use the compaction hook below instead of coarser partitions, which would break batch-level replace.
- **OPTIMIZE hook.** `python -m lakehouse.silver ... --optimize` (or `lakehouse.silver.job.optimize_tables`) runs Delta `OPTIMIZE` (bin-packing compaction) on every silver table. It is **off by default and was only exercised on the local test fixture**; schedule it after backfills (Airflow, #9) and pair it with `VACUUM` retention as ADR-0001 notes. Z-ordering is deliberately not done: keys are high-cardinality strings and the tables are small.

## Run

Like bronze, no local Java is needed: the make targets run Spark in the digest-pinned `apache/spark` image (see `mk/bronze.mk`).

```bash
PYTHONPATH=src python -m claimsgen --rows 100000 --seed 42 --out data/raw
make bronze                                    # all dates -> data/bronze
make silver                                    # all bronze batches -> data/silver
make silver BATCH_DATE=2024-01-03              # one batch (paths are inside the container)
make test-spark                                # bronze + silver Spark suites in Docker
```

With a local Java 17 and `pip install -r requirements-spark.txt`:

```bash
PYTHONPATH=src python -m lakehouse.silver --bronze data/bronze --silver data/silver --batch-date all --optimize
```

Roots are plain URIs (`/path`, `file://`, `s3a://bucket/prefix`), built in `lakehouse.silver.paths`, which delegates to the same single function as bronze until the storage abstraction (#20) lands. `s3a://` was **not** exercised for silver (bronze's MinIO check does not extend to it).

## Tests

`tests/silver/test_silver_cli.py` (plain pytest): argument handling and a guard that `rules.py` still carries the regexes documented in data-generator.md.

`tests/silver/test_silver_e2e.py` and `tests/silver/test_silver_rules.py` (`-m spark`):

- **Manifest-driven end to end** (`test_silver_e2e.py`). A 1000-claim claimsgen fixture goes through the real bronze ingest (its 370-odd daily partitions are merged into 2 batches to keep the suite fast) and then silver. For **every** defect in `defects_manifest.json` there is a check, parametrised over the manifest, and a test fails if the generator gains a defect without one: quarantined distinct keys per reason equal the manifest `count` (sample ids included), duplicates are deduped with counts equal to bronze rows minus distinct keys, late arrivals are all retained and flagged, out-of-order events are all kept, schema drift is carried as a nullable boolean.
- Reconciliation identity per dataset and batch, matched against independent bronze counts; quarantine schema and payload; silver referential cleanliness and types; idempotent re-run (same counts and `_row_hash`es) and single-batch replay isolation; OPTIMIZE; one file per batch partition.
- Hand-built bronze rows for what claimsgen does not produce (`test_silver_rules.py`): deterministic winner (timestamp, then file, then hash; reversed input order), conflicting duplicates, cast failures, trimming, case-sensitive codes, orphan claim ids, late-arrival boundary (30 days), forced reconciliation failure, missing dimension, and a CLI subprocess run.

CI runs three parallel legs of the `spark tests` job (bronze, silver-e2e, silver-rules); the first CI run of the combined silver suite took about 9 minutes, hence the split and the smaller fixture.
