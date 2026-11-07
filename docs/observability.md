# Observability (ticket #23)

Run ledger, JSON-lines logs, freshness/volume alerts. Package: `src/lakehouse/observability/`
(pyarrow + PyYAML only, no Spark). Config: `config/observability.yaml`. Targets: `mk/observability.mk`.

**What ran where.** Everything here was exercised locally on synthetic data (unit tests plus a
fixture-driven end-to-end test; no Spark, no cloud). Not verified: wiring into the real
bronze/silver/gold jobs inside Docker/Spark, and a real webhook receiver (tested against a local
HTTP server only). The Airflow DAG now runs the layer tasks under `wrap` and ends with a `check` task
([orchestration.md](orchestration.md)); that wiring is spec-tested and a rendered command is run through
bash with a no-op child, but it has not run inside Airflow or with Spark. The dashboard's pipeline-runs
view reads the ledger (see [dashboard.md](dashboard.md)).

## How it fits

```
job (any layer) --track_run()/wrap--> ops/pipeline_runs/*.parquet   (run ledger)
                                  \--> JSON-lines log (stderr, LAKEHOUSE_LOG_FILE)
gold_reconciliation (Delta) --ingest-reconciliation--> ledger rows
ledger --check--> alerts: log + alerts.jsonl + optional webhook --> dashboard / Airflow / on-call
```

Jobs are not modified. Adopt per job either way:

```python
from lakehouse.observability import track_run

with track_run("silver", "silver_batch", table="claims", batch_id="2024-01-05") as run:
    ...
    run.set(rows_in=n_in, rows_out=n_out, rows_quarantined=n_bad)
```

```sh
# no code change: wrap the existing command; exit code passes through, ledger row always written
PYTHONPATH=src python -m lakehouse.observability wrap --layer silver --job silver_batch \
  --batch-id 2024-01-05 --bytes-path data/silver -- python3 -m lakehouse.silver ...
```

The wrapped child gets `LAKEHOUSE_RUN_ID` (put it in its own logs to correlate) and may write
`{"rows_in":..,"rows_out":..,"rows_quarantined":..}` to the file named by
`LAKEHOUSE_RUN_METRICS_FILE`; `--rows-in/--rows-out/--rows-quarantined` flags work too.
A failure is recorded and re-raised/propagated; a ledger-write failure is logged and never fails the job.

## Schemas (version 1)

Any breaking change bumps `schema_version` (present on every record). Additive columns do not.

### `ops/pipeline_runs/` (Parquet, one immutable `part-*.parquet` per write; `LAKEHOUSE_OPS_ROOT`, default `data/ops`)

| column | type | notes |
| --- | --- | --- |
| run_id | string | uuid4 hex; shared by all rows of one run and by its log lines |
| layer | string | bronze, silver, gold, streaming, quality, other |
| job | string | job name, e.g. `silver_batch` |
| table_name | string | what freshness/volume is judged on. Defaults to the layer name; set `--table` / `table=` or `set_table()` for per-table rows. `expected_tables` and `tables:` in the config use these names |
| batch_id | string, null | batch date or id |
| started_at, ended_at | timestamp(us, UTC) | |
| duration_s | double | |
| status | string | `success` or `failed` |
| rows_in, rows_out, rows_quarantined, bytes_out | int64, null | `rows_out` drives volume checks |
| error | string, null | `ExceptionClass: message`, truncated at 2000 chars |
| schema_version | int32 | |

Read with `pyarrow.dataset`, DuckDB `read_parquet('data/ops/pipeline_runs/*.parquet')`, Spark, or
`python -m lakehouse.observability runs` (JSON lines). It is Parquet, not Delta, so it works without Spark.
A run killed with SIGKILL before it exits writes no row (use `wrap` so the parent records it).

### Alert records (`data/ops/alerts.jsonl`, webhook body `{"schema_version":1,"alerts":[...]}`)

| key | type | notes |
| --- | --- | --- |
| schema_version | int | |
| alert_id | string | stable hash of table, rule, severity and the triggering run/timestamp: re-evaluating the same condition gives the same id, so consumers dedupe on it |
| ts | string | evaluation time, ISO-8601 UTC |
| severity | string | `warning` or `critical` (`info` reserved) |
| table | string | ledger `table_name` |
| rule | string | `freshness_sla`, `run_failed`, `volume_zscore`, `volume_drift` |
| observed | number or string | measured value (hours, z, %, or `failed` / `never_succeeded`) |
| threshold | number or string | threshold that was crossed |
| message | string | human text |
| run_id | string, null | the run that triggered it |

```json
{"schema_version": 1, "alert_id": "29d7b1f9ac1478a8", "ts": "2024-03-10T12:00:00+00:00", "severity": "critical", "table": "silver", "rule": "volume_drift", "observed": -70.0, "threshold": 60.0, "message": "rows_out=300, -70.0% vs mean of last 5 runs", "run_id": "r5-silver"}
```

### Log lines (JSON per line)

Always: `ts, level, event, run_id, layer, job, schema_version`; plus event fields. Events:
`run_started`, `run_finished` (status, duration_s, rows_*, bytes_out, error), `alert`,
`alert_sink_failed`, `ledger_write_failed`, `reconciliation_ingested`, `reconciliation_inexact`.

## Rules

| rule | fires when | severity |
| --- | --- | --- |
| `run_failed` | the table's most recent run has status `failed` (cleared by a later success) | critical |
| `freshness_sla` | age of last successful `ended_at` > `warn_after_hours` / `critical_after_hours`; an `expected_tables` entry with no success at all is critical | warning / critical |
| `volume_zscore` | \|z\| of latest successful `rows_out` vs the previous `window` successes >= `z_warn` / `z_crit` (sample stddev; skipped if stddev is 0 or fewer than `min_history` points) | warning / critical |
| `volume_drift` | \|% change vs the mean of the same window\| >= `pct_warn` / `pct_crit` (also catches zero-spread history, and a drop to 0 rows) | warning / critical |

Defaults and per-table overrides: `config/observability.yaml`. Numbers must be YAML numbers
(`1e9` without a sign parses as a string in PyYAML and is rejected at load).

## SLOs

| SLO | target | measured by |
| --- | --- | --- |
| Freshness | each expected table has a successful run within 26 h (warn) / 50 h (critical) of now; streaming 0.5 h / 2 h | `freshness_sla` |
| Reliability | latest run of every table is `success` | `run_failed` |
| Volume sanity | latest `rows_out` within 3 sigma and 30% of the trailing-14 mean | `volume_*` |
| Detection | a breach is alerted within one `check` interval (schedule `check` hourly) | scheduler config |

## Alert routing

`check` evaluates and sends every alert to enabled sinks. Exit code: 1 when any alert is at or
above `--fail-on` (default `critical`), so a scheduler task fails and the platform's own failure
routing also fires; `--fail-on never` for log-only.

| sink | default | detail |
| --- | --- | --- |
| log | on | one JSON line per alert (error for critical, warning otherwise) |
| file | on | appends to `sinks.file.path` (`data/ops/alerts.jsonl`) |
| webhook | **off** | generic JSON POST; URL read from the env var named by `url_env` (never stored in config); `min_severity` filter; failures are logged, never raised. Point it at a Slack/PagerDuty-compatible relay of your choice |

Consumers: the dashboard (#29) reads `pipeline_runs` and `alerts.jsonl` by the schemas above;
Airflow (#9) runs `wrap` around the bronze, silver and gold tasks and a `check` task after gold (wired, see above), e.g.
`BashOperator(bash_command="python -m lakehouse.observability check --fail-on critical")`.
Set `LAKEHOUSE_ALERTS_FILE` to move the file sink (Airflow keeps it next to its ledger, `<root>/ops/alerts.jsonl`); it overrides `sinks.file.path`.

## Reconciliation ingest

`python -m lakehouse.observability ingest-reconciliation --table-dir <gold>/gold_reconciliation`
(`make -f mk/observability.mk obs-reconcile`) turns each `gold_reconciliation` row into a ledger row
(`rows_in`=silver, `rows_out`=gold, `rows_quarantined`=excluded; `failed` if `identity_ok` is false or
`key_violations` > 0). It reads Delta by replaying the JSON commits in `_delta_log`; if a checkpoint
exists it cannot replay it, logs `reconciliation_inexact`, and falls back to every data file (may
double count after rewrites). Note gold already raises `ReconciliationError` on a mismatch; this adds the
history for volume trends.

## Runbook (top 5 failure modes)

Triage first: `make -f mk/observability.mk obs-runs | tail`, then grep the log for the alert's `run_id`.

1. **`run_failed` (critical).** `error` holds the exception. Find the run's log lines by `run_id`; re-run the
   same batch (jobs are idempotent per batch date per layer docs). If it fails twice, stop downstream layers and
   page the owner; do not backfill gold from a failed silver.
2. **`freshness_sla`, table has runs but is stale.** Check the scheduler first (did the DAG/cron run? paused?),
   then the upstream layer's last success. Stale bronze means source files did not arrive: check the landing
   path. Re-trigger the missed batch; the alert clears on the next success.
3. **`freshness_sla`, `never_succeeded`.** Either a new table with no run yet (expected right after a deploy: run
   it once) or the ledger is not being written (wrong `LAKEHOUSE_OPS_ROOT`, job not wrapped). Check `obs-runs` output
   exists and `table_name` matches `expected_tables`.
4. **`volume_drift` / `volume_zscore` drop.** Compare `rows_in` vs `rows_out` vs `rows_quarantined` on the run: high
   quarantine means a contract/rule break (see quality docs); low `rows_in` means a short or empty source file. If
   the drop is a legitimate holiday/partial batch, note it in the incident and let the window absorb it; do not widen
   thresholds for one event.
5. **`volume_*` spike, or reconciliation `failed`.** A spike usually means duplicate ingest or a replayed batch:
   check for repeated `batch_id` rows in the ledger and in bronze. A reconciliation failure means silver != gold +
   excluded: read `gold_reconciliation.detail`, fix the cause, rebuild the batch; never edit gold by hand.

Webhook silent? Look for `alert_sink_failed` in the log (URL env unset, endpoint down).

## Limits

Volume baselines are per `table_name` regardless of batch size; a table with legitimately uneven batches needs
wider per-table thresholds. No dedupe/suppression of repeated alerts across `check` runs beyond the stable
`alert_id` (the consumer decides). Multi-writer safety relies on one file per write; there is no compaction yet.
