"""Pure-Python description of the daily claims pipeline. No Airflow import here.

The DAG files turn this spec into operators (``factory.py``); the unit tests assert on the spec
directly, so graph order, retry/SLA settings and write scopes are testable without a scheduler.

Idempotency: every layer CLI replaces exactly one batch (``replaceWhere _batch_id = 'bYYYYMMDD'``,
see docs/bronze.md, silver.md, gold.md). The DAG therefore passes the run's *logical date*
(``{{ ds }}``) as ``--batch-date`` to every layer and never uses wall-clock time. Re-running or
clearing a task, or backfilling a date twice, rewrites the same batch and duplicates nothing.
"""

from __future__ import annotations

import os
import re
import shlex
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

SKIP_EXIT = 99  # BashOperator default skip_on_exit_code
DS = "{{ ds }}"  # Airflow logical date, YYYY-MM-DD
SPARK_POOL = "claims_spark"
DATASETS = "claims,claim_lines,diagnoses,members,providers"  # what gold reads (as in CI e2e)

# python -m <module> -> {flag: (role, layer)}. ``write`` flags name the only place a tool writes.
CLI_FLAGS: dict[str, dict[str, tuple[str, str]]] = {
    "claimsgen": {"--out": ("write", "raw")},
    "lakehouse.bronze": {"--input": ("read", "raw"), "--bronze": ("write", "bronze")},
    "lakehouse.silver": {"--bronze": ("read", "bronze"), "--silver": ("write", "silver")},
    "lakehouse.gold": {"--silver": ("read", "silver"), "--gold": ("write", "gold")},
    "claims_dags.gates": {"--gold": ("read", "gold"), "--raw": ("read", "raw")},
}


def batch_key(ds: str | date) -> str:
    """Batch id the layer jobs use for a logical date (same formula as bronze ``batch_id_for``)."""
    d = date.fromisoformat(ds) if isinstance(ds, str) else ds
    return f"b{d:%Y%m%d}"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class PipelineConfig:
    """Where things live and how loud to be. Every field has an env override (``from_env``)."""

    repo: Path = field(default_factory=_repo_root)
    data_root: Path | None = None  # default: <repo>/data/airflow
    rows: int = 2000
    seed: int = 42
    dbt_bin: str = "dbt"
    alert_webhook_url: str | None = None  # also read at call time by callbacks.py
    schedule: str = "@daily"
    start_date: date = date(2024, 1, 1)  # claimsgen's first snapshot (members/providers) is here
    retries: int = 3
    retry_delay: timedelta = timedelta(minutes=2)
    max_retry_delay: timedelta = timedelta(minutes=30)

    @property
    def root(self) -> Path:
        return self.data_root or self.repo / "data" / "airflow"

    def layer_roots(self) -> dict[str, str]:
        r = self.root
        return {k: str(r / k) for k in ("raw", "bronze", "silver", "gold")}

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> PipelineConfig:
        e = os.environ if env is None else env
        kw: dict = {}
        if e.get("CLAIMS_REPO"):
            kw["repo"] = Path(e["CLAIMS_REPO"])
        if e.get("CLAIMS_DATA_ROOT"):
            kw["data_root"] = Path(e["CLAIMS_DATA_ROOT"])
        if e.get("CLAIMS_ROWS"):
            kw["rows"] = int(e["CLAIMS_ROWS"])
        if e.get("CLAIMS_DBT_BIN"):
            kw["dbt_bin"] = e["CLAIMS_DBT_BIN"]
        if e.get("CLAIMS_ALERT_WEBHOOK_URL"):
            kw["alert_webhook_url"] = e["CLAIMS_ALERT_WEBHOOK_URL"]
        if e.get("CLAIMS_SCHEDULE"):
            kw["schedule"] = e["CLAIMS_SCHEDULE"]
        return cls(**kw)


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    layer: str  # raw | bronze | silver | gold | dbt | gates
    command: str  # bash, may contain Jinja ({{ ds }})
    upstream: tuple[str, ...]
    execution_timeout: timedelta
    sla: timedelta  # measured from the end of the schedule interval (Airflow SLA semantics)
    retries: int
    pool: str | None = None
    writes: tuple[str, ...] = ()  # paths this task may write (must be layer roots)
    env: dict[str, str] = field(default_factory=dict)


def _py(module: str, *args: str) -> str:
    path = "src:orchestration/airflow" if module.startswith("claims_dags") else "src"
    # keep any PYTHONPATH from the environment (the local container puts its deps there)
    return f"PYTHONPATH={path}${{PYTHONPATH:+:$PYTHONPATH}} python -m {module} " + " ".join(args)


def build_daily_pipeline(cfg: PipelineConfig | None = None) -> list[TaskSpec]:
    """Task list in topological order: raw -> bronze -> silver -> gold -> dbt -> quality gates."""
    cfg = cfg or PipelineConfig()
    roots = cfg.layer_roots()
    q = shlex.quote
    retry = {"retries": cfg.retries}
    gen = _py(
        "claimsgen",
        f"--rows {cfg.rows}",
        f"--seed {cfg.seed}",
        f"--out {q(roots['raw'])}",
        "--overwrite --quiet",
    )
    gold_export = f"{roots['gold']}/export"
    return [
        TaskSpec(
            "land_raw",
            "raw",
            # seeded generator: the raw tree is the same for every run, so create it once and
            # leave it alone on re-runs (a landed file is never rewritten under a replay)
            f"test -d {q(roots['raw'] + '/claims')} || {gen}",
            (),
            timedelta(minutes=15),
            timedelta(minutes=30),
            1,
            writes=(roots["raw"],),
        ),
        TaskSpec(
            "bronze",
            "bronze",
            _py(
                "lakehouse.bronze",
                f"--input {q(roots['raw'])}",
                f"--bronze {q(roots['bronze'])}",
                f"--batch-date '{DS}'",
                f"--datasets {DATASETS}",
            ),
            ("land_raw",),
            timedelta(minutes=45),
            timedelta(hours=1),
            **retry,
            pool=SPARK_POOL,
            writes=(roots["bronze"],),
        ),
        TaskSpec(
            "silver",
            "silver",
            _py(
                "lakehouse.silver",
                f"--bronze {q(roots['bronze'])}",
                f"--silver {q(roots['silver'])}",
                f"--batch-date '{DS}'",
                f"--datasets {DATASETS}",
            ),
            ("bronze",),
            timedelta(minutes=45),
            timedelta(hours=2),
            **retry,
            pool=SPARK_POOL,
            writes=(roots["silver"],),
        ),
        TaskSpec(
            "gold",
            "gold",
            _py(
                "lakehouse.gold",
                f"--silver {q(roots['silver'])}",
                f"--gold {q(roots['gold'])}",
                f"--batch-date '{DS}'",
            ),
            ("silver",),
            timedelta(minutes=45),
            timedelta(hours=3),
            **retry,
            pool=SPARK_POOL,
            writes=(roots["gold"],),
        ),
        TaskSpec(
            "dbt_build",
            "dbt",
            # same invocation as `make gold-dbt`: dbt on DuckDB reading the real gold export
            # gold writes no fact export until a claims batch exists (first date = dimensions
            # only); exit 99 makes BashOperator mark the task, and its downstream, "skipped"
            f"{{ test -d {q(gold_export + '/fact_claim')} || "
            f"{{ echo 'no claims in gold yet: skipping dbt'; exit {SKIP_EXIT}; }}; }} && "
            f"mkdir -p {q(str(cfg.root / 'dbt'))} && "
            f"{{ test -f dbt/profiles.yml || cp dbt/profiles.yml.example dbt/profiles.yml; }} && "
            f"{cfg.dbt_bin} deps --project-dir dbt --profiles-dir dbt && "
            f"{cfg.dbt_bin} build --project-dir dbt --profiles-dir dbt --target duckdb",
            ("gold",),
            timedelta(minutes=30),
            timedelta(hours=3, minutes=30),
            **retry,
            writes=(str(cfg.root / "dbt"),),
            env={
                "GOLD_PATH": gold_export,
                "DBT_DUCKDB_PATH": str(cfg.root / "dbt" / "claims.duckdb"),
            },
        ),
        TaskSpec(
            "quality_gates",
            "gates",
            # Deterministic checks: a retry cannot fix them, so retries=0.
            _py(
                "claims_dags.gates",
                f"--gold {q(roots['gold'])}",
                f"--raw {q(roots['raw'])}",
                f"--batch-date '{DS}'",
            ),
            ("dbt_build",),
            timedelta(minutes=10),
            timedelta(hours=4),
            0,
        ),
    ]


_SEG_SPLIT = re.compile(r"\s*(?:&&|\|\||;)\s*")


def parse_cli(command: str) -> tuple[str, dict[str, str]] | None:
    """Return ``(module, {flag: value})`` for the ``python -m <module>`` call in ``command``."""
    for seg in _SEG_SPLIT.split(command):
        try:
            toks = shlex.split(seg)
        except ValueError:
            continue
        if "-m" in toks:
            i = toks.index("-m")
            flags: dict[str, str] = {}
            rest = toks[i + 2 :]
            for j, t in enumerate(rest):
                if t.startswith("--") and j + 1 < len(rest) and not rest[j + 1].startswith("--"):
                    flags[t] = rest[j + 1]
            return toks[i + 1], flags
    return None
