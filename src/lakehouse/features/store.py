"""Build, verify and read the partitioned Parquet feature store.

Layout (``<out>`` defaults to ``<gold root>/features``)::

    <out>/feature_store/member_features/as_of_month=2024-03/part-00000.parquet
    <out>/feature_store/provider_features/as_of_month=.../part-00000.parquet
    <out>/feature_store/claim_features/as_of_month=.../part-00000.parquet
    <out>/labels/claim_denial/as_of_month=.../part-00000.parquet      <- labels, NEVER features
    <out>/_manifest.json                                               <- hashes, config, counts

The build is deterministic and idempotent: the manifest has no wall-clock field, rows are
ordered by key, single-threaded DuckDB, exact integer arithmetic for the statistics.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from . import registry
from .config import SCHEMA_VERSION, FeatureConfig
from .guards import assert_label_separation, run_leakage_guard
from .sql import (
    Names,
    build_claim_features,
    build_member_features,
    build_provider_features,
    prepare_sources,
)

TABLES = {
    "member": ("member_features", ["member_id", "as_of"]),
    "provider": ("provider_features", ["provider_id", "as_of"]),
    "claim": ("claim_features", ["claim_id"]),
}
LABEL_TABLE = "claim_denial"


def connect():
    import duckdb

    con = duckdb.connect()
    con.execute("PRAGMA threads=1")  # deterministic aggregation order
    return con


def table_hash(con, table: str, keys: list[str]) -> str:
    """sha256 of the table's rows in key order (row -> VARCHAR is deterministic)."""
    order = ", ".join(keys)
    return con.execute(
        f"SELECT sha256(coalesce(string_agg(CAST(t AS VARCHAR), '|' ORDER BY {order}), '')) "
        f"FROM {table} t"
    ).fetchone()[0]


def _write_partitions(con, table: str, root: Path, keys: list[str]) -> list[str]:
    if root.exists():
        shutil.rmtree(root)
    months = [
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT strftime(as_of, '%Y-%m') FROM {table} ORDER BY 1"
        ).fetchall()
    ]
    for m in months:
        d = root / f"as_of_month={m}"
        d.mkdir(parents=True)
        con.execute(
            f"COPY (SELECT * FROM {table} WHERE strftime(as_of, '%Y-%m') = '{m}' "
            f"ORDER BY {', '.join(keys)}) TO '{d}/part-00000.parquet' "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
    return months


def month_grid_sql() -> str:
    return """
        SELECT CAST(g AS TIMESTAMP) AS as_of FROM generate_series(
          (SELECT CAST(date_trunc('month', min(received_date)) AS DATE) FROM src_claims),
          (SELECT CAST(date_trunc('month', max(received_date)) AS DATE) + INTERVAL 1 MONTH
             FROM src_claims),
          INTERVAL 1 MONTH) t(g)
    """


def build(
    gold_export: str | Path,
    out: str | Path,
    cfg: FeatureConfig | None = None,
    guard: bool = True,
    probes: int = 4,
) -> dict:
    """Build the whole store. Raises ``LeakageError`` (store left untouched) if a guard trips."""
    cfg = cfg or FeatureConfig()
    out = Path(out)
    feats = registry.load()
    con = connect()
    prepare_sources(con, str(gold_export), cfg)
    n = Names()
    guard_summary = run_leakage_guard(con, cfg, probes) if guard else None

    con.execute(f"CREATE OR REPLACE TABLE _grid AS {month_grid_sql()}")
    con.execute(
        "CREATE OR REPLACE TABLE _ms AS SELECT m.member_id, g.as_of "
        "FROM (SELECT DISTINCT member_id FROM src_dim_member) m CROSS JOIN _grid g"
    )
    con.execute(
        "CREATE OR REPLACE TABLE _ps AS SELECT p.provider_id, g.as_of "
        "FROM (SELECT DISTINCT provider_id FROM src_dim_provider) p CROSS JOIN _grid g"
    )
    build_member_features(con, n, "_ms", "member_features", cfg)
    build_provider_features(con, n, "_ps", "provider_features", cfg)
    build_claim_features(con, n, "claim_features", cfg)
    con.execute(
        """
        CREATE OR REPLACE TABLE labels AS
        SELECT claim_id, CAST(received_date AS TIMESTAMP) AS as_of,
               status = 'DENIED' AS is_denied, outcome_ts AS label_available_at
        FROM src_claims ORDER BY claim_id
        """
    )

    manifest: dict = {
        "schema_version": SCHEMA_VERSION,
        "config": cfg.as_dict(),
        "registry_sha256": hashlib.sha256(registry.registry_path().read_bytes()).hexdigest(),
        "sources": {
            t: table_hash(con, t, k)
            for t, k in (
                ("src_claims", ["claim_id"]),
                ("src_dim_member", ["member_id", "effective_from"]),
                ("src_dim_provider", ["provider_id", "effective_from"]),
            )
        },
        "leakage_guard": guard_summary,
        "tables": {},
    }
    for entity, (name, keys) in TABLES.items():
        cols = [r[0] for r in con.execute(f"DESCRIBE {name}").fetchall()]
        assert_label_separation(cols, name)
        expected = set(registry.KEYS[entity]) | set(registry.columns(entity, feats))
        if set(cols) != expected:
            raise registry.FeatureRegistryError(
                f"{name}: columns differ from the registry: extra={sorted(set(cols) - expected)} "
                f"missing={sorted(expected - set(cols))}"
            )
        months = _write_partitions(con, name, out / "feature_store" / name, keys)
        manifest["tables"][name] = {
            "rows": con.execute(f"SELECT count(*) FROM {name}").fetchone()[0],
            "partitions": months,
            "sha256": table_hash(con, name, keys),
        }
    _write_partitions(con, "labels", out / "labels" / LABEL_TABLE, ["claim_id"])
    manifest["tables"]["labels"] = {
        "rows": con.execute("SELECT count(*) FROM labels").fetchone()[0],
        "sha256": table_hash(con, "labels", ["claim_id"]),
    }
    out.mkdir(parents=True, exist_ok=True)
    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    con.close()
    return manifest


def read_table(con, out: str | Path, name: str, view: str | None = None) -> str:
    """Register a stored table as a DuckDB view (hive partition column ``as_of_month``)."""
    out = Path(out)
    root = out / ("labels" if name == LABEL_TABLE else "feature_store") / name
    view = view or name
    con.execute(
        f"CREATE OR REPLACE VIEW {view} AS SELECT * EXCLUDE (as_of_month) "
        f"FROM read_parquet('{root}/*/*.parquet', hive_partitioning = true)"
    )
    return view


def verify(out: str | Path) -> dict:
    """Re-read the Parquet files and compare to the manifest hashes. Returns the manifest."""
    out = Path(out)
    manifest = json.loads((out / "_manifest.json").read_text())
    con = connect()
    keys = {**{v[0]: v[1] for v in TABLES.values()}, LABEL_TABLE: ["claim_id"]}
    for name, key in keys.items():
        view = read_table(con, out, name)
        got = table_hash(con, view, key)
        want = manifest["tables"]["labels" if name == LABEL_TABLE else name]["sha256"]
        if got != want:
            raise ValueError(f"{name}: stored files do not match the manifest hash")
    con.close()
    return manifest


def training_set(out: str | Path, con=None):
    """Claim features joined to labels by claim_id and as_of; the ONLY place they meet.

    Fails if any label was already knowable at its row's as_of (that would mean the row's
    as_of is wrong, a leakage precondition for #27).
    """
    con = con or connect()
    read_table(con, out, "claim_features")
    read_table(con, out, LABEL_TABLE)
    bad = con.execute(
        f"SELECT count(*) FROM claim_features f JOIN {LABEL_TABLE} l USING (claim_id) "
        "WHERE l.label_available_at <= f.as_of"
    ).fetchone()[0]
    if bad:
        raise ValueError(f"{bad} labels are available at or before their feature as_of")
    return con.execute(
        f"SELECT f.*, l.is_denied FROM claim_features f JOIN {LABEL_TABLE} l "
        "ON l.claim_id = f.claim_id AND l.as_of = f.as_of ORDER BY f.claim_id"
    ).to_arrow_table()
