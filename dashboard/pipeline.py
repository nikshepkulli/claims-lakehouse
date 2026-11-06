"""Inputs for the "Pipeline runs" view (ticket #30): lineage graph, run timeline, quality trend.

Every input is optional and read from files the repository's own CLIs write; nothing is made up:

* ``--lineage-events``: OpenLineage JSON lines (``python -m lakehouse.lineage wrap|emit|quality``);
* ``--column-lineage``: ``lineage/column_lineage.yaml`` (gold column -> silver source columns);
* ``--runs``: the observability run ledger (Parquet directory/file, or the JSON lines that
  ``python -m lakehouse.observability runs`` prints);
* ``--alerts``: ``alerts.jsonl`` from ``python -m lakehouse.observability check``;
* ``--quality-history``: Great Expectations ``<layer>-result.json`` files (or directories of them)
  from ``python -m lakehouse.quality run``, one per run.

An input that is not given becomes ``None`` in the payload and the page prints "not supplied" with
the command that would produce it (``COMMANDS``). The section builders are pure functions of their
parsed inputs so tests can compare the payload with the raw files.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
#: view ids the pipeline tab declares; ``app.js`` must render each (checked by the tests)
PIPELINE_VIEWS = ("lineage", "timeline", "trend")
#: layered DAG columns, left to right
STAGES = ("raw", "bronze", "silver", "gold", "marts", "dashboard")
STAGE_LABELS = {
    "raw": "Raw",
    "bronze": "Bronze",
    "silver": "Silver",
    "gold": "Gold",
    "marts": "dbt marts",
    "dashboard": "Dashboard",
    "other": "Other",
}
LEDGER_LAYERS = ("bronze", "silver", "gold", "streaming", "quality", "other")
QUALITY_LAYERS = ("bronze", "silver", "gold")
#: the command that produces each missing input (shown on the page)
COMMANDS = {
    "lineage_events": (
        "make -f mk/lineage.mk lineage-emit lineage-quality   # then: --lineage-events data/lineage/events.jsonl"  # noqa: E501
    ),
    "column_lineage": "--column-lineage lineage/column_lineage.yaml   # checked in; verify: make -f mk/lineage.mk lineage-check",  # noqa: E501
    "runs": (
        "python -m lakehouse.observability wrap --layer silver --job silver_batch -- <cmd>   "
        "# then: --runs data/ops/pipeline_runs"
    ),
    "alerts": "python -m lakehouse.observability check   # writes data/ops/alerts.jsonl; then: --alerts data/ops/alerts.jsonl",  # noqa: E501
    "quality_history": (
        "python -m lakehouse.quality run --layer <bronze|silver|gold> --root <layer root> --out data/dq/<run>"  # noqa: E501
        "   # then: --quality-history data/dq"
    ),
    "sample": "python -m dashboard.sample --out data/dashboard-sample   # real local CLI runs on the synthetic fixture",  # noqa: E501
}


# ----------------------------------------------------------------------------- readers
def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.UTC)
        return v.astimezone(dt.UTC).isoformat()
    return str(v)


def read_jsonl(path: Path) -> list[dict]:
    out = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except ValueError as e:
            raise SystemExit(f"error: {path}:{n} is not JSON: {e}") from e
    return out


def load_ledger(path: Path | None) -> list[dict] | None:
    """Run-ledger rows (dicts, timestamps as ISO strings) from a Parquet dir/file or JSON lines."""
    if path is None:
        return None
    if not path.exists():
        raise SystemExit(f"error: --runs {path} does not exist")
    if path.is_file() and path.suffix in (".jsonl", ".json"):
        rows = read_jsonl(path)
    else:
        import pyarrow.parquet as pq

        files = [path] if path.is_file() else sorted(path.rglob("*.parquet"))
        rows = []
        for f in files:
            rows += pq.read_table(f).to_pylist()
    for r in rows:
        for k in ("started_at", "ended_at"):
            r[k] = _iso(r.get(k))
        for need in ("run_id", "layer", "job", "status", "started_at", "ended_at"):
            if r.get(need) is None:
                raise SystemExit(f"error: ledger row without {need}: {r}")
    return rows


def load_events(path: Path | None) -> list[dict] | None:
    if path is None:
        return None
    if not path.is_file():
        raise SystemExit(f"error: --lineage-events {path} is not a file")
    events = read_jsonl(path)
    for e in events:
        if not {"eventType", "run", "job"} <= set(e):
            raise SystemExit(f"error: {path}: not an OpenLineage run event: {list(e)[:5]}")
    return events


def load_alerts(path: Path | None) -> list[dict] | None:
    if path is None:
        return None
    if not path.is_file():
        raise SystemExit(f"error: --alerts {path} is not a file")
    return read_jsonl(path)


def load_column_map(path: Path | None) -> dict | None:
    if path is None:
        return None
    import yaml

    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or not isinstance(doc.get("tables"), dict):
        raise SystemExit(f"error: --column-lineage {path}: no `tables:` mapping")
    return doc


def find_results(paths: list[Path] | None) -> list[Path] | None:
    if not paths:
        return None
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            files += sorted(p.rglob("*-result.json"))
        elif p.is_file():
            files.append(p)
        else:
            raise SystemExit(f"error: --quality-history {p} does not exist")
    if not files:
        raise SystemExit("error: --quality-history matched no *-result.json file")
    return files


# ----------------------------------------------------------------------------- lineage
def dataset_ref(namespace: str, name: str) -> tuple[str, str]:
    """(stage, table) of an OpenLineage dataset.

    The stage is the right-most path segment of ``name`` (or token of the namespace, e.g. the
    bucket ``claims-silver``) that is a layer word; ``landing`` counts as ``raw``. The table is the
    last path segment. Anything else is ``other``.
    """
    parts = [p for p in re.split(r"[/\\]+", name) if p]
    table = parts[-1] if parts else name
    ns_tokens = [t.lower() for t in re.split(r"[^A-Za-z0-9]+", namespace) if t]
    for tok in [*(p.lower() for p in reversed(parts[:-1])), *reversed(ns_tokens)]:
        if tok in ("raw", "landing", "bronze", "silver", "gold"):
            return ("raw" if tok == "landing" else tok), table
    return "other", table


def node_id(stage: str, table: str) -> str:
    return f"{stage}.{table}"


def final_events(events: list[dict]) -> list[dict]:
    """One event per run: COMPLETE/FAIL/ABORT when present, else the START."""
    best: dict[str, dict] = {}
    for e in events:
        rid = e["run"]["runId"]
        cur = best.get(rid)
        if cur is None or (cur["eventType"] == "START" and e["eventType"] != "START"):
            best[rid] = e
    return sorted(best.values(), key=lambda e: (e.get("eventTime", ""), e["run"]["runId"]))


def _status(event_type: str) -> str:
    return {"COMPLETE": "success", "FAIL": "failed", "ABORT": "failed", "START": "incomplete"}.get(
        event_type, "incomplete"
    )


def static_edges(root: Path = ROOT) -> dict[str, list[dict]]:
    """Edges that are declared in repo files rather than observed in events.

    * dbt: each mart's transitive ``ref()`` chain down to ``stg_gold__<table>`` -> gold table;
    * dashboard: the ``marts.mart_*`` tables each query file reads.
    """
    models: dict[str, set[str]] = {}
    for p in (root / "dbt" / "models").rglob("*.sql"):
        text = p.read_text(encoding="utf-8")
        models[p.stem] = set(re.findall(r"ref\(\s*'([\w]+)'\s*\)", text)) | {
            f"source:{t}" for t in re.findall(r"source\(\s*'gold'\s*,\s*'(\w+)'\s*\)", text)
        }

    def gold_inputs(m: str, seen: frozenset[str] = frozenset()) -> set[str]:
        out: set[str] = set()
        for d in models.get(m, ()):
            if d.startswith("source:"):
                out.add(d[7:])
            elif d not in seen:
                out |= gold_inputs(d, seen | {m})
        return out

    dbt = [
        {"src": node_id("gold", t), "dst": node_id("marts", m), "job": "dbt run"}
        for m in sorted(models)
        if m.startswith("mart_")
        for t in sorted(gold_inputs(m))
    ]
    uses: set[tuple[str, str]] = set()
    for qdir in (root / "analytics" / "queries", root / "dashboard" / "queries"):
        for p in sorted(qdir.glob("*.sql")):
            for mart in re.findall(r"marts\.(mart_\w+)", p.read_text(encoding="utf-8")):
                uses.add((mart, p.stem))
    dash = [
        {"src": node_id("marts", m), "dst": node_id("dashboard", "dashboard"), "job": "queries"}
        for m in sorted({m for m, _ in uses})
    ]
    return {"dbt": dbt, "queries": dash}


def lineage_section(
    events: list[dict] | None, colmap: dict | None, label: str | None = None
) -> dict | None:
    """Table-level graph from events (observed) + declared edges; column map for the drill-down."""
    if events is None and colmap is None:
        return None
    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    runs: list[dict] = []

    def add_node(stage: str, table: str, source: str) -> str:
        nid = node_id(stage, table)
        n = nodes.setdefault(
            nid, {"id": nid, "stage": stage, "table": table, "source": source, "quality": None}
        )
        if source == "events":
            n["source"] = "events"
        return nid

    if events is not None:
        seen: set[tuple[str, str, str]] = set()
        for e in final_events(events):
            ins = [dataset_ref(i["namespace"], i["name"]) for i in e.get("inputs", [])]
            outs = [dataset_ref(o["namespace"], o["name"]) for o in e.get("outputs", [])]
            status = _status(e["eventType"])
            job = e["job"]["name"]
            runs.append(
                {
                    "run_id": e["run"]["runId"],
                    "job": job,
                    "status": status,
                    "event_time": e.get("eventTime"),
                    "inputs": len(ins),
                    "outputs": len(outs),
                }
            )
            for st, t in (*ins, *outs):
                add_node(st, t, "events")
            for si, ti in ins:
                for so, to in outs:
                    key = (node_id(si, ti), node_id(so, to), job)
                    if key not in seen:
                        seen.add(key)
                        edges.append(
                            {"src": key[0], "dst": key[1], "job": job, "kind": "observed",
                             "status": status}
                        )  # fmt: skip
            if job.startswith("quality.") and ins:  # a check run annotates its input tables
                for st, t in ins:
                    nodes[node_id(st, t)]["quality"] = {"job": job, "status": status}
    declared_cols: dict | None = None
    if colmap is not None:
        declared_cols = {}
        for gt, cols in colmap["tables"].items():
            add_node("gold", gt, "declared")
            src_tables: set[str] = set()
            out_cols = []
            for cname, spec in cols.items():
                spec = spec or {}
                srcs = [dict(zip(("layer", "table", "column"), s.split(".", 2), strict=True))
                        for s in spec.get("sources", [])]  # fmt: skip
                ind = [dict(zip(("layer", "table", "column"), s.split(".", 2), strict=True))
                       for s in spec.get("indirect", [])]  # fmt: skip
                for s in srcs + ind:
                    src_tables.add(s["table"])
                out_cols.append(
                    {
                        "name": cname,
                        "sources": srcs,
                        "indirect": ind,
                        "transformation": spec.get("transformation", "IDENTITY") if srcs else None,
                        "masking": bool(spec.get("masking")),
                        "derived": spec.get("derived"),
                        "description": spec.get("description"),
                    }
                )
            declared_cols[gt] = out_cols
            for st in sorted(src_tables):
                add_node("silver", st, "declared")
                edges.append(
                    {"src": node_id("silver", st), "dst": node_id("gold", gt),
                     "job": "gold (column map)", "kind": "declared", "status": None}
                )  # fmt: skip
    present = {n["stage"] for n in nodes.values()}
    if "gold" in present:  # marts and the dashboard hang off gold, from repo files
        for kind, lst in static_edges().items():
            for ed in lst:
                a, b = ed["src"], ed["dst"]
                if kind == "dbt" and a not in nodes:
                    continue  # a gold table this graph never mentions
                for nid in (a, b):
                    if nid not in nodes:
                        stage, table = nid.split(".", 1)
                        add_node(stage, table, "declared")
                edges.append({**ed, "kind": "declared", "status": None})
    stage_rank = {s: i for i, s in enumerate(STAGES)}
    node_list = sorted(nodes.values(), key=lambda n: (stage_rank.get(n["stage"], 99), n["table"]))
    return {
        "stages": [s for s in (*STAGES, "other") if s in {n["stage"] for n in node_list}],
        "stage_labels": STAGE_LABELS,
        "nodes": node_list,
        "edges": edges,
        "runs": runs,
        "events_supplied": events is not None,
        "event_count": len(events) if events is not None else 0,
        "columns": declared_cols,
        "label": label,
    }


# ----------------------------------------------------------------------------- timeline
def timeline_section(
    ledger: list[dict] | None, alerts: list[dict] | None, label: str | None = None
) -> dict | None:
    if ledger is None:
        return None
    rows = sorted(ledger, key=lambda r: (r["started_at"], r["run_id"]))
    if not rows:
        return {"runs": [], "layers": [], "t0": None, "span_s": 0, "alerts": alerts, "label": label}
    t0 = min(dt.datetime.fromisoformat(r["started_at"]) for r in rows)
    span = max((dt.datetime.fromisoformat(r["ended_at"]) - t0).total_seconds() for r in rows)
    attempts: dict[tuple, int] = {}
    runs = []
    for r in rows:
        key = (r["job"], r["table_name"], r.get("batch_id"))
        retry = attempts.get(key, 0)
        attempts[key] = retry + 1
        runs.append(
            {
                "run_id": r["run_id"],
                "layer": r["layer"],
                "job": r["job"],
                "table": r["table_name"],
                "batch_id": r.get("batch_id"),
                "status": r["status"],
                "started_at": r["started_at"],
                "ended_at": r["ended_at"],
                "offset_s": (dt.datetime.fromisoformat(r["started_at"]) - t0).total_seconds(),
                "duration_s": r["duration_s"],
                "rows_in": r.get("rows_in"),
                "rows_out": r.get("rows_out"),
                "rows_quarantined": r.get("rows_quarantined"),
                "retries": retry,
                "error": r.get("error"),
                "alerts": [],
            }
        )
    if alerts is not None:
        for a in alerts:
            hit = [x for x in runs if a.get("run_id") and x["run_id"] == a["run_id"]]
            scope = "run"
            if not hit:  # table-level alert: pin it to the table's latest run
                same = [x for x in runs if x["table"] == a.get("table")]
                hit, scope = same[-1:], "table"
            for x in hit:
                x["alerts"].append(
                    {k: a.get(k) for k in ("alert_id", "severity", "rule", "message", "table")}
                    | {"scope": scope}
                )
    return {
        "runs": runs,
        "layers": [layer for layer in LEDGER_LAYERS if any(x["layer"] == layer for x in runs)],
        "t0": _iso(t0),
        "span_s": span,
        "alerts": alerts,
        "label": label,
    }


# ----------------------------------------------------------------------------- quality trend
def _result_row(path: Path, doc: dict) -> dict:
    tables = doc.get("tables")
    if not isinstance(tables, dict) or "layer" not in doc or "generated_at" not in doc:
        raise SystemExit(f"error: {path} is not a lakehouse.quality result (layer/tables missing)")
    passed = failed_blocking = failed_warn = missing = 0
    per_table = []
    for name, t in tables.items():
        exps = t.get("expectations") or []
        p = sum(1 for e in exps if e.get("success"))
        fb = sum(1 for e in exps if not e.get("success") and e.get("severity") == "blocking")
        fw = sum(1 for e in exps if not e.get("success") and e.get("severity") != "blocking")
        is_missing = t.get("status") == "missing"
        passed, failed_blocking, failed_warn = passed + p, failed_blocking + fb, failed_warn + fw
        missing += is_missing
        per_table.append(
            {"table": name, "evaluated": len(exps), "passed": p, "failed": len(exps) - p,
             "missing": is_missing}
        )  # fmt: skip
    return {
        "layer": doc["layer"],
        "generated_at": doc["generated_at"],
        "label": path.parent.name,
        "file": path.name,
        "evaluated": passed + failed_blocking + failed_warn,
        "passed": passed,
        "failed_blocking": failed_blocking,
        "failed_warn": failed_warn,
        "tables_missing": missing,
        "tables": per_table,
        "success": bool(doc.get("success")),
    }


def trend_section(files: list[Path] | None, label: str | None = None) -> dict | None:
    if files is None:
        return None
    rows = [_result_row(f, json.loads(f.read_text(encoding="utf-8"))) for f in files]
    layers = {}
    for layer in QUALITY_LAYERS:
        mine = sorted((r for r in rows if r["layer"] == layer), key=lambda r: r["generated_at"])
        layers[layer] = mine
    extra = sorted({r["layer"] for r in rows} - set(QUALITY_LAYERS))
    if extra:
        raise SystemExit(f"error: unknown quality layer(s) in --quality-history: {extra}")
    return {"layers": layers, "order": list(QUALITY_LAYERS), "label": label}


# ----------------------------------------------------------------------------- assemble
def pipeline_section(
    *,
    events: Path | None = None,
    column_lineage: Path | None = None,
    runs: Path | None = None,
    alerts: Path | None = None,
    quality_history: list[Path] | None = None,
    label: str | None = None,
) -> dict:
    return {
        "views": list(PIPELINE_VIEWS),
        "label": label,
        "commands": COMMANDS,
        "lineage": lineage_section(load_events(events), load_column_map(column_lineage), label),
        "timeline": timeline_section(load_ledger(runs), load_alerts(alerts), label),
        "trend": trend_section(find_results(quality_history), label),
    }
