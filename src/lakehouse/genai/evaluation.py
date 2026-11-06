"""Deterministic evaluation harness: golden set + prompt-injection set, scored with FakeLLM.

What this measures: that scripted model outputs flow through the real guardrails + sandbox and
produce result sets equal to independently written reference SQL, and that hostile outputs are
blocked with the expected rule. What it does NOT measure: how accurate or how jailbreak-resistant
any real LLM (including a Bedrock-hosted one) is. No model is called.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

import duckdb
import yaml

from .catalog import build_catalog
from .config import GenAIConfig
from .executor import Executor
from .llm import FakeLLM
from .service import ANSWERED, REFUSED, REJECTED, GovernedQA

PKG = Path(__file__).parent
NOT_MEASURED = (
    "Real-model accuracy was NOT measured: every model output in this evaluation is scripted by "
    "a human (FakeLLM). The score checks the pipeline and guardrails, not an LLM."
)


def load_items(name: str) -> list[dict]:
    return yaml.safe_load((PKG / name).read_text())["items"]


def scripted_llm() -> FakeLLM:
    """FakeLLM scripted with every golden and injection question (also the CLI local fallback)."""
    items = load_items("golden_set.yaml") + load_items("injection_set.yaml")
    return FakeLLM({i["question"]: i["model_output"] for i in items})


def reference_connection(gold_dir: Path) -> duckdb.DuckDBPyConnection:
    """UNRESTRICTED view of gold, for reference answers only. Never used by the service."""
    con = duckdb.connect()
    for d in sorted(Path(gold_dir).iterdir()):
        if d.is_dir() and list(d.glob("*.parquet")):
            con.execute(f"CREATE VIEW {d.name} AS SELECT * FROM read_parquet('{d}/*.parquet')")
    return con


def _norm(v):
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float):
        return round(v, 4)
    return v


def normalise(rows: list[tuple], ordered: bool) -> list[tuple]:
    out = [tuple(_norm(v) for v in r) for r in rows]
    return out if ordered else sorted(out, key=repr)


@dataclass
class ItemResult:
    id: str
    passed: bool
    detail: str


@dataclass
class EvalReport:
    name: str
    results: list[ItemResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def passed(self) -> int:
        return sum(r.passed for r in self.results)

    @property
    def score(self) -> float:
        return self.passed / self.total if self.total else 0.0

    def format(self) -> str:
        lines = [f"{self.name}: {self.passed}/{self.total} ({self.score:.0%})"]
        lines += [f"  FAIL {r.id}: {r.detail}" for r in self.results if not r.passed]
        return "\n".join(lines)


def run_golden(qa: GovernedQA, ref: duckdb.DuckDBPyConnection) -> EvalReport:
    report = EvalReport("golden set (scripted FakeLLM)")
    for item in load_items("golden_set.yaml"):
        ans = qa.ask(item["question"])
        if ans.status != ANSWERED:
            report.results.append(
                ItemResult(item["id"], False, f"{ans.status} at {ans.stage}: {ans.reasons}")
            )
            continue
        ordered = bool(item.get("ordered"))
        expected = normalise(ref.execute(item["reference_sql"]).fetchall(), ordered)
        actual = normalise(ans.rows, ordered)
        ok = expected == actual
        detail = "ok" if ok else f"result mismatch: expected {expected[:3]}..., got {actual[:3]}..."
        report.results.append(ItemResult(item["id"], ok, detail))
    return report


def run_injection(qa: GovernedQA) -> EvalReport:
    report = EvalReport(f"injection set (screen={'on' if qa.cfg.screen_questions else 'off'})")
    for item in load_items("injection_set.yaml"):
        ans = qa.ask(item["question"])
        expect = item["expect"]
        if ans.status == ANSWERED:
            ok, detail = False, f"query was ANSWERED: {ans.sql}"
        elif expect == "refused":
            ok = ans.status == REFUSED or (ans.status == REJECTED and ans.stage == "screen")
            detail = f"{ans.status}/{ans.stage}"
        elif expect == "rejected_or_refused":
            ok, detail = ans.status in (REJECTED, REFUSED), f"{ans.status}/{ans.stage}"
        else:
            ok = ans.status == REJECTED
            detail = f"{ans.status}/{ans.stage}"
            if ok and ans.stage == "guardrails":
                got = set(ans.decision.rules)
                if not got & set(item.get("expect_rules", [])):
                    ok, detail = False, f"rules {sorted(got)} lack any of {item['expect_rules']}"
        report.results.append(ItemResult(item["id"], ok, detail))
    return report


def evaluate(gold_dir: Path, role: str = "analyst", audit=None) -> list[EvalReport]:
    """Run golden + injection (screen on and off) against a fresh sandbox."""
    catalog = build_catalog(role)
    executor = Executor(catalog, gold_dir)
    ref = reference_connection(gold_dir)
    reports = []
    try:
        for screen in (True, False):
            cfg = GenAIConfig(role=role, screen_questions=screen, session_max_queries=10_000)
            qa = GovernedQA(scripted_llm(), executor, catalog, cfg, audit, user="eval")
            if screen:
                reports.append(run_golden(qa, ref))
            reports.append(run_injection(qa))
    finally:
        executor.close()
        ref.close()
    return reports
