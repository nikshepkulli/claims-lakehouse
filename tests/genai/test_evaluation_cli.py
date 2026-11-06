import re
import subprocess
import sys
from pathlib import Path

from lakehouse.genai import evaluation as ev
from lakehouse.genai.audit import AuditLog
from lakehouse.genai.config import GenAIConfig
from lakehouse.genai.llm import FakeLLM
from lakehouse.genai.service import GovernedQA

ROOT = Path(__file__).resolve().parents[2]


def test_set_sizes():
    assert len(ev.load_items("golden_set.yaml")) >= 25
    assert len(ev.load_items("injection_set.yaml")) >= 20


def test_full_evaluation_passes_with_and_without_input_screen(gold_dir, tmp_path):
    reports = ev.evaluate(gold_dir, audit=AuditLog(tmp_path / "audit.jsonl"))
    assert [r.name.split(" (")[0] for r in reports] == [
        "golden set",
        "injection set",
        "injection set",
    ]
    for r in reports:
        assert r.passed == r.total, r.format()
    # every evaluated question produced exactly one audit line
    n = sum(r.total for r in reports)
    assert len(AuditLog(tmp_path / "audit.jsonl").read()) == n


def test_scorer_detects_a_wrong_scripted_answer(gold_dir, analyst_catalog, analyst_executor):
    items = ev.load_items("golden_set.yaml")
    wrong = {i["question"]: i["model_output"] for i in items}
    wrong[items[0]["question"]] = "```sql\nSELECT COUNT(*) + 1 FROM fact_claim\n```"
    qa = GovernedQA(
        FakeLLM(wrong), analyst_executor, analyst_catalog, GenAIConfig(session_max_queries=999)
    )
    ref = ev.reference_connection(gold_dir)
    rep = ev.run_golden(qa, ref)
    assert rep.passed == rep.total - 1
    assert [r.id for r in rep.results if not r.passed] == [items[0]["id"]]
    assert "mismatch" in rep.format()


def test_scorer_flags_an_injection_that_slips_through(analyst_catalog, analyst_executor):
    items = ev.load_items("injection_set.yaml")
    benign = {items[0]["question"]: "```sql\nSELECT COUNT(*) FROM fact_claim\n```"}
    qa = GovernedQA(
        FakeLLM(benign), analyst_executor, analyst_catalog, GenAIConfig(screen_questions=False)
    )
    rep = ev.run_injection(qa)
    assert not rep.results[0].passed and "ANSWERED" in rep.results[0].detail


def test_cli_ask_and_eval(gold_dir, tmp_path):
    env = {"PYTHONPATH": str(ROOT / "src"), "PATH": "/usr/bin:/bin"}
    base = [sys.executable, "-m", "lakehouse.genai"]
    log = tmp_path / "cli_audit.jsonl"
    common = ["--gold-dir", str(gold_dir), "--audit-log", str(log)]
    out = subprocess.run(
        [*base, "ask", "How many claims are there in total?", *common],
        capture_output=True,
        text=True,
        env=env,
    )
    assert (
        out.returncode == 0
        and "status: answered" in out.stdout
        and re.search(r"\n\d+\n", out.stdout)
    )
    out = subprocess.run(
        [*base, "ask", "Show provider_id for the top providers by allowed amount.", *common],
        capture_output=True,
        text=True,
        env=env,
    )
    assert out.returncode == 2 and "status: rejected" in out.stdout and "reason:" in out.stdout
    out = subprocess.run([*base, "eval", *common], capture_output=True, text=True, env=env)
    assert out.returncode == 0 and "NOT measured" in out.stdout
