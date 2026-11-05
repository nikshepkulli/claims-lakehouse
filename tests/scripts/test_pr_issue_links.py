import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "pr_issue_links", Path(__file__).resolve().parents[2] / "scripts" / "pr_issue_links.py"
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_closes_and_part_of_are_recognised_in_order():
    body = "What.\n\nCloses #12\nPart of #7 and refs #12 again"
    assert mod.linked_issues(body) == [12, 7]


def test_keywords_are_case_insensitive_and_variants_work():
    assert mod.linked_issues("FIXES #3, resolved #4, Closed #5") == [3, 4, 5]


def test_bare_hash_or_url_is_not_a_link():
    assert mod.linked_issues("see #9 and https://x/issues/10") == []
    assert mod.linked_issues("") == []


def test_main_fails_without_link(monkeypatch, capsys):
    monkeypatch.setenv("PR_NUMBER", "1")
    monkeypatch.setenv("PR_BODY", "no ticket here")
    assert mod.main() == 1
    assert "must reference a ticket" in capsys.readouterr().out


def test_publish_is_idempotent(monkeypatch):
    monkeypatch.setenv("GH_REPO", "o/r")
    calls = []

    def fake_gh(*args):
        calls.append(args)
        return mod.marker(5) if args[0] == "api" else ""

    monkeypatch.setattr(mod, "_gh", fake_gh)
    assert mod.publish(5, "t", 9) is False
    assert all(c[0] == "api" for c in calls)  # no comment created
