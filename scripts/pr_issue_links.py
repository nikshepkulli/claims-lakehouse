"""Enforce and publish PR <-> issue cross-links.

Usage (CI): PR_NUMBER, PR_TITLE, PR_BODY and GH_REPO are passed as env vars so the PR text is
never interpolated into a shell command. Exits 1 when the PR body names no issue.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

LINK_RE = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|part of|refs?)\s+#(\d+)", re.IGNORECASE
)


def linked_issues(body: str) -> list[int]:
    """Issue numbers referenced with a closing/reference keyword, in first-seen order."""
    seen: list[int] = []
    for m in LINK_RE.finditer(body or ""):
        n = int(m.group(1))
        if n not in seen:
            seen.append(n)
    return seen


def marker(pr: int) -> str:
    return f"<!-- pr-link:{pr} -->"


def comment_text(pr: int, title: str) -> str:
    return f"{marker(pr)}\nLinked PR: #{pr} — {title}"


def _gh(*args: str) -> str:
    return subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout


def publish(pr: int, title: str, issue: int) -> bool:
    """Comment the PR link on the issue once. Returns True if a comment was created."""
    existing = _gh(
        "api",
        f"repos/{os.environ['GH_REPO']}/issues/{issue}/comments",
        "--paginate",
        "-q",
        ".[].body",
    )
    if marker(pr) in existing:
        return False
    _gh("issue", "comment", str(issue), "--body", comment_text(pr, title))
    return True


def main() -> int:
    pr = int(os.environ["PR_NUMBER"])
    issues = linked_issues(os.environ.get("PR_BODY", ""))
    if not issues:
        print("::error::PR body must reference a ticket, e.g. 'Closes #12' or 'Part of #12'.")
        return 1
    for n in issues:
        try:
            created = publish(pr, os.environ.get("PR_TITLE", ""), n)
            print(f"issue #{n}: {'commented' if created else 'already linked'}")
        except subprocess.CalledProcessError as exc:  # e.g. read-only token on fork PRs
            print(f"::warning::could not comment on #{n}: {exc.stderr.strip()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
