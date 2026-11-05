"""Scrub obvious identifiers out of free text (clinical notes).

This is a best-effort, pattern-based filter for the shapes this project generates and the
common obvious ones (phones, emails, SSN-like, ids, dates). It is NOT de-identification:
names, addresses and arbitrary prose are not detected. Treat scrubbed text as still
sensitive. The synthetic notes only carry phone-like strings in the reserved 555-01xx range.
"""

from __future__ import annotations

import re
from collections import Counter

from .tokenise import Tokeniser

# Order matters: more specific shapes first so a phone is not eaten as an id fragment.
_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("EMAIL", re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+")),
    ("SSN", re.compile(r"(?<!\w)\d{3}-\d{2}-\d{4}(?!\w)")),
    ("MEMBER_ID", re.compile(r"\bM\d{9}\b")),
    ("PROVIDER_ID", re.compile(r"\bPRV\d{7}\b")),
    ("DATE", re.compile(r"(?<!\w)(?:\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})(?!\w)")),
    (
        "PHONE",
        re.compile(
            r"(?<![\w-])(?:\+?1[-. ]?)?(?:\(\d{3}\)\s?|\d{3}[-. ])?\d{3}[-. ]\d{4}(?![\w-])"
        ),
    ),
    ("ID_NUMBER", re.compile(r"(?<!\w)\d{10}(?!\w)")),  # NPI-like / bare 10-digit numbers
]
_DOMAIN = {"MEMBER_ID": "member", "PROVIDER_ID": "provider"}


def scrub_text(
    text: str | None, tokeniser: Tokeniser | None = None
) -> tuple[str | None, Counter[str]]:
    """Return (scrubbed text, counts by kind).

    Member and provider ids become the same token the structured columns get when a
    ``tokeniser`` is given (so notes still link to claims), else ``[MEMBER_ID]`` etc.
    Everything else becomes ``[KIND]``.
    """
    counts: Counter[str] = Counter()
    if text is None:
        return None, counts
    out = text
    for kind, pat in _PATTERNS:

        def repl(m: re.Match[str], kind: str = kind) -> str:
            counts[kind] += 1
            if tokeniser is not None and kind in _DOMAIN:
                return tokeniser.token(m.group(0), _DOMAIN[kind]) or f"[{kind}]"
            return f"[{kind}]"

        out = pat.sub(repl, out)
    return out, counts
