"""Defence-in-depth redaction of identifier-shaped strings.

The primary control is that PII columns do not exist in the sandbox. This catches the residual
case: an identifier-shaped value that arrives inside an allowed text column (e.g. a free-text
description) or inside the user's own question before it is written to the audit log.
"""

from __future__ import annotations

import re

REDACTED = "[REDACTED]"
_PATTERNS = (
    re.compile(r"\bM\d{9}\b"),  # synthetic member id
    re.compile(r"\bPRV\d{7}\b"),  # synthetic provider id
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),  # US SSN shape
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),  # email
    re.compile(r"(?<!\d)(?:\+?\d{1,2}[ -.])?\(?\d{3}\)?[ -.]\d{3}[ -.]\d{4}(?!\d)"),  # phone
)


def scrub(text: str) -> str:
    for p in _PATTERNS:
        text = p.sub(REDACTED, text)
    return text


def mask_rows(rows: list[tuple]) -> tuple[list[tuple], int]:
    """Redact string cells; returns (rows, number_of_cells_changed)."""
    changed = 0
    out = []
    for row in rows:
        new = []
        for v in row:
            if isinstance(v, str):
                s = scrub(v)
                changed += s != v
                v = s
            new.append(v)
        out.append(tuple(new))
    return out, changed
