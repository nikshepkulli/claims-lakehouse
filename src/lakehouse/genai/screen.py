"""Heuristic screen for obvious prompt-injection / destructive-intent questions.

This is a cheap first filter with an explainable reason. It is NOT a security boundary: the SQL
guardrails and the column-less sandbox hold even when every pattern here is bypassed (the
injection test set runs with the screen disabled to prove it).
"""

from __future__ import annotations

import re

_RULES = (
    (
        "override_instructions",
        r"\b(ignore|disregard|forget|override)\b.{0,40}\b(instruction|rule|prompt|polic|guardrail)",
    ),
    (
        "reveal_prompt",
        r"\b(reveal|show|print|repeat|leak)\b.{0,30}\b(system prompt|instructions|your prompt)",
    ),
    ("role_switch", r"\b(you are now|act as|pretend (to be|you)|developer mode|jailbreak)\b"),
    (
        "bypass_controls",
        r"\b(bypass|disable|turn off|skip)\b.{0,30}\b(guardrail|filter|validator|masking|security)",
    ),
    (
        "destructive_sql",
        r"\b(drop|truncate|alter)\s+(table|schema|database)\b|\bdelete\s+from\b|\binsert\s+into\b|\bupdate\s+\w+\s+set\b",
    ),
    ("raw_sql_smuggling", r"(;|--|/\*)\s*(select|drop|delete|update|insert|attach|copy|pragma)\b"),
)
_COMPILED = tuple((name, re.compile(rx, re.I | re.S)) for name, rx in _RULES)


def screen_question(question: str) -> str | None:
    """Reason string if the question looks hostile, else None."""
    for name, rx in _COMPILED:
        if rx.search(question):
            return f"question matches injection/destructive pattern '{name}'"
    return None
