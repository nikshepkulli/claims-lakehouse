"""Generalisation of quasi-identifiers: birth year, age and dates."""

from __future__ import annotations

from datetime import date

AGE_TOP_CODE = 90  # HIPAA Safe Harbor aggregates ages over 89


def bucket_start(value: int | None, width: int = 5) -> int | None:
    """Start of the ``width``-wide bucket containing value (1987 -> 1985). Matches the
    Snowflake policy ``val - MOD(val, 5)`` and the DuckDB analyst view."""
    if value is None:
        return None
    return value - (value % width)


def birth_year_band(year: int | None, width: int = 5) -> str | None:
    """Human label for a birth-year bucket: 1987 -> '1985-1989'."""
    start = bucket_start(year, width)
    return None if start is None else f"{start}-{start + width - 1}"


def age_band(age: int | None, width: int = 5, top: int = AGE_TOP_CODE) -> str | None:
    """Age band label, top-coded: 37 -> '35-39', 93 -> '90+'."""
    if age is None:
        return None
    if age >= top:
        return f"{top}+"
    start = bucket_start(age, width)
    return f"{start}-{start + width - 1}"


def year_of(d: date | None) -> date | None:
    """Generalise a date to January 1 of its year (matches DATE_TRUNC('YEAR', d))."""
    return None if d is None else date(d.year, 1, 1)
