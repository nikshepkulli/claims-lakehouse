"""Surrogate keys and identifier derivation for gold. EVERYTHING that mints a key lives here.

Scheme (documented in docs/gold.md, "Key scheme")::

    sk = sha256_hex( "<kind>:v1" | part1 | part2 | ... )        64 hex chars, VARCHAR(64)

* Dimensions with history (SCD2): ``sk = derive_sk(kind, natural_id, effective_from)``. The
  SK is a pure function of the natural key and the version's start, so a re-run, a replay on
  another machine or a Snowflake reload re-derives the same keys, and a fact row never needs a
  lookup table to find its dimension row.
* Type-1 dimensions: ``sk = derive_sk("diagnosis", icd10_code)``.
* Facts keep their natural business keys (``claim_id``, ``line_id``) as primary keys.

Ticket #71 will move identifier tokenisation to a keyed HMAC (key held outside the data). That
change must be a one-function edit: ``derive_sk`` (digest) and ``natural_id`` (how a source id
appears in gold) are the only places gold derives keys or identifiers. Do not call
``F.sha2`` / ``F.md5`` for keys anywhere else.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

SCHEME_VERSION = "v1"
KINDS = ("member", "provider", "diagnosis")


def derive_sk(kind: str, *parts: Column) -> Column:
    """Deterministic surrogate key: sha256 of the kind tag and the parts, ``|`` separated.

    ``parts`` are cast to string (timestamps render as ``yyyy-MM-dd HH:mm:ss``), so the Python
    mirror in ``tests/gold`` can reproduce a key with ``hashlib``.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown key kind {kind!r}; expected one of {KINDS}")
    if not parts:
        raise ValueError("derive_sk needs at least one part")
    tag = F.lit(f"{kind}:{SCHEME_VERSION}")
    return F.sha2(F.concat_ws("|", tag, *[p.cast("string") for p in parts]), 256)


def natural_id(col: Column) -> Column:
    """How a silver business identifier is carried into gold. Today: unchanged.

    The synthetic ids (``M000000001``, ``PRV0000001``) are already fabricated. When #71 lands
    this is where a keyed pseudonym replaces them, in every dimension and fact at once.
    """
    return col
