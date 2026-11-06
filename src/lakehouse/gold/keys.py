"""Surrogate keys and identifier tokens for gold. EVERYTHING that mints a key lives here.

Two different things are minted, with different sensitivity (ticket #71; the per-column
decisions are recorded in governance/data_classification.yaml):

* **Identifier tokens** (``natural_id``): ``member_id`` / ``provider_id`` in gold are NOT the
  source ids but keyed HMAC-SHA256 tokens, ``tok_<key_id>_<32 hex>``
  (``lakehouse.governance.tokenise``). The ids are sequential (``M000000001``), so an unkeyed
  hash of them could be reversed by enumeration; the HMAC key (``LAKEHOUSE_PSEUDONYM_KEY``)
  is supplied through the environment / secret store and never stored in the repo or the data.
  The key id inside the token makes rotation visible (docs/privacy-and-access.md).
* **Surrogate keys** (``derive_sk``)::

      sk = sha256_hex( "<kind>:v1" | part1 | part2 | ... )        64 hex chars, VARCHAR(64)

  For member/provider the parts include the already-keyed token, so the digest input carries
  128 bits the attacker does not have: it cannot be enumerated back to an id without the key.
  ``diagnosis_sk`` hashes an ICD-10 code, which is public reference data, not an identifier;
  it stays an unkeyed hash on purpose (non-sensitive, documented in the registry).

  SCD2 dimensions: ``sk = derive_sk(kind, natural_id(id), effective_from)``. The SK is a pure
  function of the token and the version's start, so a re-run, a replay on another machine
  or a Snowflake reload re-derives the same keys, and a fact never needs a lookup table.
  Consequence of keying: rotating the pseudonym key changes every member/provider token and
  therefore every SK; the runbook is a full rebuild of gold from silver, not a patch.

* Facts keep their business keys (``claim_id``, ``line_id``) as primary keys; they are
  synthetic opaque ids classified INTERNAL, not person identifiers.

Do not call ``F.sha2`` / ``F.md5`` for keys anywhere else (a test enforces it).

Spark has no native HMAC, so tokens are produced by a Python UDF over the driver-built
``Tokeniser``. The key is therefore pickled into the task closure and reaches executors; on
a shared cluster restrict who can read job definitions/task binaries, or move the lookup to
a secret scope read on the executors. Not exercised on a real cluster here.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from lakehouse.governance.tokenise import Tokeniser

SCHEME_VERSION = "v1"
KINDS = ("member", "provider", "diagnosis")
TOKEN_DOMAINS = ("member", "provider")  # identifier domains; one per kind, reused in every table


def derive_sk(kind: str, *parts: Column) -> Column:
    """Deterministic surrogate key: sha256 of the kind tag and the parts, ``|`` separated.

    For ``member`` / ``provider`` pass the TOKEN (``natural_id(...)``), never the raw id.

    ``parts`` are cast to string (timestamps render as ``yyyy-MM-dd HH:mm:ss``), so the Python
    mirror in ``tests/gold`` can reproduce a key with ``hashlib``.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown key kind {kind!r}; expected one of {KINDS}")
    if not parts:
        raise ValueError("derive_sk needs at least one part")
    tag = F.lit(f"{kind}:{SCHEME_VERSION}")
    return F.sha2(F.concat_ws("|", tag, *[p.cast("string") for p in parts]), 256)


def natural_id(col: Column, domain: str, tokeniser: Tokeniser | None = None) -> Column:
    """How a silver business identifier is carried into gold: a keyed HMAC token.

    ``domain`` is ``member`` or ``provider`` (a member id never links to a provider id).
    ``tokeniser`` defaults to ``Tokeniser.from_env()`` on the driver, so the job fails fast
    with ``MissingKeyError`` when ``LAKEHOUSE_PSEUDONYM_KEY`` is absent. NULL stays NULL.
    Used in every dimension and fact path, so the same id tokenises identically everywhere.
    """
    if domain not in TOKEN_DOMAINS:
        raise ValueError(f"unknown token domain {domain!r}; expected one of {TOKEN_DOMAINS}")
    tk = tokeniser or Tokeniser.from_env()
    return F.udf(tk.token_fn(domain), StringType())(col.cast("string"))
