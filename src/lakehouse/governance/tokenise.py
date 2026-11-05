"""Deterministic keyed pseudonymisation (HMAC-SHA256) of identifiers.

Why HMAC and not a bare hash: member and provider ids here are sequential
(``M000000001``), so an unkeyed SHA-256 can be reversed by enumerating the id space. A
keyed HMAC cannot, as long as the key stays secret. The key comes from the environment
(``LAKEHOUSE_PSEUDONYM_KEY``) and is never stored in the repo.

Properties (all covered by tests/governance):
* deterministic: same key + domain + value -> same token, so joins survive tokenisation;
* domain separated: the same raw string in two domains yields unrelated tokens;
* key rotation: a different key yields different tokens for every value (re-tokenise the
  whole dataset after rotating; tokens from two keys must never be mixed);
* nulls stay null.

Spark: ``Tokeniser`` is plain Python and picklable-by-key, so wrap ``token`` in a pandas
UDF (``pandas_udf("string")(lambda s: s.map(lambda v: tk.token(v, "member")))``) or call
``tokenise_table`` per Arrow batch. No Spark dependency lives in this module.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from collections.abc import Mapping

import pyarrow as pa

KEY_ENV_VAR = "LAKEHOUSE_PSEUDONYM_KEY"
MIN_KEY_LENGTH = 32
TOKEN_PREFIX = "tok_"
TOKEN_HEX_CHARS = 32  # 128 bits: collision-safe at any dataset size here


class MissingKeyError(RuntimeError):
    """The pseudonymisation key is absent or too short."""


class Tokeniser:
    def __init__(self, key: str | bytes) -> None:
        kb = key.encode() if isinstance(key, str) else bytes(key)
        if len(kb) < MIN_KEY_LENGTH:
            raise MissingKeyError(f"pseudonymisation key must be at least {MIN_KEY_LENGTH} bytes")
        self._key = kb

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Tokeniser:
        key = (os.environ if env is None else env).get(KEY_ENV_VAR)
        if not key:
            raise MissingKeyError(f"set {KEY_ENV_VAR} (>= {MIN_KEY_LENGTH} chars); never commit it")
        return cls(key)

    def __repr__(self) -> str:  # never leak key material into logs or tracebacks
        return "Tokeniser(key=<redacted>)"

    def token(self, value: str | None, domain: str) -> str | None:
        if value is None:
            return None
        msg = domain.encode() + b"\x00" + str(value).encode()
        digest = hmac.new(self._key, msg, hashlib.sha256).hexdigest()
        return TOKEN_PREFIX + digest[:TOKEN_HEX_CHARS]

    def tokenise_array(self, arr: pa.Array | pa.ChunkedArray, domain: str) -> pa.Array:
        return pa.array([self.token(v, domain) for v in arr.to_pylist()], type=pa.string())

    def tokenise_table(self, table: pa.Table, columns: Mapping[str, str]) -> pa.Table:
        """Replace ``columns`` ({column: domain}) with tokens, keeping column order.

        Use one domain per identifier kind (``member``, ``provider``) and reuse it in every
        table, so a member_id tokenises identically in claims, members and notes.
        """
        out = table
        for col, domain in columns.items():
            idx = out.schema.get_field_index(col)
            if idx < 0:
                raise KeyError(col)
            out = out.set_column(idx, col, self.tokenise_array(out.column(col), domain))
        return out
