"""Deterministic keyed pseudonymisation (HMAC-SHA256) of identifiers, with key versions.

Why HMAC and not a bare hash: member and provider ids here are sequential
(``M000000001``), so an unkeyed SHA-256 can be reversed by enumerating the id space. A
keyed HMAC cannot, as long as the key stays secret. The key comes from the environment
(``LAKEHOUSE_PSEUDONYM_KEY``) or a secret store that populates it, and is never stored in
the repo.

Token format: ``tok_<key_id>_<32 hex>`` (e.g. ``tok_v1_3f9a...``), 39 characters for a
two-character key id. The key id is part of the token, so a dataset that holds tokens of two
key generations is readable: ``KeyRing.verify`` picks the right key from the token itself.

Properties (all covered by tests/governance):
* deterministic: same key + domain + value -> same token, so joins survive tokenisation and
  a re-run of a job produces byte-identical output;
* domain separated: the same raw string in two domains yields unrelated tokens;
* key rotation: a different key yields different tokens for every value, and the key id
  tells old from new. Rotation = new key id, rebuild from the raw source (HMAC is one way,
  tokens cannot be converted), retire the old key into ``LAKEHOUSE_PSEUDONYM_RETIRED_KEYS``
  until no old-id token remains (docs/privacy-and-access.md, "Key rotation runbook");
* nulls stay null.

Spark: ``Tokeniser`` is plain Python and picklable, so wrap ``token`` in a UDF (see
``lakehouse.gold.keys.natural_id``) or call ``tokenise_table`` per Arrow batch. No Spark
dependency lives in this module.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
from collections import Counter
from collections.abc import Iterable, Mapping

import pyarrow as pa

KEY_ENV_VAR = "LAKEHOUSE_PSEUDONYM_KEY"  # the ACTIVE key (secret-store reference in prod)
KEY_ID_ENV_VAR = "LAKEHOUSE_PSEUDONYM_KEY_ID"  # id of the active key, default "v1"
RETIRED_KEYS_ENV_VAR = "LAKEHOUSE_PSEUDONYM_RETIRED_KEYS"  # JSON {"v1": "<key>"}; verify only
DEFAULT_KEY_ID = "v1"
MIN_KEY_LENGTH = 32
TOKEN_PREFIX = "tok_"
TOKEN_HEX_CHARS = 32  # 128 bits: collision-safe at any dataset size here
KEY_ID_PATTERN = re.compile(r"[a-z0-9]{1,8}")
TOKEN_PATTERN = re.compile(rf"tok_([a-z0-9]{{1,8}})_([0-9a-f]{{{TOKEN_HEX_CHARS}}})")
MAX_TOKEN_LENGTH = len(TOKEN_PREFIX) + 8 + 1 + TOKEN_HEX_CHARS  # widest allowed key id


class MissingKeyError(RuntimeError):
    """The pseudonymisation key is absent or too short."""


def _check_key_id(key_id: str) -> str:
    if not KEY_ID_PATTERN.fullmatch(key_id):
        raise ValueError(f"key id {key_id!r} must match [a-z0-9]{{1,8}} (it is embedded in tokens)")
    return key_id


def parse_token(token: str | None) -> tuple[str, str] | None:
    """``(key_id, digest)`` of a well-formed token, else None."""
    m = TOKEN_PATTERN.fullmatch(token) if isinstance(token, str) else None
    return (m.group(1), m.group(2)) if m else None


class Tokeniser:
    """HMAC tokeniser for ONE key generation. Use :class:`KeyRing` when several can coexist."""

    def __init__(self, key: str | bytes, key_id: str = DEFAULT_KEY_ID) -> None:
        kb = key.encode() if isinstance(key, str) else bytes(key)
        if len(kb) < MIN_KEY_LENGTH:
            raise MissingKeyError(f"pseudonymisation key must be at least {MIN_KEY_LENGTH} bytes")
        self._key = kb
        self.key_id = _check_key_id(key_id)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Tokeniser:
        """The active key (``LAKEHOUSE_PSEUDONYM_KEY``) and its id (``..._KEY_ID``, default v1)."""
        e = os.environ if env is None else env
        key = e.get(KEY_ENV_VAR)
        if not key:
            raise MissingKeyError(f"set {KEY_ENV_VAR} (>= {MIN_KEY_LENGTH} chars); never commit it")
        return cls(key, e.get(KEY_ID_ENV_VAR) or DEFAULT_KEY_ID)

    def __repr__(self) -> str:  # never leak key material into logs or tracebacks
        return f"Tokeniser(key_id={self.key_id!r}, key=<redacted>)"

    def token(self, value: str | None, domain: str) -> str | None:
        if value is None:
            return None
        return f"{TOKEN_PREFIX}{self.key_id}_{self._digest(value, domain)}"

    def _digest(self, value: str, domain: str) -> str:
        msg = domain.encode() + b"\x00" + str(value).encode()
        return hmac.new(self._key, msg, hashlib.sha256).hexdigest()[:TOKEN_HEX_CHARS]

    def token_fn(self, domain: str, memo_max: int = 100_000):
        """A SELF-CONTAINED ``value -> token`` function for engines that ship code to workers.

        The closure holds only the key bytes, the key id, the domain and stdlib calls, so
        cloudpickle serialises it by value and a Spark Python worker needs neither this
        package on its path nor this class. Output equals :meth:`token` (tested). Memoises
        per worker. NOTE: the closure carries the key; see lakehouse.gold.keys.
        """
        key, prefix, dom, n = self._key, f"{TOKEN_PREFIX}{self.key_id}_", domain.encode(), memo_max
        memo: dict = {}

        def fn(value):
            if value is None:
                return None
            hit = memo.get(value)
            if hit is None:
                msg = dom + b"\x00" + str(value).encode()
                hit = prefix + hmac.new(key, msg, hashlib.sha256).hexdigest()[:TOKEN_HEX_CHARS]
                if len(memo) < n:
                    memo[value] = hit
            return hit

        return fn

    def verify(self, token: str | None, value: str | None, domain: str) -> bool:
        """True if ``token`` is this key's token for ``value`` (constant-time compare)."""
        parsed = parse_token(token)
        if parsed is None or value is None or parsed[0] != self.key_id:
            return False
        return hmac.compare_digest(parsed[1], self._digest(value, domain))

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


class KeyRing:
    """The active key (all new writes) plus retired keys (verification of old tokens only).

    Rotation never edits tokens in place: new writes carry the new key id, old tokens keep
    verifying against the retired key until the dataset is rebuilt and the old key dropped.
    """

    def __init__(self, active: Tokeniser, retired: Iterable[Tokeniser] = ()) -> None:
        self.active = active
        self._retired: dict[str, Tokeniser] = {}
        for t in retired:
            if t.key_id == active.key_id or t.key_id in self._retired:
                raise ValueError(f"duplicate key id {t.key_id!r} in the key ring")
            self._retired[t.key_id] = t

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> KeyRing:
        e = os.environ if env is None else env
        raw = e.get(RETIRED_KEYS_ENV_VAR)
        retired: list[Tokeniser] = []
        if raw:
            try:
                mapping = json.loads(raw)
                retired = [Tokeniser(k, kid) for kid, k in mapping.items()]
            except (ValueError, AttributeError, TypeError) as exc:
                # Never echo the value: it contains key material.
                raise MissingKeyError(
                    f"{RETIRED_KEYS_ENV_VAR} must be a JSON object {{key_id: key}}"
                ) from exc
        return cls(Tokeniser.from_env(e), retired)

    def __repr__(self) -> str:
        return f"KeyRing(active={self.active.key_id!r}, retired={sorted(self._retired)})"

    @property
    def key_ids(self) -> tuple[str, ...]:
        return (self.active.key_id, *sorted(self._retired))

    def token(self, value: str | None, domain: str) -> str | None:
        return self.active.token(value, domain)

    def verify(self, token: str | None, value: str | None, domain: str) -> bool:
        """Verify a token of ANY key generation held in the ring (False for unknown ids)."""
        parsed = parse_token(token)
        if parsed is None:
            return False
        t = self.active if parsed[0] == self.active.key_id else self._retired.get(parsed[0])
        return t is not None and t.verify(token, value, domain)

    def is_current(self, token: str | None) -> bool:
        parsed = parse_token(token)
        return parsed is not None and parsed[0] == self.active.key_id


def key_id_counts(tokens: Iterable[str | None]) -> Counter[str]:
    """Count tokens per key id (``"?"`` for malformed values, nulls skipped).

    Rotation is finished when only the active id remains: the old key can then be dropped.
    """
    c: Counter[str] = Counter()
    for t in tokens:
        if t is None:
            continue
        p = parse_token(t)
        c[p[0] if p else "?"] += 1
    return c
