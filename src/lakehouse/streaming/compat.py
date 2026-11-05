"""Offline BACKWARD-compatibility check for JSON Schema contracts.

BACKWARD means: a consumer using the NEW schema can read data written with the OLD one.
This is a conservative subset of what Redpanda's registry enforces (it is the authority; this
check lets unit tests and CI fail a breaking change without a broker). It handles the
constructs our contracts use: type, enum/const, properties, required, additionalProperties,
items, numeric/length bounds, pattern, format. When it cannot prove a change is safe (e.g. a
pattern changed) it reports it as breaking.
"""

from __future__ import annotations

from typing import Any

Schema = dict[str, Any]

# (keyword, direction): 'min' keywords may only be relaxed (lowered); 'max' only raised.
_BOUNDS = {
    "minimum": "min",
    "exclusiveMinimum": "min",
    "minLength": "min",
    "minItems": "min",
    "minProperties": "min",
    "maximum": "max",
    "exclusiveMaximum": "max",
    "maxLength": "max",
    "maxItems": "max",
    "maxProperties": "max",
}


def _types(schema: Schema) -> set[str] | None:
    t = schema.get("type")
    if t is None:
        return None
    return set(t) if isinstance(t, list) else {t}


def _enum(schema: Schema) -> list[Any] | None:
    if "enum" in schema:
        return list(schema["enum"])
    if "const" in schema:
        return [schema["const"]]
    return None


def check_backward(old: Schema, new: Schema, path: str = "$") -> list[str]:
    """Return reasons ``new`` cannot read data written under ``old`` (empty = compatible)."""
    problems: list[str] = []

    old_t, new_t = _types(old), _types(new)
    if new_t is not None:
        if old_t is None:
            problems.append(f"{path}: type constrained to {sorted(new_t)} (was unconstrained)")
        else:
            widened = set(new_t) | ({"integer"} if "number" in new_t else set())
            if not old_t <= widened:
                problems.append(f"{path}: type changed {sorted(old_t)} -> {sorted(new_t)}")

    old_e, new_e = _enum(old), _enum(new)
    if new_e is not None:
        if old_e is None:
            problems.append(f"{path}: enum introduced (was unconstrained)")
        else:
            missing = [v for v in old_e if v not in new_e]
            if missing:
                problems.append(f"{path}: enum values removed {missing}")

    for kw, direction in _BOUNDS.items():
        if kw not in new:
            continue
        if kw not in old:
            problems.append(f"{path}: {kw} added")
        elif (direction == "min" and new[kw] > old[kw]) or (
            direction == "max" and new[kw] < old[kw]
        ):
            problems.append(f"{path}: {kw} tightened {old[kw]} -> {new[kw]}")

    for kw in ("pattern", "format"):
        if kw in new and new[kw] != old.get(kw):
            problems.append(f"{path}: {kw} changed {old.get(kw)!r} -> {new[kw]!r}")

    old_props, new_props = old.get("properties", {}), new.get("properties", {})
    old_req, new_req = set(old.get("required", [])), set(new.get("required", []))
    for name in sorted(new_req - old_req):
        problems.append(f"{path}.{name}: newly required (old data may lack it)")

    new_closed = new.get("additionalProperties", True) is False
    old_closed = old.get("additionalProperties", True) is False
    if new_closed and not old_closed:
        problems.append(f"{path}: additionalProperties set to false (closed content model)")
    if new_closed:
        for name in sorted(set(old_props) - set(new_props)):
            problems.append(f"{path}.{name}: property removed from a closed schema")

    for name, new_sub in new_props.items():
        if name in old_props:
            problems += check_backward(old_props[name], new_sub, f"{path}.{name}")

    if isinstance(old.get("items"), dict) and isinstance(new.get("items"), dict):
        problems += check_backward(old["items"], new["items"], f"{path}[]")
    return problems


def is_backward_compatible(old: Schema, new: Schema) -> bool:
    return not check_backward(old, new)
