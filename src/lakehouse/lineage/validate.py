"""Validate events against the vendored OpenLineage JSON Schemas (``spec/``, Apache-2.0).

The event is checked against ``RunEvent`` of the core schema, then every facet (run, job, dataset,
input, output) against the schema its own ``_schemaURL`` names. Facets whose URL is not vendored
(our custom ``lakehouse_run``) are checked against ``BaseFacet`` only. Needs ``jsonschema``
(test/dev dependency; ``requirements-lineage.txt``), imported lazily.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

SPEC_DIR = Path(__file__).parent / "spec"
CORE_ID = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
EVENT_TYPES = ("START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER")


class InvalidEvent(ValueError):
    """The event does not satisfy the OpenLineage schema."""


@lru_cache(maxsize=1)
def _registry() -> Any:
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    reg: Any = Registry()
    for f in [SPEC_DIR / "OpenLineage.json", *sorted((SPEC_DIR / "facets").glob("*.json"))]:
        doc = json.loads(f.read_text(encoding="utf-8"))
        reg = reg.with_resource(doc["$id"], Resource.from_contents(doc, DRAFT202012))
    return reg


def _errors(instance: Any, ref: str, where: str) -> list[str]:
    from jsonschema import Draft202012Validator, FormatChecker

    v = Draft202012Validator({"$ref": ref}, registry=_registry(), format_checker=FormatChecker())
    return [
        f"{where}: {e.message} (at {'/'.join(map(str, e.absolute_path)) or '<root>'})"
        for e in v.iter_errors(instance)
    ]


def _facet_ref(facet: dict[str, Any]) -> str:
    url = str(facet.get("_schemaURL", ""))
    base, _, frag = url.partition("#")
    try:
        _registry().get_or_retrieve(base)
    except Exception:
        return f"{CORE_ID}#/$defs/BaseFacet"
    return url if frag else f"{CORE_ID}#/$defs/BaseFacet"


def _facets(event: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []

    def take(prefix: str, group: Any) -> None:
        for k, v in (group or {}).items():
            out.append((f"{prefix}.{k}", v))

    take("run.facets", (event.get("run") or {}).get("facets"))
    take("job.facets", (event.get("job") or {}).get("facets"))
    for side, key in (("inputs", "inputFacets"), ("outputs", "outputFacets")):
        for i, ds in enumerate(event.get(side) or []):
            take(f"{side}[{i}].facets", ds.get("facets"))
            take(f"{side}[{i}].{key}", ds.get(key))
    return out


def errors(event: dict[str, Any]) -> list[str]:
    """All schema violations of one event (empty list = valid)."""
    errs = _errors(event, f"{CORE_ID}#/$defs/RunEvent", "event")
    if event.get("eventType") not in EVENT_TYPES:
        errs.append("event: eventType missing or unknown")
    for where, facet in _facets(event):
        errs += _errors(facet, _facet_ref(facet), where)
    return errs


def check(event: dict[str, Any]) -> None:
    errs = errors(event)
    if errs:
        raise InvalidEvent("; ".join(errs[:5]))
