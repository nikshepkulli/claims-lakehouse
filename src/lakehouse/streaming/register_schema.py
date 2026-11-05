"""Register the claim-event contract and enforce BACKWARD compatibility.

    python -m lakehouse.streaming.register_schema                 # set BACKWARD, check, register
    python -m lakehouse.streaming.register_schema --check-only    # compatibility check, no write
    python -m lakehouse.streaming.register_schema --schema my.json --check-only

Exit codes: 0 ok, 1 incompatible with the registered schema, 2 usage/IO/registry error.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import EVENTS_TOPIC, contract_path, registry_url, subject_for
from .registry import RegistryClient, RegistryError


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--schema", type=Path, default=contract_path(), help="contract file")
    p.add_argument("--topic", default=EVENTS_TOPIC, help="subject is <topic>-value")
    p.add_argument("--registry-url", default=registry_url())
    p.add_argument("--check-only", action="store_true", help="check compatibility, do not register")
    args = p.parse_args(argv)

    try:
        schema = json.loads(args.schema.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"cannot read schema {args.schema}: {exc}", file=sys.stderr)
        return 2
    subject = subject_for(args.topic)
    client = RegistryClient(args.registry_url)
    try:
        if not args.check_only:
            client.set_compatibility(subject, "BACKWARD")
        if not client.is_compatible(subject, schema):
            print(f"INCOMPATIBLE: {args.schema} breaks BACKWARD for {subject}", file=sys.stderr)
            return 1
        if args.check_only:
            print(f"compatible: {args.schema} -> {subject}")
            return 0
        schema_id = client.register(subject, schema)
    except (RegistryError, OSError) as exc:
        print(f"registry error at {args.registry_url}: {exc}", file=sys.stderr)
        return 2
    print(f"registered subject={subject} id={schema_id} compatibility=BACKWARD")
    return 0


if __name__ == "__main__":
    sys.exit(main())
