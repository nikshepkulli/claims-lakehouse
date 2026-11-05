"""Consume a topic from the beginning and count / validate messages (debug + smoke test).

    python -m lakehouse.streaming.tail --topic claims.events.v1 --expect 1000

Uses a throwaway consumer group with auto-commit off. Stops after ``--expect`` messages or
``--timeout`` seconds of silence. Exit 0 iff the count matches ``--expect`` (when given).
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid

from .config import EVENTS_TOPIC, bootstrap_servers, client_config
from .contract import validate_message


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--topic", default=EVENTS_TOPIC)
    p.add_argument("--expect", type=int, default=None)
    p.add_argument("--timeout", type=float, default=15.0, help="seconds of silence before stop")
    p.add_argument("--bootstrap", default=bootstrap_servers())
    args = p.parse_args(argv)

    from confluent_kafka import Consumer

    consumer = Consumer(
        client_config(
            args.bootstrap,
            **{
                "group.id": f"tail-{uuid.uuid4().hex[:8]}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            },
        )
    )
    consumer.subscribe([args.topic])
    count = valid = with_schema_header = 0
    keys: set[bytes] = set()
    last = time.monotonic()
    try:
        while time.monotonic() - last < args.timeout:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            last = time.monotonic()
            count += 1
            valid += validate_message(msg.value()).ok
            with_schema_header += any(k == "schema-id" for k, _ in (msg.headers() or []))
            keys.add(msg.key() or b"")
            if args.expect is not None and count >= args.expect:
                break
    finally:
        consumer.close()
    print(
        f"consumed={count} valid={valid} with_schema_id_header={with_schema_header} "
        f"distinct_keys={len(keys)}"
    )
    return 0 if args.expect is None or count == args.expect else 1


if __name__ == "__main__":
    sys.exit(main())
