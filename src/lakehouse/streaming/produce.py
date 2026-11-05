"""Replay the generator's claim-event file into Redpanda at a configurable rate.

    python -m lakehouse.streaming.produce --events data/raw/events/claim_events.jsonl \
        --rate 200 --topic claims.events.v1 [--max-events 1000]

Each line is sent verbatim (so the generator's injected duplicate / out-of-order events reach
the broker unchanged: dedupe and watermarking are the consumer's job). Key = claim_id, so all
events of one claim land on one partition in order. Lines failing the contract are NOT sent to
the main topic; they go to ``<topic>.dlq`` wrapped in the DLQ envelope. Producer is idempotent
(acks=all, enable.idempotence).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from . import PRODUCER_VERSION, dlq
from .config import (
    EVENTS_TOPIC,
    bootstrap_servers,
    client_config,
    dlq_topic,
    registry_url,
    subject_for,
)
from .contract import validate_message

PRODUCER_CONFIG: dict[str, Any] = {
    "acks": "all",
    "enable.idempotence": True,
    "max.in.flight.requests.per.connection": 5,
    "compression.type": "zstd",
    "linger.ms": 20,
    "batch.num.messages": 1000,
    "client.id": f"claims-replay/{PRODUCER_VERSION}",
}


class ProducerLike(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...
    def poll(self, timeout: float = ...) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...


@dataclass
class ReplayStats:
    sent: int = 0
    dead_lettered: int = 0
    delivered: int = 0
    failed: int = 0
    elapsed_s: float = 0.0

    @property
    def rate(self) -> float:
        return self.sent / self.elapsed_s if self.elapsed_s else 0.0


def read_lines(path: Path) -> Iterator[bytes]:
    """Non-empty lines of a JSONL file as bytes, trailing newline stripped."""
    with path.open("rb") as fh:
        for line in fh:
            line = line.rstrip(b"\r\n")
            if line:
                yield line


def _key_of(raw: bytes, result: Any) -> bytes | None:
    """Message key = claim_id (from the validated event, or best-effort when unvalidated)."""
    try:
        event = result.event if result is not None else json.loads(raw)
        return str(event["claim_id"]).encode()
    except (ValueError, KeyError, TypeError):
        return None


def headers_for(schema_id: int | None) -> list[tuple[str, bytes]]:
    hdrs = [("producer-version", PRODUCER_VERSION.encode())]
    if schema_id is not None:
        hdrs.insert(0, ("schema-id", str(schema_id).encode()))
    return hdrs


def replay(
    lines: Iterable[bytes],
    producer: ProducerLike,
    *,
    topic: str,
    rate: float = 0.0,
    max_events: int | None = None,
    schema_id: int | None = None,
    validate: bool = True,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayStats:
    """Send ``lines`` to ``topic`` at ``rate`` events/s (0 = unthrottled).

    Pacing is schedule-based (event i is due at start + i/rate), so a slow poll or a GC pause
    is caught up rather than accumulating drift. ``clock`` / ``sleep`` are injectable for tests.
    """
    stats = ReplayStats()
    hdrs = headers_for(schema_id)

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is None:
            stats.delivered += 1
        else:
            stats.failed += 1

    def send(target: str, key: bytes | None, value: bytes, headers: list) -> None:
        while True:
            try:
                producer.produce(
                    target, key=key, value=value, headers=headers, on_delivery=on_delivery
                )
                return
            except BufferError:  # local queue full: drain callbacks, retry
                producer.poll(0.5)

    start = clock()
    for n, raw in enumerate(lines):
        if max_events is not None and stats.sent + stats.dead_lettered >= max_events:
            break
        if rate > 0:
            wait = start + n / rate - clock()
            if wait > 0.001:
                sleep(wait)
        result = validate_message(raw) if validate else None
        if result is not None and not result.ok:
            env = dlq.build_envelope(
                raw=raw,
                errors=result.errors,
                error_type=result.error_type or "contract_violation",
                stage="producer",
                source_topic=topic,
                key=(result.event or {}).get("claim_id"),
                schema_id=schema_id,
            )
            target, value, dlq_headers = dlq.to_message(env)
            send(target, None, value, dlq_headers + hdrs)
            stats.dead_lettered += 1
        else:
            send(topic, _key_of(raw, result), raw, hdrs)
            stats.sent += 1
        producer.poll(0)
    producer.flush(30)
    stats.elapsed_s = clock() - start
    return stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--events", type=Path, default=Path("data/raw/events/claim_events.jsonl"))
    p.add_argument("--topic", default=EVENTS_TOPIC)
    p.add_argument("--rate", type=float, default=100.0, help="events/s; 0 = unthrottled")
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--schema-id", type=int, default=None, help="skip the registry lookup")
    p.add_argument("--bootstrap", default=bootstrap_servers())
    p.add_argument("--registry-url", default=registry_url())
    p.add_argument("--no-validate", action="store_true", help="send lines without contract check")
    args = p.parse_args(argv)

    if not args.events.is_file():
        print(
            f"events file not found: {args.events} (run python -m claimsgen first)", file=sys.stderr
        )
        return 2
    schema_id = args.schema_id
    if schema_id is None:
        from .registry import RegistryClient, RegistryError

        try:
            schema_id = RegistryClient(args.registry_url).latest_schema_id(subject_for(args.topic))
        except (RegistryError, OSError) as exc:
            print(
                f"no schema id for {subject_for(args.topic)} ({exc}); run register_schema first "
                "or pass --schema-id",
                file=sys.stderr,
            )
            return 2

    from confluent_kafka import Producer

    producer = Producer(client_config(args.bootstrap, **PRODUCER_CONFIG))
    stats = replay(
        read_lines(args.events),
        producer,
        topic=args.topic,
        rate=args.rate,
        max_events=args.max_events,
        schema_id=schema_id,
        validate=not args.no_validate,
    )
    print(
        f"sent={stats.sent} dead_lettered={stats.dead_lettered} (-> {dlq_topic(args.topic)}) "
        f"delivered={stats.delivered} failed={stats.failed} "
        f"in {stats.elapsed_s:.1f}s ({stats.rate:.0f} ev/s) schema_id={schema_id}"
    )
    return 1 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
