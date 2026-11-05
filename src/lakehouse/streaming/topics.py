"""Topic provisioning (idempotent).

    python -m lakehouse.streaming.topics                    # events topic + its .dlq
    python -m lakehouse.streaming.topics --topic my.topic   # ad hoc topic (+ .dlq), event settings
    python -m lakehouse.streaming.topics --delete my.topic  # remove ad hoc topic and its DLQ

Design decisions are documented in docs/streaming.md (partitions, retention, why no compaction).
Local stack is a single broker, so replication factor is 1; raise it on a real cluster.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field

from .config import EVENTS_TOPIC, bootstrap_servers, client_config, dlq_topic

DAY_MS = 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    cleanup_policy: str = "delete"
    replication_factor: int = 1
    extra: dict[str, str] = field(default_factory=dict)

    def configs(self) -> dict[str, str]:
        return {
            "retention.ms": str(self.retention_ms),
            "cleanup.policy": self.cleanup_policy,
            **self.extra,
        }


def specs_for(topic: str) -> list[TopicSpec]:
    """Event topic + its DLQ. Events: 6 partitions, 7d. DLQ: 1 partition, 30d."""
    return [
        TopicSpec(topic, partitions=6, retention_ms=7 * DAY_MS),
        TopicSpec(dlq_topic(topic), partitions=1, retention_ms=30 * DAY_MS),
    ]


def ensure(specs: list[TopicSpec], bootstrap: str) -> dict[str, str]:
    """Create missing topics; existing ones are left untouched. Returns name -> status."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(client_config(bootstrap))
    futures = admin.create_topics(
        [
            NewTopic(
                s.name,
                num_partitions=s.partitions,
                replication_factor=s.replication_factor,
                config=s.configs(),
            )
            for s in specs
        ]
    )
    status: dict[str, str] = {}
    for name, fut in futures.items():
        try:
            fut.result()
            status[name] = "created"
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            status[name] = "exists"
    return status


def delete(names: list[str], bootstrap: str) -> None:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(client_config(bootstrap))
    for name, fut in admin.delete_topics(names, operation_timeout=15).items():
        try:
            fut.result()
            print(f"deleted {name}")
        except KafkaException as exc:
            print(f"delete {name}: {exc}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--topic", default=EVENTS_TOPIC)
    p.add_argument("--delete", metavar="TOPIC", help="delete TOPIC and TOPIC.dlq")
    p.add_argument("--bootstrap", default=bootstrap_servers())
    args = p.parse_args(argv)
    if args.delete:
        delete([args.delete, dlq_topic(args.delete)], args.bootstrap)
        return 0
    for name, st in ensure(specs_for(args.topic), args.bootstrap).items():
        print(f"{st:8} {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
