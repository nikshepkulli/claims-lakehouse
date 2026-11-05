"""Connection defaults and naming conventions shared by every streaming component.

Topic convention: ``<domain>.<entity>.v<major>`` (e.g. ``claims.events.v1``). A breaking
contract change ships as a new major topic (``claims.events.v2``), never as an in-place edit.
The dead-letter topic of ``T`` is ``T.dlq``. The registry subject of ``T`` is ``T-value``
(Confluent TopicNameStrategy, which Redpanda's registry supports).
"""

from __future__ import annotations

import os
from pathlib import Path

EVENTS_TOPIC = "claims.events.v1"
DLQ_SUFFIX = ".dlq"
CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def bootstrap_servers() -> str:
    """Kafka bootstrap address; override with KAFKA_BOOTSTRAP."""
    return os.environ.get("KAFKA_BOOTSTRAP") or (
        f"localhost:{os.environ.get('REDPANDA_KAFKA_PORT', '19092')}"
    )


def client_config(bootstrap: str | None = None, **extra: object) -> dict[str, object]:
    """Base librdkafka config. IPv4 only: the broker advertises ``localhost`` and Docker
    publishes on 127.0.0.1, so trying ::1 first just spams connection-refused warnings."""
    return {
        "bootstrap.servers": bootstrap or bootstrap_servers(),
        "broker.address.family": "v4",
        **extra,
    }


def registry_url() -> str:
    """Schema registry base URL; override with SCHEMA_REGISTRY_URL."""
    return os.environ.get("SCHEMA_REGISTRY_URL") or (
        f"http://localhost:{os.environ.get('REDPANDA_SCHEMA_REGISTRY_PORT', '18081')}"
    )


def dlq_topic(topic: str) -> str:
    """Dead-letter topic for ``topic``. Idempotent: a DLQ topic maps to itself."""
    return topic if topic.endswith(DLQ_SUFFIX) else topic + DLQ_SUFFIX


def subject_for(topic: str) -> str:
    """Registry subject (value schema) for ``topic``."""
    return f"{topic}-value"


def contract_path(name: str = "claim_event", major: int = 1) -> Path:
    return CONTRACTS_DIR / f"{name}.v{major}.schema.json"
