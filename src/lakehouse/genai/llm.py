"""Provider-swappable LLM interface.

* ``FakeLLM``    deterministic, scripted; used by every test and CI run, and the local fallback
                 that needs no network or account.
* ``BedrockLLM`` Amazon Bedrock via the boto3 ``bedrock-runtime`` Converse API. It has NEVER been
                 called against a real account from this repository; tests only exercise it with
                 an injected stub client that mimics the documented response shape.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from .config import DEFAULT_MODEL_ID, aws_region


@dataclass(frozen=True)
class LLMResponse:
    text: str
    input_tokens: int = 0
    output_tokens: int = 0
    model_id: str = "fake"


class LLMClient(Protocol):
    def generate(self, system: str, question: str) -> LLMResponse: ...


class LLMError(RuntimeError):
    """The provider call failed or returned nothing usable."""


class FakeLLM:
    """Scripted model: question text -> raw model output. Unscripted questions get a refusal.

    ``script`` values may be a string or a callable(question) -> string. Token counts are a
    deterministic character-based estimate so the cost-cap logic is testable.
    """

    model_id = "fake-scripted"

    def __init__(self, script: Mapping[str, str | Callable[[str], str]] | None = None):
        self.script = dict(script or {})
        self.calls: list[tuple[str, str]] = []

    def generate(self, system: str, question: str) -> LLMResponse:
        self.calls.append((system, question))
        out = self.script.get(question)
        text = out(question) if callable(out) else out
        if text is None:
            text = "REFUSE: no scripted answer for this question"
        return LLMResponse(text, (len(system) + len(question)) // 4, len(text) // 4, self.model_id)


class BedrockLLM:
    """Converse API client. Region from env, model id from config, creds from the boto3 chain."""

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL_ID,
        max_tokens: int = 512,
        client: Any | None = None,
    ):
        self.model_id = model_id
        self.max_tokens = max_tokens
        self._client = client

    @property
    def client(self) -> Any:
        if self._client is None:
            region = aws_region()  # fail on a missing region before importing boto3
            import boto3  # lazy: boto3 is only needed for the real provider

            self._client = boto3.client("bedrock-runtime", region_name=region)
        return self._client

    def generate(self, system: str, question: str) -> LLMResponse:
        try:
            resp = self.client.converse(
                modelId=self.model_id,
                system=[{"text": system}],
                messages=[{"role": "user", "content": [{"text": question}]}],
                inferenceConfig={"maxTokens": self.max_tokens, "temperature": 0.0},
            )
            blocks = resp["output"]["message"]["content"]
            text = "".join(b.get("text", "") for b in blocks)
            usage = resp.get("usage", {})
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMError(f"unexpected Bedrock response shape: {exc!r}") from exc
        except Exception as exc:  # botocore errors; no import of botocore at module level
            raise LLMError(f"Bedrock call failed: {type(exc).__name__}") from exc
        if not text.strip():
            raise LLMError("Bedrock returned empty text")
        return LLMResponse(
            text, int(usage.get("inputTokens", 0)), int(usage.get("outputTokens", 0)), self.model_id
        )


_FENCE = re.compile(r"```(?:sql)?\s*(.*?)```", re.I | re.S)


def extract_sql(text: str) -> tuple[str | None, str | None]:
    """(sql, refusal_reason) from raw model output. Exactly one of them is not None.

    Takes the first fenced block, else the whole text. Anything the model says outside the fence
    is discarded, so trailing instructions or SQL cannot ride along.
    """
    stripped = text.strip()
    if stripped.upper().startswith("REFUSE"):
        return None, stripped.split(":", 1)[1].strip() if ":" in stripped else "model refused"
    m = _FENCE.search(stripped)
    sql = (m.group(1) if m else stripped).strip()
    if not sql:
        return None, "model returned no SQL"
    return sql, None
