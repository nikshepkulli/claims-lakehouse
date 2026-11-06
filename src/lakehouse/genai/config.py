"""Configuration for governed Q&A. No credentials live here: AWS auth comes from the standard
boto3 chain (env vars, profile, role) at call time, and the region from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass

# Placeholder Bedrock model id. It is configuration, NOT a verified choice: the id must be enabled
# in the caller's AWS account/region, and nothing in this repo has ever called it.
DEFAULT_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"


@dataclass(frozen=True)
class GenAIConfig:
    role: str = "analyst"
    max_rows: int = 100  # hard cap injected as LIMIT and enforced on fetch
    max_joins: int = 4
    max_sql_chars: int = 4000
    max_question_chars: int = 500
    timeout_s: float = 10.0  # wall-clock query timeout (DuckDB interrupt)
    max_cost_units: int = 400  # static plan-cost ceiling, see guardrails.estimate_cost
    session_max_queries: int = 50  # cost cap: queries (LLM calls) per session
    session_max_tokens: int = 200_000  # cost cap: LLM input+output tokens per session
    model_id: str = DEFAULT_MODEL_ID
    max_output_tokens: int = 512
    screen_questions: bool = True  # heuristic input screen; the SQL guardrails never depend on it

    @classmethod
    def from_env(cls, **overrides) -> GenAIConfig:
        env = {"model_id": os.environ.get("LAKEHOUSE_GENAI_MODEL_ID")}
        base = {k: v for k, v in env.items() if v}
        return cls(**{**base, **overrides})


def aws_region() -> str:
    """Region for bedrock-runtime, from the environment only (never hard-coded)."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise RuntimeError("set AWS_REGION (or AWS_DEFAULT_REGION) to use the Bedrock provider")
    return region
