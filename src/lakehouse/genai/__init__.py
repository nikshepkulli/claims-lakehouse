"""Governed natural-language Q&A over the gold star schema (ticket #28).

Pipeline: catalog (registry-derived allow-list) -> prompt -> LLM -> sqlglot guardrails ->
DuckDB sandbox -> masking -> audit. See docs/genai.md. No real Bedrock call is made anywhere in
this repository's tests or CI.
"""
