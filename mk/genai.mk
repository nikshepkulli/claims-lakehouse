# Governed GenAI Q&A (ticket #28, docs/genai.md). Needs `pip install -r requirements-genai.txt`
# (boto3 only for the bedrock provider). Everything here is offline: the default provider is the
# scripted FakeLLM. Nothing in this file calls AWS.
.PHONY: genai-test genai-eval genai-ask

GENAI_GOLD ?= data/gold-delta/export
GENAI_AUDIT ?= data/genai/audit.jsonl

genai-test: ## Governed Q&A unit + integration tests (no Spark, no AWS)
	PYTHONPATH=src python -m pytest -p no:cacheprovider tests/genai

genai-eval: ## Golden (30) + injection (24) evaluation with scripted FakeLLM over GENAI_GOLD
	PYTHONPATH=src python -m lakehouse.genai eval --gold-dir $(GENAI_GOLD) --audit-log $(GENAI_AUDIT)

genai-ask: ## Ask one question (Q="..."), offline fake provider; PROVIDER=bedrock needs AWS_REGION + creds
	PYTHONPATH=src python -m lakehouse.genai ask "$(Q)" --provider $(or $(PROVIDER),fake) --gold-dir $(GENAI_GOLD) --audit-log $(GENAI_AUDIT)
