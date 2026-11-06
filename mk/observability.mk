# Observability (ticket #23). Spark-free: needs pyarrow + PyYAML only.
PYTHON ?= python
OBS := PYTHONPATH=src $(PYTHON) -m lakehouse.observability
OPS ?= data/ops
.PHONY: obs-check obs-runs obs-reconcile obs-test

obs-check: ## Evaluate freshness/volume/failure rules over the run ledger (OPS=data/ops); exit 1 on critical
	$(OBS) --ops-root $(OPS) check

obs-runs: ## Print the run ledger as JSON lines
	$(OBS) --ops-root $(OPS) runs

obs-reconcile: ## Load gold_reconciliation (GOLD=data/gold-delta) into the ledger
	$(OBS) --ops-root $(OPS) ingest-reconciliation --table-dir $(GOLD)/gold_reconciliation

obs-test: ## Observability unit + e2e tests (no Spark)
	pytest tests/observability
