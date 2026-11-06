# Lineage (ticket #24). Spark-free: PyYAML + pyarrow (+ jsonschema for --validate and tests).
# Events go to a JSON-lines file by default; add TRANSPORT="--transport http" (Marquez, started with
# `make up LINEAGE=1`) to also POST them. Transport failures are logged and counted, never fatal.
PYTHON ?= python
LIN := PYTHONPATH=src $(PYTHON) -m lakehouse.lineage
LIN_OPS ?= data/ops
LIN_BASE ?= data/e2e
LIN_FILE ?= data/lineage/events.jsonl
LIN_DQ ?= data/dq
TRANSPORT ?=
MARQUEZ_URL ?= http://localhost:5002
LIN_ROOTS = --landing $(LIN_BASE)/raw --bronze $(LIN_BASE)/bronze --silver $(LIN_BASE)/silver --gold $(LIN_BASE)/gold
LIN_OUT = --file $(LIN_FILE) --marquez-url $(MARQUEZ_URL) $(TRANSPORT)
.PHONY: lineage-emit lineage-quality lineage-check lineage-test

lineage-emit: ## Replay the run ledger (LIN_OPS) as OpenLineage events for the layers under LIN_BASE
	$(LIN) emit --ops-root $(LIN_OPS) $(LIN_ROOTS) $(LIN_OUT) --validate

lineage-quality: ## Emit quality.<layer> runs from the #22 result JSON in LIN_DQ (run `make dq` first)
	$(LIN) quality $(foreach l,bronze silver gold,--result $(LIN_DQ)/$(l)-result.json) $(LIN_ROOTS) $(LIN_OUT) --validate

lineage-check: ## Fail if lineage/column_lineage.yaml drifts from the gold schema / silver contract
	$(LIN) check-mapping

lineage-test: ## Lineage unit + CLI tests (no Spark, no Docker, no network beyond loopback)
	PYTHONPATH=src $(PYTHON) -m pytest tests/lineage
