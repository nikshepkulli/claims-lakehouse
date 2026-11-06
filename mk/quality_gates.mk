# Data-quality gates (ticket #22). Included by the root Makefile via `-include mk/*.mk`.
# Runs the Great Expectations suites of every layer against Delta/Parquet on disk; pure Python
# (pandas + delta-rs), no Spark or Docker. Needs `pip install -r requirements-quality.txt`.
# Produces $(DQ_OUT)/<layer>-result.json and the HTML data docs in $(DQ_OUT)/data_docs/index.html.
.PHONY: dq dq-layer dq-test

DQ_BASE ?= data/e2e
DQ_OUT ?= data/dq
DQ_LAYERS ?= bronze silver gold
DQ_ARGS ?=

dq: ## Validate bronze/silver/gold under DQ_BASE (default data/e2e, from `make pipeline-e2e`); exit 1 on a blocking failure
	@rc=0; for l in $(DQ_LAYERS); do \
	  PYTHONPATH=src python -m lakehouse.quality run --layer $$l --root $(DQ_BASE)/$$l --out $(DQ_OUT) $(DQ_ARGS) || rc=1; \
	done; \
	echo "data docs: $(DQ_OUT)/data_docs/index.html"; exit $$rc

dq-layer: ## Validate one layer: make dq-layer LAYER=silver [DQ_BASE=...]
	PYTHONPATH=src python -m lakehouse.quality run --layer $(LAYER) --root $(DQ_BASE)/$(LAYER) --out $(DQ_OUT) $(DQ_ARGS)

dq-test: ## Unit + negative (injected-defect) tests of the quality framework, no Spark
	PYTHONPATH=src python -m pytest tests/quality
