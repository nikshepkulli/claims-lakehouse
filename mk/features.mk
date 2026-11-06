# Feature engineering (ticket #26). Included by the root Makefile via `-include mk/*.mk`.
# Pure Python + DuckDB over the gold Parquet export: no Spark, no Java, no Docker.
.PHONY: features features-demo features-verify test-features

FEATURES ?= data/gold-delta/features
GOLD_EXPORT ?= data/gold-delta/export
DEMO ?= data/features-demo
DEMO_ROWS ?= 20000

features: ## Build the point-in-time feature store from the gold export (GOLD_EXPORT=, FEATURES=); fails on leakage
	PYTHONPATH=src python -m lakehouse.features --gold-export $(GOLD_EXPORT) --out $(FEATURES)

features-demo: ## claimsgen -> gold-shaped export (no Spark) -> feature store under data/features-demo (DEMO_ROWS=)
	rm -rf $(DEMO)
	PYTHONPATH=src python -m claimsgen --rows $(DEMO_ROWS) --seed 42 --out $(DEMO)/raw --overwrite --quiet
	PYTHONPATH=src python -c "from lakehouse.features.fixture import from_claimsgen as f; f('$(DEMO)/raw', '$(DEMO)/export')"
	PYTHONPATH=src python -m lakehouse.features --gold-export $(DEMO)/export --out $(DEMO)/store

features-verify: ## Re-read the stored Parquet and compare to the manifest hashes (FEATURES=)
	PYTHONPATH=src python -m lakehouse.features --out $(FEATURES) --verify

test-features: ## Leakage + parity tests only (also part of `make test`)
	pytest -m "not spark" tests/features
