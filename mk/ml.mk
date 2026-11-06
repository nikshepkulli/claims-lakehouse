# SageMaker-ready claim-denial-risk model (ticket #27). Included by the root Makefile via `-include mk/*.mk`.
# Pure Python + scikit-learn + DuckDB: no AWS calls, no Spark, no Docker. Needs requirements-ml.txt.
.PHONY: ml-demo ml-train ml-score test-ml ml-image

ML_DEMO ?= data/ml-demo
ML_ROWS ?= 20000
FEATURES ?= data/gold-delta/features
ML_MODEL ?= data/ml/model
ML_OUT ?= data/ml/output
ML_SCORES ?= data/ml/scores

ml-demo: ## claimsgen -> features -> train twice (assert identical metrics) -> score twice (assert idempotent) under ML_DEMO (ML_ROWS=)
	PYTHONPATH=src python -W ignore -m lakehouse.ml demo --workdir $(ML_DEMO) --rows $(ML_ROWS)

ml-train: ## Train + calibrate + evaluate on a feature store (FEATURES=, ML_MODEL=, ML_OUT=); add HP="--hp seed=1"
	PYTHONPATH=src python -W ignore -m lakehouse.ml train --store $(FEATURES) --model-dir $(ML_MODEL) --output-dir $(ML_OUT) $(HP)

ml-score: ## Batch-score claim_features with a trained model (FEATURES=, ML_MODEL=, ML_SCORES=); idempotent
	PYTHONPATH=src python -W ignore -m lakehouse.ml score --store $(FEATURES) --model-dir $(ML_MODEL) --out $(ML_SCORES)

ml-image: ## Build the SageMaker container locally (not run in CI)
	docker build -f src/lakehouse/ml/Dockerfile -t claims-denial-risk:local .

test-ml: ## ML unit tests only (also part of `make test`)
	pytest -m "not spark" tests/ml
