# Gold layer (ticket #8). Included by the root Makefile via `-include mk/*.mk`. Reuses SPARK_RUN
# and the BRONZE/SILVER/BATCH_DATE variables from mk/bronze.mk and mk/silver.mk (recipe variables
# expand lazily, so load order between the .mk files does not matter).
.PHONY: gold gold-dbt pipeline-e2e

# Delta gold root. Not data/gold: that is where the dbt TEST FIXTURE writes (mk/dbt.mk).
GOLD ?= data/gold-delta
SPARK_TESTS += tests/gold

gold: ## Build the Delta gold star schema + Parquet export from silver in Docker (SILVER=, GOLD=, BATCH_DATE=YYYY-MM-DD|all)
	$(call SPARK_RUN,python3 -m lakehouse.gold --silver $(SILVER) --gold $(GOLD) --batch-date $(BATCH_DATE))

gold-dbt: dbt/profiles.yml ## dbt build on DuckDB against the REAL gold export ($(GOLD)/export) instead of the fixture
	GOLD_PATH=$(GOLD)/export $(DBT) deps
	GOLD_PATH=$(GOLD)/export $(DBT) build --target $(DBT_TARGET)

E2E ?= data/e2e
E2E_DS ?= claims,claim_lines,diagnoses,members,providers
pipeline-e2e: ## claimsgen -> bronze -> silver -> gold -> dbt (+ end-to-end checks), 2000 claims in 3 batches
	rm -rf $(E2E)
	PYTHONPATH=src python -m claimsgen --rows 2000 --seed 42 --out $(E2E)/raw --overwrite --quiet
	python scripts/bucket_raw_batches.py --raw $(E2E)/raw --out $(E2E)/raw_batched --batches 3
	$(call SPARK_RUN,python3 -m lakehouse.bronze --input $(E2E)/raw_batched --bronze $(E2E)/bronze --batch-date all --datasets $(E2E_DS))
	$(call SPARK_RUN,python3 -m lakehouse.silver --bronze $(E2E)/bronze --silver $(E2E)/silver --batch-date all --datasets $(E2E_DS))
	$(call SPARK_RUN,python3 -m lakehouse.gold --silver $(E2E)/silver --gold $(E2E)/gold --batch-date all)
	$(MAKE) gold-dbt GOLD=$(E2E)/gold
	$(call SPARK_RUN,GOLD_E2E_DIR=$(E2E) python3 -m pytest -m spark -p no:cacheprovider tests/gold/test_gold_pipeline_spark.py)
