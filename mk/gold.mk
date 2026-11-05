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
pipeline-e2e: ## claimsgen -> bronze -> silver -> gold -> dbt on a small fixture (2000 claims, 14 days)
	rm -rf $(E2E)
	PYTHONPATH=src python -m claimsgen --rows 2000 --days 14 --seed 42 --out $(E2E)/raw --overwrite --quiet
	$(MAKE) bronze RAW=$(E2E)/raw BRONZE=$(E2E)/bronze
	$(MAKE) silver BRONZE=$(E2E)/bronze SILVER=$(E2E)/silver
	$(MAKE) gold SILVER=$(E2E)/silver GOLD=$(E2E)/gold
	$(MAKE) gold-dbt GOLD=$(E2E)/gold
