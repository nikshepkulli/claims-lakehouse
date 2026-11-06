# Gold layer (ticket #8). Included by the root Makefile via `-include mk/*.mk`. Reuses SPARK_RUN
# and the BRONZE/SILVER/BATCH_DATE variables from mk/bronze.mk and mk/silver.mk (recipe variables
# expand lazily, so load order between the .mk files does not matter).
.PHONY: gold gold-dbt pipeline-e2e keyed-tokens-test

# Delta gold root. Not data/gold: that is where the dbt TEST FIXTURE writes (mk/dbt.mk).
GOLD ?= data/gold-delta
SPARK_TESTS += tests/gold

# Gold tokenises member_id/provider_id with a keyed HMAC (ticket #71) and refuses to run without a
# key. Supply it from your secret store: `export LAKEHOUSE_PSEUDONYM_KEY=...` (>= 32 chars,
# LAKEHOUSE_PSEUDONYM_KEY_ID names its generation, default v1). Never commit a key. The e2e
# target below uses a FAKE, public, test-only key because its data is synthetic and throwaway.
E2E_PSEUDONYM_KEY ?= GOLD-TESTS-ONLY-NOT-A-SECRET-0123456789

gold: ## Build the Delta gold star schema + Parquet export from silver in Docker (SILVER=, GOLD=, BATCH_DATE=YYYY-MM-DD|all; needs LAKEHOUSE_PSEUDONYM_KEY)
	@test -n "$$LAKEHOUSE_PSEUDONYM_KEY" || { echo "set LAKEHOUSE_PSEUDONYM_KEY (>= 32 chars, from your secret store; see docs/privacy-and-access.md)"; exit 1; }
	$(call SPARK_RUN,python3 -m lakehouse.gold --silver $(SILVER) --gold $(GOLD) --batch-date $(BATCH_DATE))

gold-dbt: dbt/profiles.yml ## dbt build on DuckDB against the REAL gold export ($(GOLD)/export) instead of the fixture
	GOLD_PATH=$(GOLD)/export $(DBT) deps $(DBT_FLAGS)
	GOLD_PATH=$(GOLD)/export $(DBT) build $(DBT_FLAGS) --target $(DBT_TARGET)

E2E ?= data/e2e
E2E_DS ?= claims,claim_lines,diagnoses,members,providers
pipeline-e2e: export LAKEHOUSE_PSEUDONYM_KEY := $(E2E_PSEUDONYM_KEY)
pipeline-e2e: ## claimsgen -> bronze -> silver -> gold -> dbt (+ end-to-end checks), 2000 claims in 3 batches
	rm -rf $(E2E)
	PYTHONPATH=src python -m claimsgen --rows 2000 --seed 42 --out $(E2E)/raw --overwrite --quiet
	python scripts/bucket_raw_batches.py --raw $(E2E)/raw --out $(E2E)/raw_batched --batches 3
	$(call SPARK_RUN,python3 -m lakehouse.bronze --input $(E2E)/raw_batched --bronze $(E2E)/bronze --batch-date all --datasets $(E2E_DS))
	$(call SPARK_RUN,python3 -m lakehouse.silver --bronze $(E2E)/bronze --silver $(E2E)/silver --batch-date all --datasets $(E2E_DS))
	$(call SPARK_RUN,python3 -m lakehouse.gold --silver $(E2E)/silver --gold $(E2E)/gold --batch-date all)
	$(MAKE) gold-dbt GOLD=$(E2E)/gold
	$(call SPARK_RUN,GOLD_E2E_DIR=$(E2E) python3 -m pytest -m spark -p no:cacheprovider tests/gold/test_gold_pipeline_spark.py)

keyed-tokens-test: ## Ticket #71: keyed-token tests that need no Spark (tokeniser, rotation, fixture, registry, Snowflake code)
	python -m pytest -p no:cacheprovider tests/governance tests/gold/test_gold_ddl_contract.py tests/dbt/test_analyst_masking.py
