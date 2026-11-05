# Silver layer. Included by the root Makefile via `-include mk/*.mk`. Reuses SPARK_RUN and the
# variables from mk/bronze.mk (Docker, pinned Spark image, cached pip volume), so it must load
# after it (alphabetical: bronze.mk < silver.mk).
.PHONY: silver

SILVER ?= data/silver
SPARK_TESTS += tests/silver

silver: ## Build Delta silver from bronze in Docker (BRONZE=, SILVER=, BATCH_DATE=YYYY-MM-DD|all)
	$(call SPARK_RUN,python3 -m lakehouse.silver --bronze $(BRONZE) --silver $(SILVER) --batch-date $(BATCH_DATE))
