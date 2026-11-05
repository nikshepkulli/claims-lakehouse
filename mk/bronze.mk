# Bronze ingestion + Spark tests. Included by the root Makefile via `-include mk/*.mk`.
# `bronze` and `test-spark` run Spark inside the digest-pinned apache/spark image from
# docker-compose.yml (no local Java needed). Caches (pip, ivy jars) persist in a named volume.
.PHONY: bronze test-spark

SPARK_IMAGE ?= $(shell sed -n 's/^ *image: *\(apache\/spark@sha256:[0-9a-f]*\).*/\1/p' docker-compose.yml | head -1)
SPARK_CACHE_VOL ?= claims-lakehouse-spark-cache
RAW ?= data/raw
BRONZE ?= data/bronze
BATCH_DATE ?= all

# Shared container prelude: install pinned deps into the cached volume, then run $(1).
define SPARK_RUN
docker run --rm -u root -e HOME=/cache/home -e PYTHONPATH=/work/src:/cache/py \
	-e LAKEHOUSE_S3_ENDPOINT -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
	-v "$(CURDIR)":/work -v $(SPARK_CACHE_VOL):/cache -w /work --entrypoint bash $(SPARK_IMAGE) -c \
	'unset SPARK_HOME; h=$$(cat requirements-*.txt | md5sum | cut -c1-12); \
	 [ -f /cache/ok-$$h ] || { rm -rf /cache/py /cache/ok-*; pip install -q --disable-pip-version-check --target /cache/py -r requirements-spark.txt && touch /cache/ok-$$h; } && $(1)'
endef

bronze: ## Ingest data/raw into Delta bronze in Docker (RAW=, BRONZE=, BATCH_DATE=YYYY-MM-DD|all)
	$(call SPARK_RUN,python3 -m lakehouse.bronze --input $(RAW) --bronze $(BRONZE) --batch-date $(BATCH_DATE))

test-spark: ## Run the Spark/Delta pytest suite (-m spark) inside the pinned Spark image
	$(call SPARK_RUN,python3 -m pytest -m spark -p no:cacheprovider tests/bronze)
