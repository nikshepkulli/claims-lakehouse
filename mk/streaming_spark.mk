# Spark Structured Streaming job (ticket #12). Loaded by the root Makefile via `-include mk/*.mk`
# after mk/bronze.mk (alphabetical), so SPARK_IMAGE and SPARK_CACHE_VOL are available.
#
# Why not reuse SPARK_RUN: the pinned apache/spark image ships Python 3.10, but
# requirements-claimsgen.txt pins numpy 2.4.x (needs Python >= 3.11, which CI uses). STREAM_RUN
# installs the same requirements with the numpy pin relaxed into its own cache dir, so the
# existing bronze/silver cache is untouched. CI is unaffected (it runs Python 3.11 + the pins).
.PHONY: streaming-spark-test streaming-spark-run streaming-spark-demo streaming-spark-smoke spark-py

STREAM_NETWORK ?= claims-lakehouse_default
STREAM_ROOT ?= data/streaming
STREAM_BOOTSTRAP ?= redpanda:9092
STREAM_TOPIC ?= claims.events.v1
STREAM_ARGS ?=
# Explicit files: the image's Python is 3.10 and other tests/streaming modules need 3.11 (CI runs the dir)
STREAM_SPARK_TESTS ?= tests/streaming/test_spark_job.py tests/streaming/test_spark_rules.py tests/streaming/test_spark_gates.py tests/streaming/test_dlq_replay.py
# the job reaches Redpanda's internal listener, so the container joins the compose network
STREAM_DOCKER_ARGS ?= --network $(STREAM_NETWORK)

define STREAM_RUN
docker run --rm -u root $(STREAM_DOCKER_ARGS) -e HOME=/cache/home -e PYTHONPATH=/work/src:/cache/spy \
	-e LAKEHOUSE_S3_ENDPOINT -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
	-v "$(CURDIR)":/work -v $(SPARK_CACHE_VOL):/cache -w /work --entrypoint bash $(SPARK_IMAGE) -c \
	'unset SPARK_HOME; h=$$(cat requirements-*.txt | md5sum | cut -c1-12); \
	 [ -f /cache/sok-$$h ] || { rm -rf /cache/spy /cache/sok-*; \
	   cat requirements-spark.txt requirements-claimsgen.txt | grep -v "^-r" | sed "s/^numpy==.*/numpy<2.3/" > /tmp/req.txt; \
	   pip install -q --disable-pip-version-check --target /cache/spy -r /tmp/req.txt \
	   && touch /cache/sok-$$h; } && $(1)'
endef

spark-py: ## Run a python script inside the pinned Spark image (PY=path/to/script.py)
	$(call STREAM_RUN,python3 $(PY))

streaming-spark-test: STREAM_DOCKER_ARGS =
streaming-spark-test: ## Run the streaming Spark tests (-m spark) in the pinned Spark image
	$(call STREAM_RUN,python3 -m pytest -m spark -p no:cacheprovider $(STREAM_SPARK_TESTS))

streaming-spark-demo: ## Kafka -> bronze -> silver with a processingTime trigger for 60s (needs `make up` + `make produce-demo`)
	$(MAKE) streaming-spark-run STREAM_MODE=processingTime STREAM_ARGS="--run-seconds 60 $(STREAM_ARGS)"

streaming-spark-run: ## Run the job once against the stack (STREAM_MODE=availableNow|processingTime, STREAM_ARGS=...)
	$(call STREAM_RUN,python3 -m lakehouse.streaming.spark_job --source kafka \
		--bootstrap $(STREAM_BOOTSTRAP) --topic $(STREAM_TOPIC) --root $(STREAM_ROOT) \
		--trigger $(or $(STREAM_MODE),availableNow) $(STREAM_ARGS))

streaming-spark-smoke: ## Integration test against the running stack (5,000 events, availableNow); not in CI
	bash tests/stack/streaming_spark_smoke.sh

# ---- ticket #13: gates, DLQ topic, replay
.PHONY: streaming-dlq-replay streaming-dq-smoke

streaming-dlq-replay: ## Re-validate dead letters, append the now-valid ones to bronze (STREAM_ARGS="--source kafka --bootstrap redpanda:9092 --dry-run")
	$(call STREAM_RUN,python3 -m lakehouse.streaming.spark_job replay-dlq \
		--root $(STREAM_ROOT) --topic $(STREAM_TOPIC) $(STREAM_ARGS))

streaming-dq-smoke: ## Gates + Kafka DLQ topic + replay against the running stack; not in CI
	bash tests/stack/streaming_dq_smoke.sh
