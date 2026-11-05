# Streaming ingest targets (ticket #11). Needs the local stack (`make up`) and
# `pip install -r requirements-streaming.txt -r requirements-claimsgen.txt`.
PYTHON ?= python
STREAM_ENV := PYTHONPATH=src
EVENTS_FILE ?= data/raw/events/claim_events.jsonl
DEMO_EVENTS ?= 1000
DEMO_RATE ?= 500

.PHONY: topics register-schema produce-demo streaming-smoke

topics: ## Create claims.events.v1 and its .dlq topic (idempotent)
	$(STREAM_ENV) $(PYTHON) -m lakehouse.streaming.topics

register-schema: ## Register the claim-event contract (BACKWARD) in the registry
	$(STREAM_ENV) $(PYTHON) -m lakehouse.streaming.register_schema

produce-demo: topics register-schema ## Replay DEMO_EVENTS generated events at DEMO_RATE/s
	@test -f $(EVENTS_FILE) || $(STREAM_ENV) $(PYTHON) -m claimsgen --rows 2000 --out data/raw
	$(STREAM_ENV) $(PYTHON) -m lakehouse.streaming.produce --events $(EVENTS_FILE) \
		--rate $(DEMO_RATE) --max-events $(DEMO_EVENTS) --topic claims.events.v1

streaming-smoke: ## Integration test against the running stack (1,000 events round trip)
	bash tests/stack/streaming_smoke.sh
