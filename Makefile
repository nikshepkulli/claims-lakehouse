# claims-lakehouse root Makefile. Targets for lint/test/fmt live in mk/quality.mk.
.DEFAULT_GOAL := help
COMPOSE ?= docker compose
# Set LINEAGE=1 to include Marquez (compose profile `lineage`), e.g. `make demo LINEAGE=1`
PROFILE_FLAGS := $(if $(LINEAGE),--profile lineage,)

.PHONY: help env up down ps logs demo clean smoke verify-pins

help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

env: ## Create .env from .env.example if missing
	@test -f .env || { cp .env.example .env && echo "created .env from .env.example"; }

up: env ## Start the local stack and wait until healthy
	$(COMPOSE) $(PROFILE_FLAGS) up -d
	bash scripts/wait-healthy.sh 240

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) --profile lineage down --remove-orphans

clean: ## Stop the stack and delete all volumes
	$(COMPOSE) --profile lineage down --remove-orphans --volumes

ps: ## Show service status and health
	$(COMPOSE) --profile lineage ps

logs: ## Tail logs (SERVICE=name to filter)
	$(COMPOSE) --profile lineage logs -f --tail=100 $(SERVICE)

smoke: ## Probe every service endpoint
	bash tests/stack/smoke.sh

verify-pins: ## Fail if any compose image is not digest-pinned
	bash scripts/verify-pins.sh

demo: up smoke ## One command: up + wait healthy + verify + print URLs
	@echo ""
	@echo "Stack is healthy. Open:"
	@echo "  MinIO console     http://localhost:9001   (see .env for creds)"
	@echo "  Redpanda Console  http://localhost:8080"
	@echo "  Spark master UI   http://localhost:8081"
	@echo "  Spark worker UI   http://localhost:8082"
	@echo "  Postgres          localhost:5432"
	@if [ -n "$(LINEAGE)" ]; then echo "  Marquez UI        http://localhost:3002"; fi
	@echo ""
	@echo "TODO(#5):  generate synthetic claims"
	@echo "TODO(#6):  land raw files in bronze"
	@echo "TODO(#7):  bronze -> silver Spark job"
	@echo "TODO(#8):  gold facts and dimensions"
	@echo "TODO(#11): stream claim events through Redpanda"
	@echo "TODO(#24): emit OpenLineage events to Marquez"
	@echo "TODO(#31): finalize the 10-minute demo"

-include mk/*.mk
