# Airflow orchestration (ticket #9). Included by the root Makefile via `-include mk/*.mk`.
# Standalone Airflow in one container (orchestration/airflow/docker-compose.yml); see
# docs/orchestration.md. First run installs dependencies into a named volume (a few minutes).
.PHONY: airflow-up airflow-down airflow-test airflow-backfill airflow-sh airflow-clean test-airflow

AIRFLOW_COMPOSE ?= $(COMPOSE) -f orchestration/airflow/docker-compose.yml
# Logical dates for airflow-test, in order (the first date also carries the dimension snapshot).
AIRFLOW_DATES ?= 2024-01-01 2024-01-02 2024-01-03
AIRFLOW_START ?= 2024-01-01
AIRFLOW_END ?= 2024-01-03

airflow-up: ## Start standalone Airflow on http://localhost:8085 (user admin; password in `docker compose logs`)
	$(AIRFLOW_COMPOSE) up -d
	@echo "Airflow UI http://localhost:8085  (password: $(AIRFLOW_COMPOSE) logs airflow | grep -i password)"

airflow-down: ## Stop Airflow (keeps the cache volume and data/airflow)
	$(AIRFLOW_COMPOSE) down --remove-orphans

airflow-test: ## airflow dags test claims_daily for AIRFLOW_DATES, in order (runs Spark locally in Docker)
	$(AIRFLOW_COMPOSE) run --rm --no-deps -T airflow test $(AIRFLOW_DATES)

airflow-backfill: ## airflow dags backfill claims_daily AIRFLOW_START..AIRFLOW_END
	$(AIRFLOW_COMPOSE) run --rm --no-deps -T airflow backfill $(AIRFLOW_START) $(AIRFLOW_END)

airflow-sh: ## Shell in the prepared Airflow container (e.g. airflow tasks clear)
	$(AIRFLOW_COMPOSE) run --rm --no-deps airflow sh bash

airflow-clean: ## Delete the Airflow cache volume (metadata db, installed deps) and data/airflow
	$(AIRFLOW_COMPOSE) down --remove-orphans --volumes
	rm -rf data/airflow

test-airflow: ## DagBag + structural tests (needs `pip install -r requirements-airflow.txt` with constraints)
	pytest -p no:cacheprovider tests/orchestration
