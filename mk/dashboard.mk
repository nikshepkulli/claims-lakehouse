# Static analytics dashboard (ticket #29). Needs `pip install -r requirements-dbt.txt`.
# Reads the gold Parquet (the fixture from `make dbt-data` by default), builds site/index.html.
.PHONY: dashboard dashboard-serve dashboard-test dashboard-sample

GOLD ?= data/gold
RAW ?= data/raw
SITE ?= site
PORT ?= 8000

dashboard: ## Build the static dashboard (site/index.html) from the gold fixture
	@test -d $(GOLD)/fact_claim || $(MAKE) dbt-data
	python -m dashboard.build --gold $(GOLD) --raw $(RAW) --out $(SITE)

dashboard-serve: dashboard ## Build and serve the dashboard on http://localhost:$(PORT)
	python -m http.server $(PORT) --directory $(SITE)

dashboard-test: ## Run the dashboard tests (builds a throwaway site, under a minute)
	python -m pytest tests/dashboard -q

# Pipeline runs view (#30): real local CLI runs on the synthetic fixture, then the page with them.
SAMPLE ?= data/dashboard-sample
dashboard-sample: ## Build site/index.html with the lineage graph, run timeline and quality trend from a fresh sample run
	python -m dashboard.sample --out $(SAMPLE)
	@test -d $(GOLD)/fact_claim || $(MAKE) dbt-data
	python -m dashboard.build --gold $(GOLD) --raw $(RAW) --out $(SITE) \
	  --lineage-events $(SAMPLE)/lineage/events.jsonl --column-lineage lineage/column_lineage.yaml \
	  --runs $(SAMPLE)/ops/pipeline_runs --alerts $(SAMPLE)/ops/alerts.jsonl \
	  --quality-history $(SAMPLE)/dq --pipeline-label "sample run from the synthetic fixture"
