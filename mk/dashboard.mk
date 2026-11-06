# Static analytics dashboard (ticket #29). Needs `pip install -r requirements-dbt.txt`.
# Reads the gold Parquet (the fixture from `make dbt-data` by default), builds site/index.html.
.PHONY: dashboard dashboard-serve dashboard-test

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
