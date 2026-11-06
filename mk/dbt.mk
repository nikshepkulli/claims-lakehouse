# dbt targets (ticket #15). Needs `pip install -r requirements-dbt.txt`. Runs from the repo root.
# The profile is dbt/profiles.yml (git-ignored); it is copied from the example when missing.
.PHONY: dbt-data dbt-build dbt-docs dbt-lint dbt-clean

# dbt-core 1.12 rejects --project-dir/--profiles-dir BEFORE the subcommand: they must follow it
# (as CI does). So the command and the shared flags are separate: `$(DBT) <sub> $(DBT_FLAGS)`.
DBT ?= dbt
DBT_FLAGS ?= --project-dir dbt --profiles-dir dbt
DBT_TARGET ?= duckdb
CLAIMS ?= 5000

dbt/profiles.yml:
	cp dbt/profiles.yml.example dbt/profiles.yml

dbt-data: ## Generate claimsgen data and the gold TEST FIXTURE (data/gold)
	@test -d data/raw/claims || PYTHONPATH=src python -m claimsgen --rows $(CLAIMS) --out data/raw
	python scripts/make_gold_fixture.py --raw data/raw --out data/gold

dbt-build: dbt/profiles.yml ## dbt deps + build (models + tests) on DuckDB
	$(DBT) deps $(DBT_FLAGS)
	$(DBT) build $(DBT_FLAGS) --target $(DBT_TARGET)

dbt-docs: dbt/profiles.yml ## Generate the dbt docs site (dbt/target/index.html)
	$(DBT) deps $(DBT_FLAGS)
	$(DBT) docs generate $(DBT_FLAGS) --target $(DBT_TARGET)

dbt-lint: ## sqlfluff for dbt/ (Jinja templater, needs to run inside dbt/)
	cd dbt && sqlfluff lint .

dbt-clean: ## Remove dbt build output and the local DuckDB file
	rm -rf dbt/target dbt/dbt_packages data/claims.duckdb
