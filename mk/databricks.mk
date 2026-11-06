# Databricks Asset Bundle (ticket #10). Included by the root Makefile via `-include mk/*.mk`.
# docs/databricks.md has the full runbook. Only `databricks-validate` runs without an account.
.PHONY: databricks-validate databricks-wheel databricks-test databricks-deploy databricks-run

DATABRICKS ?= databricks
DBX_TARGET ?= dev

databricks-test: ## Offline bundle tests (YAML structure, CLI args, entrypoint, wheel contents)
	pytest -p no:cacheprovider tests/databricks

databricks-validate: ## `databricks bundle validate` for dev+prod against a local stub (no account; needs the CLI)
	python databricks/offline_validate.py

databricks-wheel: ## Build the lakehouse wheel the jobs install into dist/
	python -m pip wheel . --no-deps --wheel-dir dist

databricks-deploy: ## Deploy the bundle to YOUR workspace (DBX_TARGET=dev|prod; needs DATABRICKS_HOST + DATABRICKS_TOKEN or a profile)
	cd databricks && $(DATABRICKS) bundle deploy -t $(DBX_TARGET)

databricks-run: ## Run the deployed pipeline job once (BATCH_DATE=all|YYYY-MM-DD)
	cd databricks && $(DATABRICKS) bundle run -t $(DBX_TARGET) $(if $(filter prod,$(DBX_TARGET)),claims_pipeline_cluster,claims_pipeline) --params batch_date=$(or $(BATCH_DATE),all)
