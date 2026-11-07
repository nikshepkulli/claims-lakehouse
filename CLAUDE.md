# CLAUDE.md — claims-lakehouse

Open-source portfolio project by Nikshep Kulli: a multi-cloud lakehouse on synthetic healthcare claims. Target: show Senior Data Engineer skills (Spark/Databricks, Snowflake, AWS+Azure, streaming, IaC, data quality, lineage, ML/GenAI enablement).

## Rules

- Ticket first; PR says `Closes #N`; stacked PRs name their dependency. Plan lives in milestones M0-M7.
- Tests and docs in the same PR as code. No task is done without both.
- All data is synthetic and labelled. Never commit real data, PHI, or credentials.
- Be honest in docs: say what runs locally, what ran on a real cloud, and what did not. Never claim a service was used if the code does not run on it.
- One ADR per architectural choice (docs/adr).
- Run `make lint test` before every commit once the Makefile exists (ticket #2/#3).

## Running log

- 2026-11-05: repo created, 32 tickets across 8 milestones filed; scaffold PR for #1.
- 2026-11-06: #71 keyed tokens. Gold `member_id`/`provider_id` are HMAC tokens `tok_<key_id>_<hex>` (key from `LAKEHOUSE_PSEUDONYM_KEY`; gold CLI fails without it; CI e2e/`make pipeline-e2e` use a fake test key). Registry handling `hash` replaced by `token`; no SHA2 in Snowflake policies. Rotation = rebuild gold from silver (runbook in docs/privacy-and-access.md). Python workers cannot import `lakehouse` in plain `pytest` (sys.path only) - UDFs must stay self-contained (`Tokeniser.token_fn`).
- 2026-11-06: #26 point-in-time feature store (src/lakehouse/features, docs/features.md). Gotcha: gold has no adjudication date, so outcome features use an assumed 30-day lag (FeatureConfig); test file basenames must be unique across tests/ (no __init__.py), e.g. tests/governance/test_registry.py already exists.
- 2026-11-06: #27 ML pipeline (src/lakehouse/ml, docs/ml.md, docs/model-card.md, infra/aws/modules/sagemaker_ml). Gotcha: claimsgen denials are iid 8% noise, so ROC-AUC ~0.5 is the correct demo result; tests plant a label rule to prove the pipeline learns. Nothing ran on real SageMaker; scikit-learn pinned in requirements-ml.txt (pulled into requirements-dev.txt); no pandas needed.
- 2026-11-06: #91 docs/dashboard refresh. `make dbt-build` flags now follow the subcommand (dbt 1.12). Dashboard build runs the gold GE quality CLI on the fixture; exit 1 is expected (fixture lacks gold_reconciliation/gold_exclusions) and the page shows those as missing.
- 2026-11-07: #23 Airflow wiring: bronze/silver/gold tasks run under observability wrap, observability_check task after gold (LAKEHOUSE_ALERTS_FILE moves alert sink). Not run inside Airflow/Spark.
