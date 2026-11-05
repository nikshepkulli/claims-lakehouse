# lakehouse_warehouse (Snowflake)

Database, four schemas (RAW, STAGING, MARTS, SECURE), three warehouses with
resource monitors, a least-privilege role hierarchy, and an optional S3
storage integration + external stage for the gold Parquet export.

Not applied by CI and **not yet applied to a live Snowflake account**.
Design, role diagram, cost guardrails and the trial runbook are in
[docs/snowflake.md](../../../../docs/snowflake.md).

Credentials come from `SNOWFLAKE_*` environment variables (key-pair auth);
nothing secret is in code. Offline checks:
`terraform fmt -check && terraform init -backend=false && terraform validate`
and `bash infra/snowflake/tests/static_checks.sh`.
