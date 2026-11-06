# CI

Workflow: `.github/workflows/ci.yml`. Runs on every pull request and on pushes to `main`
(ubuntu-latest, Python 3.11, actions pinned to major tags, Terraform pinned to 1.5.7). Each check is its own job so a
failure points at one cause.

| Job | What it runs |
| --- | --- |
| `python lint + format` | `ruff check .` and `ruff format --check .` |
| `pytest` | `make -f mk/quality.mk test` |
| `spark tests (<suite>)` | One parallel job per layer (`bronze`, `silver`, `gold`): `pytest -m spark tests/<suite>` on Java 17 + PySpark/Delta |
| `pipeline e2e` | claimsgen (2000 claims) -> bronze -> silver -> gold on Spark/Delta, then `dbt build --target duckdb` pointed at the **real** gold Parquet export (`--vars gold_path`). Then runs `tests/gold/test_gold_pipeline_spark.py` against the output. The job sets a FAKE, public, test-only `LAKEHOUSE_PSEUDONYM_KEY` (gold refuses to run without one, ticket #71) and checks that gold and the export hold keyed tokens only. Also runs the Great Expectations quality gate per layer (`python -m lakehouse.quality run`, one step after gold; blocking failures fail the job) and uploads the `data-quality-docs` artifact (HTML data docs + result JSON; not published to Pages). Fails if any stage, reconciliation, dbt test, quality gate or check fails. See [gold.md](gold.md), [dbt.md](dbt.md), [data-quality.md](data-quality.md). |
| `sqlfluff` | `sqlfluff lint .` (dialect `sparksql`, see `.sqlfluff`) |
| `discover terraform dirs` | Runs `.github/scripts/tf-discover.sh`; emits the JSON matrix of terraform roots |
| `terraform (<dir>)` | Per directory (Terraform 1.5.7): `terraform fmt -check -recursive`, `init -backend=false`, `validate`. Skipped when no roots are found. |
| `checkov (non-blocking)` | `bridgecrewio/checkov-action` (pinned tag) on `infra/`, `continue-on-error` and `soft_fail`: findings are visible, never block. Becomes blocking (drop both flags) once the baseline is clean. |
| `terraform (gate)` | Single stable check name; passes when terraform succeeded or was skipped because there is no terraform. **Fails** if `infra/` contains any `*.tf` but discovery found no roots (nothing validated). Use this as the required status check, since matrix job names change as `infra/` grows. |

## How terraform discovery works

Roots are every directory matching `infra/*/envs/*` or `infra/*/modules/*` that directly contains
`*.tf` files (e.g. `infra/aws/envs/dev`, `infra/azure/modules/storage`). Modules are validated
standalone via `init -backend=false`. Top-level `infra/aws` or `infra/azure` are not roots, since
they hold no `.tf` themselves and would validate vacuously. A `.tf` file placed anywhere else under
`infra/` trips the gate. Test discovery locally: `cd <tree> && GITHUB_OUTPUT=/dev/stdout /path/to/.github/scripts/tf-discover.sh`.

Dependabot (`.github/dependabot.yml`) updates github-actions, pip and terraform dirs weekly,
grouped, max 3 open PRs per ecosystem.

## Run locally

```sh
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
make -f mk/quality.mk lint test   # fmt target auto-fixes
```

Terraform (per root): `terraform -chdir=infra/aws/envs/dev fmt -check -recursive && terraform -chdir=... init -backend=false && terraform -chdir=... validate`.
Checkov: `pip install checkov && checkov -d infra --framework terraform --soft-fail`.
Individual checks: `ruff check .`, `ruff format --check .`, `pytest`, `sqlfluff lint .`.

Tool versions are pinned in `requirements-dev.txt`; ruff and pytest settings live in
`pyproject.toml`.

## Required status checks on `main`

Branch protection is a repo setting, not code. Require: `python lint + format`, `pytest`,
`sqlfluff`, `terraform (gate)`.

## Deployment (CD)

`terraform-plan.yml` (PR plan comments) and `terraform-apply.yml` (manual, approval-gated) use OIDC and are documented in [cd.md](cd.md). Plan jobs are skipped (green) until the OIDC variables are set. They are not part of the required status checks.

`dbt.yml` (path-filtered, separate from `ci.yml`) builds and tests the dbt project on DuckDB against the gold-shaped test fixture (a fast fallback) and uploads the dbt docs site as an artifact; the real-gold build is the `pipeline e2e` job above. See [dbt.md](dbt.md).

## Dependabot policy

`.github/dependabot.yml` groups updates weekly, with deliberate exceptions:

- **pyspark / delta-spark majors are ignored**: they must match the pinned Spark Docker image used by `spark tests`. Dependabot proposed Spark 4 / Delta 4 and the Spark, lint and sqlfluff jobs failed.
- **numpy / pyarrow majors are ignored**: they can change generator output and compaction behaviour.
- **ruff and sqlfluff form their own group**: lint-rule changes should be reviewed alone, never mixed with runtime bumps.
- **Deploy-path action majors are ignored** (`aws-actions/configure-aws-credentials`, `azure/login`, artifact actions): the OIDC workflows are skipped in CI until a cloud is attached, so majors there are untestable. Bump manually when exercised.
- **Terraform provider majors are ignored**: CI runs `init`/`validate` only, never `plan`, so a provider major (e.g. AWS provider 5 to 6) cannot be verified. Upgrade deliberately with a reviewed plan.
