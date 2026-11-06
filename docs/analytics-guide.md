# Analytics guide: the marts, their grain and how to query them

Ticket #17 (M3). For analysts and BI developers. You query the four `marts.mart_*` tables and never staging, intermediate or raw data. All data is synthetic.

> **What this is built on.** The marts are built by the dbt project in `dbt/` (see [dbt.md](dbt.md)). Today that runs on **DuckDB against a gold-shaped test fixture** made from the data generator, not on the Spark gold layer by default (the gold layer, #8, is merged and `make gold-dbt` builds the same models on its real export; the dashboard and this guide's numbers still come from the fixture) and not on Snowflake (never run). Every number below describes that fixture (5,000 generated claims, seed 42, service year 2024). Where this guide says "defects are excluded", it means the fixture's approximation of the gold quarantine rules, not the real gold job.

## Metric definitions (in code)

Defined once in the marts and mirrored as MetricFlow metrics in `dbt/models/semantic/_semantic_layer.yml` (parse-validated only; nothing here queries MetricFlow).

| Metric | Definition | Where |
|---|---|---|
| **PMPM paid cost** | `paid_amount / member_months` for the service month | `mart_claims_monthly.pmpm_paid`, metric `pmpm_paid_cost` |
| **Denial rate** | `denied_claim_count / claim_count` (claims, not lines or dollars), 0..1 | `denial_rate` in `mart_claims_monthly`, `mart_provider_performance`, `mart_denials`; metric `denial_rate_metric` |
| **Readmission proxy** | A paid institutional claim whose member's previous paid institutional claim was 0 to 30 days earlier (`readmission_window_days` dbt var) | `readmission_proxy_count`; metric `readmission_proxy_total` |

Caveats that apply to all three:

- **PMPM denominator.** `member_months` (`int_member_months`) counts members whose *current* version has `enrollment_date` on or before the last day of the month (null counts as enrolled). A member enrolled mid-month counts as a full member-month, and the synthetic data has no disenrollment. Treat PMPM as an approximation. `member_months` is a monthly denominator: never sum it across breakdowns, which is why `mart_claims_monthly` has month grain only.
- **Readmission proxy is not a clinical measure.** It is computed on service dates only. The data has no discharge dates, admission types, diagnosis grouping or planned-readmission exclusions. Do not report it as a readmission rate. Denied claims are excluded from the chain.
- **Denial rate** is a ratio, so combine groups as `sum(denied) / sum(claims)`, never as an average of rates.

## Caveats common to every mart

- **Synthetic data.** Rates, costs, specialties and plan mixes are generator parameters, not real payer or provider behaviour. Do not infer anything about real populations.
- **Defects excluded, not repaired.** The generator injects defects (`docs/data-generator.md`). The fixture, like gold, deduplicates re-sent claims and removes whole claims that have a null or invalid procedure code, a negative line amount or an orphan member, so header totals equal the sum of their lines. An invalid primary diagnosis code is nulled on the claim rather than dropping the claim. Excluded claims are simply absent from the marts: the fixture keeps 4,864 of the 5,000 generated claims (about 2.7 percent removed), so the marts understate volume by that share and cannot tell you which claims were dropped. dbt singular tests fail the build if any negative amount, header/line mismatch or fan-out reaches a mart.
- **Late arrivals.** About 2 percent of generated claims (89 of 5,000) arrive 31 to 90 days after the service date. They are kept and bucketed by *service month*, so a recent month's cost, PMPM and denial rate can still move after it first looks complete. The marts do not carry `received_date`, so you cannot measure the late-arrival share from the marts; that needs the staging fact (`received_date`) or a mart extension.
- **SCD2 effects.** Member and provider dimensions have history. In the fixture roughly 5 percent of members and 4 percent of providers get a second version on 2024-07-01 (plan type or cost tier rotated). Facts already point at the version valid on the service date, but marts differ in what they show: `mart_member_risk` and `mart_provider_performance` use the **current** version (so a changed member or provider is restated to the latest plan, specialty or tier for all months), while `mart_denials` uses the specialty **as of the service date**. Expect small differences between marts when you slice by the same attribute.
- **Refresh cadence.** There is no scheduler yet (Airflow is ticket #9). Marts are rebuilt on demand with `make dbt-data dbt-build`, and CI rebuilds them on every PR that touches the dbt project. They are full-refresh tables, not incremental. In a real deployment, the intended cadence is a rebuild after each daily gold load; that has not been run anywhere.

## The marts

### `mart_claims_monthly`

| | |
|---|---|
| Grain | one row per service month, all claim types |
| Primary key | `month_key` (YYYYMM); `service_month` is also unique |
| Rows (fixture) | 12 (Jan to Dec 2024) |
| Refresh | full rebuild per `dbt build`; see cadence above |
| For | finance and actuarial-style trend reporting, executive dashboards (headline PMPM, denial rate, cost by claim type) |
| Key columns | `claim_count`, `denied_claim_count`, `member_months`, `paid_amount`, `pmpm_paid`, `denial_rate`, `avg_allowed_per_line`, `*_paid_amount` by claim type, `readmission_proxy_count` |
| Caveats | PMPM denominator above; `active_member_count` is distinct members with a claim that month, not enrolled members; months with no claims have no row; do not sum `member_months` or `active_member_count` across months as if they were members |

### `mart_provider_performance`

| | |
|---|---|
| Grain | one row per `provider_id` x `service_month` (only months with at least one claim) |
| Primary key | (`provider_id`, `service_month`) |
| Rows (fixture) | 1,740 |
| For | network and provider-relations analysts, cost benchmarking, outlier review |
| Key columns | `claim_count`, `allowed_amount`, `paid_amount`, `denial_rate`, `avg_paid_per_claim`, `specialty_cost_index` |
| Caveats | `specialty`, `state_code`, `network_status`, `cost_tier` are the current SCD2 version. `specialty_cost_index` = provider allowed per claim / specialty allowed per claim in the same month, with no case-mix or severity adjustment, so it flags cost, not quality. `unique_members` is distinct within a provider-month and cannot be summed to a distinct count across months. Cells with one or two claims give extreme ratios: filter on volume |

### `mart_member_risk`

| | |
|---|---|
| Grain | one row per `member_id` (current version), including members with no claims |
| Primary key | `member_id` |
| Rows (fixture) | 1,000 |
| For | care-management and population-health analysts building cohorts (high-cost members, utilisation by age or plan) |
| Key columns | `claim_count`, `paid_amount`, `distinct_provider_count`, `readmission_proxy_count`, `risk_score`, `risk_tier` |
| Caveats | `risk_score` is the percent rank of total paid (0..100) and `risk_tier` is HIGH at 90 or above, MEDIUM at 60 or above, else LOW. It is a retrospective cost heuristic, not a clinical or actuarial risk model, and is circular if used to explain cost. `age_years` is measured at the last service date in the data, not today. Whole history is aggregated: there is no time dimension, so you cannot trend a member. `plan_type` is the current plan |

### `mart_denials`

| | |
|---|---|
| Grain | one row per service month x `claim_type` x `provider_specialty` (specialty as of the service date) |
| Primary key | (`service_month`, `claim_type`, `provider_specialty`) |
| Rows (fixture) | 425 |
| For | revenue-cycle and utilisation-management analysts looking at denials |
| Key columns | `claim_count`, `denied_claim_count`, `denial_rate`, `billed_amount`, `denied_billed_amount` |
| Caveats | `denied_billed_amount` is what was billed on denied claims; paid is always 0 on denials. No denial reason or plan dimension exists, so "why" and "by plan" questions need `mart_member_risk` (current plan, approximate). Sparse cells make monthly rates noisy |

`marts.metricflow_time_spine` is a day-grain helper for MetricFlow, not an analysis table.

## Example queries

Ten tested queries live in `analytics/queries/`. Each has a header (question, grain, caveat, expected columns and key columns, expected rows) and reads only `marts.mart_*`.

| File | Question | Mart |
|---|---|---|
| `01_monthly_pmpm_trend.sql` | Monthly PMPM and month-over-month change | `mart_claims_monthly` |
| `02_top_providers_by_allowed.sql` | Top 10 providers by allowed amount, with cost tier | `mart_provider_performance` |
| `03_denial_rate_by_claim_type.sql` | Denial rate and denied billed value by claim type | `mart_denials` |
| `04_denial_rate_by_plan_type.sql` | Denial rate by (current) plan type | `mart_member_risk` |
| `05_high_cost_member_cohort.sql` | HIGH/MEDIUM risk-tier members by plan and their spend share | `mart_member_risk` |
| `06_specialty_benchmark.sql` | Cost per claim and denial rate by specialty | `mart_provider_performance` |
| `07_claim_type_cost_mix.sql` | Monthly paid mix across claim types | `mart_claims_monthly` |
| `08_readmission_proxy_trend.sql` | Readmission proxy per 1,000 claims by month | `mart_claims_monthly` |
| `09_outlier_providers_vs_specialty.sql` | High-volume providers 25 percent above specialty average | `mart_provider_performance` |
| `10_member_utilisation_by_age_band.sql` | Claims and paid per member by age band, incl. non-claimants | `mart_member_risk` |

A "late-arrival share by month" question is deliberately absent: the marts do not carry `received_date` (see caveats).

### Header format

```sql
-- question: ...
-- grain: ...
-- caveat: ...
-- columns: exact, output, column, names, in, order
-- key_columns: columns, that, must, never, be, NULL
-- rows: expected size (free text)
```

A line beginning `--` plus two or more spaces continues the previous field. To add a query, add a file with this header, run it locally, and keep it passing `sqlfluff lint analytics/`.

### Running them

```bash
make dbt-data dbt-build                                  # builds data/claims.duckdb
python scripts/run_analytics_queries.py                   # runs all queries, checks each header
python scripts/run_analytics_queries.py --db other.duckdb
```

For every query the runner checks that the header is complete, that only `marts.mart_*` tables are read, that it executes, returns at least one row, returns exactly the declared columns, and has no NULL in the declared key columns. CI runs it after `dbt build` in `.github/workflows/dbt.yml`. The header parser and the rules are unit-tested in `tests/scripts/test_run_analytics_queries.py` (the parts that need DuckDB skip when it is missing). What the check does **not** prove: that the numbers are right (that is dbt's reconciliation tests), or that the SQL runs on Snowflake (never run; schema `MARTS` is the same name, but only DuckDB is exercised).

### SQL style

`analytics/.sqlfluff` sets dialect `duckdb` with the raw templater (these are plain SQL, no Jinja) and inherits line length, rule exclusions and upper-case keywords from the root `.sqlfluff`. Run `sqlfluff lint analytics/` from the repo root; the nested file overrides the root's `sparksql` dialect (checked: a DuckDB-only `FROM t SELECT a` parses, and fails when forced to `sparksql`).
