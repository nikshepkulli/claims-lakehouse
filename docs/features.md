# Feature engineering

Ticket #26. Point-in-time-correct member, provider and claim features for the claim denial-risk model ([#27](https://github.com/nikshepkulli/claims-lakehouse/issues/27)), built from the [gold](gold.md) Parquet export. Code: `src/lakehouse/features/`. Catalogue source of truth: `governance/feature_registry.yaml`.

> **What ran and what did not.** Everything here runs locally in plain Python + DuckDB (no Spark, no Java) and in the normal `pytest -m "not spark"` CI job. The data is 100% synthetic. The feature job has been run on a **gold-shaped Parquet export built by `features/fixture.py` from claimsgen output** (a Python stand-in for the Spark gold job), and the export column names/types follow [gold.md](gold.md), but it has **not** been run against the export of a real `lakehouse.gold` (Spark/Delta) run, on S3/ADLS, or in Snowflake/Databricks. The Spark and Snowflake SQL in "Parity path" is documented translation, not executed.

## Point-in-time correctness in one picture

```
 event time ------------------------------------------------------------------------>

   claim A1        claim A2         as_of (scoring moment)        claim A6
   received        received         = midnight of the day         received
      |               |                       |                      |
 -----o---------------o-----------------------|----------------------o------>
      \_____ visible: event_ts <= as_of ______/\__ NOT visible: after as_of __/

   outcome of A2 (PAID/DENIED) becomes known only at  A2.received + outcome_lag_days
   -> A2 counts in volume/billed features at as_of, but in denial/paid features only
      once  outcome_ts <= as_of.

   dim_member:  [ v1 HMO  1900-01-01 .. 2024-03-01 )[ v2 PPO  2024-03-01 .. open )
                 as_of = 2024-02-29 -> v1        as_of = 2024-03-01 -> v2
                 rule: effective_from <= as_of < effective_to
```

Rules, all enforced in `sql.py` and re-implemented independently in `online.py`:

1. **Event time.** A claim becomes visible at the *end* of its receipt day (`event_ts = received_date 23:59:59`). Claim-level rows use `as_of` = the *start* of the receipt day, so a claim never sees itself or its same-day siblings. Member/provider rows use `as_of` = first of each month (a grid; any day works through the same functions).
2. **Windows** are `(as_of - w, as_of]` on `event_ts`, `w` in 30/90/365 days. `*_all history*` features use every claim with `event_ts <= as_of`.
3. **Outcomes are known late.** Gold has no adjudication date, so `FeatureConfig.outcome_lag_days` (default **30**, an assumption, not a measured property of the data) delays outcome-based features (`paid_sum_365d`, `denied_count_365d`, `denial_rate_365d`, provider `denial_rate_365d`): a claim contributes only when `received + lag <= as_of` and its status is PAID or DENIED. Volume and billed features use receipt time only.
4. **SCD2 as-of.** Dimensions are joined by natural id and `effective_from <= as_of < effective_to` (NULL = open), not through the fact's surrogate key (which points at the version valid on the *service* date). More than one matching version fails the build (overlap = upstream bug).
5. **Label separation.** `is_denied` lives in `labels/claim_denial/`, with `label_available_at`. No feature table has a status/paid/label column of the claim being scored, and `store.training_set()` is the only place they meet (it also fails if a label was knowable at its row's `as_of`).

## Feature catalogue

From `governance/feature_registry.yaml` (a test fails if a feature is missing here or in the registry, or if the registry and the written tables disagree). The claim table also carries every member feature as `m_<name>` and every provider feature as `p_<name>`, computed at the claim's own `as_of`. Keys: member `(member_id, as_of)`, provider `(provider_id, as_of)`, claim `(claim_id, member_id, provider_id, as_of)`.

**member**

| Feature | Type | Window | Class. | Outcome | Description |
|---|---|---|---|---|---|
| `claims_30d` | int | 30d | CONFIDENTIAL |  | Claims received by the member in the 30 days up to as_of |
| `claims_90d` | int | 90d | CONFIDENTIAL |  | Claims received by the member in the 90 days up to as_of |
| `claims_365d` | int | 365d | CONFIDENTIAL |  | Claims received by the member in the 365 days up to as_of |
| `billed_sum_90d` | decimal(18,2) | 90d | CONFIDENTIAL |  | Sum of billed amounts of claims received in the trailing 90 days |
| `billed_sum_365d` | decimal(18,2) | 365d | CONFIDENTIAL |  | Sum of billed amounts of claims received in the trailing 365 days |
| `paid_sum_365d` | decimal(18,2) | 365d | CONFIDENTIAL | yes | Sum of paid amounts over trailing-365d claims whose outcome is known at as_of (received + outcome_lag_days <= as_of) |
| `denied_count_365d` | int | 365d | CONFIDENTIAL | yes | Denied claims among trailing-365d claims whose outcome is known at as_of |
| `denial_rate_365d` | double | 365d | CONFIDENTIAL | yes | Denied / adjudicated claims among trailing-365d claims with a known outcome; NULL when none |
| `distinct_providers_365d` | int | 365d | CONFIDENTIAL |  | Distinct providers seen in trailing-365d claims |
| `days_since_last_claim` | int | all history | CONFIDENTIAL |  | Whole days from the latest received claim to as_of; NULL when the member has no claim yet. A relative interval, not a calendar date. |
| `has_diabetes` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in ICD-10 E10-E14 |
| `has_hypertension` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in I10-I16 |
| `has_coronary_artery_disease` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in I20-I25 |
| `has_heart_failure` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in I50 |
| `has_copd` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in J40-J44 |
| `has_asthma` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in J45-J46 |
| `has_ckd` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in N18 |
| `has_depression` | boolean | all history | CONFIDENTIAL |  | Any claim received by as_of has a primary diagnosis in F32-F33 |
| `chronic_condition_count` | int | all history | CONFIDENTIAL |  | Number of has_* chronic-condition flags set |
| `plan_type` | string | as-of | INTERNAL |  | Plan type of the dim_member version valid at as_of (SCD2 as-of join) |
| `state` | string | as-of | CONFIDENTIAL / quasi |  | Member state of the dim_member version valid at as_of |
| `age_band` | string | as-of | CONFIDENTIAL / quasi |  | 5-year age band (90+ top-coded) at as_of from birth_year; the raw birth_year never enters the store |

**provider**

| Feature | Type | Window | Class. | Outcome | Description |
|---|---|---|---|---|---|
| `claims_90d` | int | 90d | INTERNAL |  | Claims received by the provider in the trailing 90 days |
| `claims_365d` | int | 365d | INTERNAL |  | Claims received by the provider in the trailing 365 days |
| `distinct_members_365d` | int | 365d | INTERNAL |  | Distinct members in trailing-365d claims (aggregate count, no member ids exposed) |
| `denial_rate_365d` | double | 365d | INTERNAL | yes | Denied / adjudicated trailing-365d claims with a known outcome; NULL when none |
| `avg_billed_365d` | double | 365d | INTERNAL |  | Mean billed amount per claim over 365d (computed on integer cents, rounded half up) |
| `cost_z_365d` | double | 365d | INTERNAL |  | Peer-normalised cost: z-score of the provider's mean billed per claim vs providers of the same as-of specialty (>= min_peer_claims claims, >= min_peers peers); NULL otherwise |
| `specialty` | string | as-of | INTERNAL |  | Specialty of the dim_provider version valid at as_of |
| `network_status` | string | as-of | INTERNAL |  | Network status valid at as_of |
| `cost_tier` | string | as-of | INTERNAL |  | Cost tier valid at as_of |

**claim**

| Feature | Type | Window | Class. | Outcome | Description |
|---|---|---|---|---|---|
| `billed_amount` | decimal(12,2) | claim | CONFIDENTIAL |  | Billed amount of the claim being scored (known at receipt) |
| `line_count` | smallint | claim | INTERNAL |  | Number of service lines on the claim |
| `claim_type` | string | claim | INTERNAL |  | Claim type |
| `place_of_service` | string | claim | INTERNAL |  | Place-of-service code |
| `service_to_received_lag_days` | int | claim | INTERNAL |  | Days between date of service and receipt (difference of two dates, no calendar date exposed) |
| `primary_dx_chapter` | string | claim | CONFIDENTIAL |  | Leading letter of the primary ICD-10 code (chapter group) |


**Chronic flags** use the *primary* diagnosis only (gold keeps just `primary_diagnosis_sk`), matched with one regex per condition in `chronic.py` (well-formed codes only). **Peer z-score**: provider mean billed per claim over 365d (integer cents, rounded half up) vs all providers of the same *as-of* specialty with >= 5 claims in the window; needs >= 3 peers and non-zero spread, else NULL. All aggregates are exact integer/decimal arithmetic, so results do not depend on row or thread order.

## Registry and PII

`governance/feature_registry.yaml` lists each feature's `name, entity, dtype, window, description, owner, source_columns, transform, classification, identifier, handling, outcome_dependent`, using the vocabulary of [`data_classification.yaml`](privacy-and-access.md). Enforced by `lakehouse.features.registry` and `tests/features/test_registry.py`:

- no feature is `PHI_LIKE` or a direct identifier; ids are join keys listed under `key_columns`, never sources;
- every source column exists in the classification registry; a feature reading a `PHI_LIKE` source (`received_date_key`, `birth_year`) must be derived (aggregate, difference, generalise, flag): `birth_year` only reaches the store as the 5-year `age_band` (top-coded 90+), dates only as relative intervals;
- the only quasi-identifier features are `state` and `age_band`, each with a written justification;
- key columns keep their classification: `member_id`/`provider_id` are direct identifiers (hashed in analyst views), and the claim table's `as_of` equals the receipt date (PHI_LIKE quasi), so the claim table inherits `fact_claim` handling. Drop keys and `as_of` before fitting a model.

When #71 tokenises identifiers in gold, the ids here become tokens with no code change (features read natural ids from the dimensions); that has not been exercised.

## Store layout

```
<out>/feature_store/member_features/as_of_month=2024-03/part-00000.parquet
<out>/feature_store/provider_features/as_of_month=.../
<out>/feature_store/claim_features/as_of_month=.../       (month of the receipt date)
<out>/labels/claim_denial/as_of_month=.../                (claim_id, as_of, is_denied, label_available_at)
<out>/_manifest.json      (config, registry hash, sha256 of each source and output table, guard summary)
```

`<out>` defaults to `data/gold-delta/features`, next to the gold `export/`. Parquet is zstd, one file per month, rows sorted by key. The manifest has no wall-clock field, so the whole directory is byte-reproducible.

```bash
make features                      # gold export -> feature store (fails on leakage)
make features GOLD_EXPORT=... FEATURES=...
make features-demo                 # claimsgen -> gold-shaped export -> store, no Spark (DEMO_ROWS=20000)
make features-verify               # re-read Parquet, compare to the manifest hashes
python -m lakehouse.features --gold-export <gold>/export --out <gold>/features
```

Measured locally (one laptop core, guard on): 20k claims about 9 s, 100k claims (270k member-month rows) about 21 s end to end including claimsgen.

## Leakage guards

`tests/features/` is the heart of the ticket; all of it runs in `pytest -m "not spark"`.

| # | Guard | Where | What it proves |
|---|---|---|---|
| 1 | Future rows never matter | `test_leakage_property.py` (12 seeded random datasets, random `as_of`) | Shuffling inputs, dropping every post-`as_of` row/dimension version, or rewriting them (ids, amounts, status, dx, plan) leaves member, provider and same-day claim features unchanged. Seeded loops, not a property library, so no new dependency and every failure reproduces by seed. |
| 2 | Injected leak is caught | `test_guard.py`, `guards.py` | The build runs `run_leakage_guard`: at probe moments it recomputes all three entities from (a) truncated history and (b) a copy where everything unknowable at `A` is scrambled (outcomes landing after `A`, all post-`A` claims, future dimension versions; own-claim features that are legitimate are left alone) and demands identical output. Tests break the engine four ways (no upper bound, +40 days peek, outcome used before its lag, "current dimension version only") and assert `LeakageError`, a non-zero CLI exit, and that **no store is written**. The property test is also shown to fail under two of those leaks. A label/outcome column in a feature table fails the build (`assert_label_separation`). |
| 3 | Deterministic, idempotent | `test_determinism.py` | Same inputs give the same manifest and byte-identical Parquet in a fresh directory; rebuilding in place changes no byte; shuffled input row order gives the same hashes; tampering with a stored file is caught by `verify`. |
| 4 | Training/serving parity | `test_parity.py`, `online.py` | An independent row-at-a-time Python implementation (no shared SQL) equals the stored batch features for sampled member (60), provider (60, including z-scores) and claim (80) keys, and serves SCD2 versions as-of. Floats within 1e-6, everything else exact. |

Plus hand-computed expectations (`test_pit_hand.py`): window edges (a claim at `as_of - 30d` 23:59:59 is out, the next day is in), the as-of day and future excluded, outcome lag, diagnosis visibility, SCD2 boundary (`effective_from == as_of` picks the new version), exact z-scores (-1, 0, +1), and registry/PII/docs/make wiring (`test_registry.py`).

**What the guards cannot see.** They catch a feature that reads data after `as_of`. They do not catch a wrong *definition* of when something becomes known (the 30-day outcome lag, "visible at the end of the receipt day"), nor leakage that is already inside the gold input (for example a first SCD2 version dated 1900-01-01 makes today's attributes visible for old claims, a property of gold's `EPOCH` rule, see [gold.md](gold.md)).

## How #27 consumes this

- **Training set:** `lakehouse.features.store.training_set(out)` returns the claim features joined to `is_denied` by `claim_id` and `as_of` (Arrow table). Drop `claim_id, member_id, provider_id, as_of` before fitting; they are keys, and `as_of` is a PHI_LIKE date.
- **Split by time, not at random:** order by `as_of` (receipt day) and cut on a date at least `outcome_lag_days` before the end of the data, otherwise the validation labels of the last weeks are not yet known.
- **Batch scoring:** re-run `make features` on new gold; claims scored today need only `claim_features` rows with their `as_of`; write scores back to gold keyed by `claim_id`. Model input columns are exactly `registry.columns("claim")`, which a test ties to the written table, so the model can pin the registry hash from the manifest (`registry_sha256`).
- **Online scoring (design, not built):** `online.OnlineFeatures(claims, dim_member, dim_provider, cfg).claim(claim_id)` / `.member(id, as_of)` / `.provider(id, as_of)` is the serving-side reference that parity tests against; a real service would back it with an online store. Nothing here has served traffic.

## Parity path: Spark and Snowflake SQL

The engine is ordinary SQL over three tables (`src_claims`, `src_dim_member`, `src_dim_provider`) plus a spine. Translation:

| DuckDB (here) | Spark SQL | Snowflake |
|---|---|---|
| `count(*) FILTER (WHERE c)` | `count_if(c)` | `COUNT_IF(c)` |
| `sum(x) FILTER (WHERE c)` | `sum(x) FILTER (WHERE c)` (3.0+) | `SUM(IFF(c, x, NULL))` |
| `as_of - INTERVAL 30 DAY` | `as_of - INTERVAL 30 DAYS` | `DATEADD(day, -30, as_of)` |
| `a // b` (integer division) | `a DIV b` | `FLOOR(a / b)` or `DIV0` |
| `regexp_matches(s, p)` | `s RLIKE p` | `REGEXP_LIKE(s, p)` |
| `bool_or(x)` | `bool_or(x)` | `BOOLOR_AGG(x)` |
| `sha256(string_agg(...))` | `sha2(concat_ws(...), 256)` | `SHA2(LISTAGG(...), 256)` |

The point-in-time conditions (`HIST_COND`, `OUTCOME_COND`, `SCD2_COND` at the top of `sql.py`) are the only lines that decide visibility, so a port reviews three predicates. Hugeint sums are exact; the z-score converts to double only after exact integer arithmetic and is exact below 2^53 (about 9e15 cents-squared, far above this data). Expect doubles to agree to ~1e-6, not bit-for-bit, across engines. A Spark port would partition the history join by `member_id` and run `as_of` as a spine column; **it has not been written or run**.

## Limits, stated plainly

- Synthetic data; the 30-day outcome lag, 365-day windows and chronic regexes are design choices for a demo, not clinical or actuarial claims.
- Primary diagnosis only; secondary diagnoses are not in gold.
- `member_id` etc. are direct identifiers in the synthetic ids; tokenisation is #71 and untested here.
- The guard samples probe moments (default 4, `--probes`); it is a strong check, not a proof for every possible `as_of`.
- Fixture second SCD2 versions (10% of members/providers on 2024-07-01) are synthetic, added by `fixture.py`, because claimsgen emits one snapshot.
- The feature store is not registered in a SageMaker/Feature Store service; it is Parquet files. `store.training_set` is the contract #27 builds on.
