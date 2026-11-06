# Privacy and access control: classification, masking, row access, control mapping

Ticket #25 (M5, governance); keyed identifier tokens: ticket #71. Code: [`governance/data_classification.yaml`](../governance/data_classification.yaml), `src/lakehouse/governance/`, `warehouse/snowflake/90_*.sql` to `93_*.sql`, `infra/snowflake/modules/lakehouse_warehouse/privacy.tf`.

> **NOT LEGAL ADVICE. NOT A HIPAA COMPLIANCE CLAIM.** Every record in this repo is synthetic (see [data-generator.md](data-generator.md)); there is no PHI, no covered entity, no business associate agreement and no compliance program. The controls are written *as if* the data were real so the design can be reviewed, and the mapping table below says exactly how much of each is built, tested or merely designed. HIPAA applicability and adequacy are for counsel and a qualified assessor.

> **Snowflake status: validated offline only. The masking policies, tags, row access policy and Terraform in this change have NOT been executed against a Snowflake account** (none is attached). What was checked: `sqlfluff lint` (with documented exclusions, below), `terraform fmt` and `validate`, structural pytest, and a DuckDB model of the analyst view. What was not: any statement's behaviour on Snowflake. The first live run is the remaining acceptance work on #25.

## What is verified where

| Layer | What it proves | Runs in |
|---|---|---|
| Registry tests (`tests/governance/test_registry.py`) | Every column of the six gold tables and four marts is classified, nothing extra, rules hold (direct identifiers never shown raw, sensitive "as is" columns carry a justification) | plain `pytest` |
| Pseudonymisation (`tests/governance/test_tokenise.py`, `test_generalise_scrub.py`) | Determinism, different keys give different tokens, enumerating the sequential id space (with SHA-256/MD5/SHA-1 and domain variants) never hits a keyed token, key ids and rotation (old-version tokens stay verifiable while new writes use the new key), no raw id substring survives, referential integrity (including orphans staying orphans), scrubber removes the generator's phone-like strings | plain `pytest` (pure Python and pyarrow, no Java) |
| Keyed gold fixture (`tests/governance/test_keyed_gold_fixture.py`) | Every cell of every gold-shaped table: no source id, no unkeyed MD5/SHA-256 of an id; `member_id`/`provider_id` are tokens; surrogate keys are derived from the token; same key gives identical output across runs; different keys give different tokens and surrogate keys | plain `pytest` (needs DuckDB, skipped without it) |
| Keyed Spark gold (`tests/gold/test_gold_keyed_spark.py`, plus two checks in `test_gold_pipeline_spark.py` on the real pipeline output) | The same on the **Spark** gold: idempotent re-run with the same key, key change, no source id in `gold_exclusions.reason_detail`, rotation rebuild writes only the new key id, missing key fails before anything is written | `-m spark` (CI `spark tests (gold)` and `pipeline e2e`) |
| Snowflake code structure (`tests/governance/test_snowflake_enforcement.py`) | Policies exist, raw values only for `TRANSFORMER`, generated tag scripts equal the registry, Terraform mirror equals the SQL bodies | plain `pytest` |
| Analyst view on DuckDB (`tests/dbt/test_analyst_masking.py`) | Scans every cell of every analyst view over the gold fixture: no raw `member_id` / `provider_id`, keyed-token ids still join, quasi-identifiers generalised, dropped columns NULL | `dbt` workflow (needs DuckDB, skipped in the plain job) |
| Snowflake policies behaving as written | **Nothing yet** | needs a live account |

The DuckDB test models the `ANALYST` role's view in DuckDB (which has no roles); it is evidence the *rules* leak nothing, not that Snowflake enforces them.

## Threat model

Protected assets: member and provider identifiers, birth year and dates (quasi-identifiers), claim-level health and payment detail, free-text notes.

| Threat actor | Can do | Protected by | Residual risk |
|---|---|---|---|
| Analyst (legitimate user) browsing `MARTS` | `SELECT` on marts and secure objects | Tag-based masking: ids hashed, birth year to 5-year bucket, dates to year, `record_hash` NULL; row access policy example limits states | Dates of service, state, sex and plan type remain (limited-data-set style); re-identification by linkage is **not measured** (no k-anonymity or small-cell analysis) |
| Analyst with stolen credentials | Same as analyst | Same; analyst cannot see `RAW`/`STAGING`, has no write privileges, has a 5-minute statement timeout | MFA and network policy are designed, not built |
| Loader service | Write `RAW` | Cannot read `MARTS`/`SECURE`; no `GRANT`, no ownership | Loader sees raw landing data by design |
| Transformer service / engineer | Read `RAW`, write `STAGING`/`MARTS`/`SECURE` | Needs raw identifiers to `MERGE` and build marts, so it is the one role that sees them; not given to people by default | Highest-privilege data role; `SYSADMIN` inherits it and so sees raw too |
| Someone with direct bucket access | Read parquet | SSE-KMS/CMK, public access blocked, TLS-only, role-scoped IAM or Entra RBAC ([infra-aws.md](infra-aws.md), [infra-azure.md](infra-azure.md)) | Bronze and silver hold raw ids (the gold job needs them to tokenise); **gold and its export hold keyed tokens only** (#71). Whoever can read both silver and the pseudonym key can re-link |
| Offline attacker with an analyst export | Brute-forces identifiers | Gold stores keyed HMAC-SHA256 tokens (`tok_<key_id>_<hex>`), never the source id, and nothing in Snowflake hashes an id any more (#71). Sequential ids (`M000000001`) are enumerable, tokens are not without the key (tested) | The key is the single secret: anyone holding it can token-map the whole id space. It travels inside the Spark task closure (see limitations). Tokens are stable, so linkage *between* tables sharing a key remains possible by design |
| Note reader | Reads free text | Scrubber removes phones, emails, SSN-like, ids, dates | Regex only: names, addresses and ages in prose (`16-year-old`) are not detected; scrubbed text is still sensitive |
| Dropped-tag surprise | dbt rebuild recreates a mart with `CREATE OR REPLACE`, dropping column tags | Run `92_apply_tags_marts.sql` after every dbt build | Between the build and the script the mart is **unprotected**; a post-hook or scheduled tag check would close it (not built) |

Out of scope: real PHI handling, BAAs, breach notification, workforce training, physical safeguards of a data centre (inherited from the cloud provider).

## Classification registry

`governance/data_classification.yaml` is the single source of truth. Each column gets a **classification** (`PUBLIC`, `INTERNAL`, `CONFIDENTIAL`, `PHI_LIKE`), an **identifier** type (`direct`, `quasi`, `none`) and a **handling** for readers without the unmasked privilege (`none`, `token`, `mask`, `generalise`, `drop`). `PHI_LIKE` means "identifies a person or is an identifying attribute of one"; the suffix is deliberate because the data is synthetic. `token` means the stored value is already a keyed pseudonym made by the gold job: every role sees it as stored (it is stable, so joins work) and no Snowflake policy touches it. There is deliberately **no** unkeyed `hash` handling any more. A sensitive column left as is (`handling: none`) or marked `token` must carry a written justification.

Highlights: `member_id` and `provider_id` have handling `token` (the stored value is already a keyed HMAC token); `birth_year`, `age_years`, `enrollment_date` and `last_service_date` are generalised; `dim_member.record_hash` is dropped because it is an unkeyed md5 over `(birth_year, sex, state, plan_type)`, which is brute-forceable back to birth year; surrogate keys are kept (opaque, needed for joins); see [Key decisions](#key-decisions-per-column-ticket-71).

**Drift guard.** `tests/governance/test_registry.py` parses `01_gold_ddl.sql` and the dbt mart YAML and fails if a column exists there but not in the registry (or the reverse). The analyst-view builder raises `KeyError` on an unclassified column too, so a new column cannot pass through unmasked. Adding a column therefore takes four steps: DDL or dbt YAML, registry entry, `python -m lakehouse.governance.snowflake_sql`, commit.

## Snowflake enforcement (unexecuted)

Requires **Enterprise Edition or higher** (tags, masking and row access policies are not in Standard); a trial account can be created as Enterprise.

| Object | Where | Notes |
|---|---|---|
| Tags `DATA_CLASSIFICATION`, `PRIVACY_HANDLING` | `90_masking_policies.sql` (or `privacy.tf`) in `SECURE` | First is for discovery and audit; the second (`MASK`, `GENERALISE`, `DROP`) carries the masking policies. `token` columns get only the first tag |
| Masking policies `MP_PRIVACY_VARCHAR`, `_NUMBER`, `_DATE` | same | One per data type, bound to `PRIVACY_HANDLING`; body branches on the column's tag value. Raw only for `CLAIMS_DEV_TRANSFORMER`; no `ELSE`, so unknown values and `DROP` return NULL (fail closed). No `SHA2`/`MD5` branch exists (#71) |
| Column tags | `91_apply_tags_gold.sql`, `92_apply_tags_marts.sql` | **Generated** from the registry; a test fails if they drift |
| Row access policy `RAP_STATE` + `SECURE.STATE_ENTITLEMENT` | `93_row_access_example.sql` | Opt-in example on `dim_member.state` and `dim_provider.state`. Empty entitlement table means analysts see zero rows (fail closed). Hiding dimension rows also drops joined fact rows, which is the intent of a region restriction |
| Terraform mirror | `privacy.tf`, `manage_privacy_policies = false` by default | Provider `snowflakedb/snowflake ~> 2.22` has `snowflake_tag` (with `masking_policies`) and `snowflake_masking_policy`, so tags and policies can be Terraform-owned. **Mutually exclusive with `90_masking_policies.sql`.** Terraform cannot tag columns (tables come from SQL after it) and the row access policy reads a SQL-created table, so those stay in SQL. A test keeps both sets of policy bodies identical |

### Apply order

1. `terraform apply` (roles, schemas, grants; [snowflake.md](snowflake.md)). Optional: `-var manage_privacy_policies=true` instead of step 3.
2. `01_gold_ddl.sql`, then load and merge as `TRANSFORMER` (raw values pass through the policies).
3. `90_masking_policies.sql` as `SYSADMIN` (skip if Terraform owns it).
4. `91_apply_tags_gold.sql` as `SYSADMIN`.
5. `dbt build` as `TRANSFORMER`, then **`92_apply_tags_marts.sql`** (every time).
6. Optional: `93_row_access_example.sql`, then insert entitlements through a reviewed PR.
7. Verify as `CLAIMS_DEV_ANALYST`: `SELECT member_id, birth_year, enrollment_date FROM marts.dim_member LIMIT 5;` must show a `tok_<key_id>_<32 hex>` token (the same under every role: the value is stored tokenised), a multiple of 5, and a January 1 date; the same query as `TRANSFORMER` shows the raw birth year and enrollment date. Record the result in #25.

Linter note: sqlfluff 4.5 parses the masking and row access policies but still wants off-style layout and capitalisation for them (`AS(`, lower-case types), so those statements sit inside `-- noqa: disable=` blocks (layout and capitalisation rules, plus `RF03` for the row access policy, whose `row_state` argument cannot be table-qualified). `ALTER TAG ... SET MASKING POLICY ... FORCE` is valid Snowflake but not parsed by 4.5, so those three statements have a narrow `PRS` suppression. The rest of each file is linted. Their structure is covered by `tests/governance` instead.

### Known risks for the first live run

- `SYSTEM$GET_TAG_ON_CURRENT_COLUMN` argument form (fully qualified, upper case, used here) and the `ALTER TAG ... SET MASKING POLICY ... FORCE` syntax are from documentation, not from a run.
- Policy `NUMBER` / `DATE` signatures must match column types (`SMALLINT` is `NUMBER(38,0)`); a mismatch silently leaves a column unprotected, so check with `INFORMATION_SCHEMA.POLICY_REFERENCES` / `TAG_REFERENCES` that every tagged column shows a policy.
- Managed-access schemas: tagging and policy application need the right owner; the scripts assume `SYSADMIN` (it inherits `TRANSFORMER`, which owns dbt-built marts).
- `IS_ROLE_IN_SESSION` honours the role hierarchy, so `SYSADMIN` and `ACCOUNTADMIN` see raw values through inheritance.
- Masked `state`/`sex` joins and policy performance on large tables are unmeasured.

## Portable enforcement (verified)

`src/lakehouse/governance/`, pure Python and pyarrow so it runs without Java:

- `tokenise.Tokeniser`: HMAC-SHA256 over `domain || 0x00 || value`, 128 bits, `tok_<key_id>_<32 hex>` (e.g. `tok_v1_3f9a...`, at most 45 chars). Key from `LAKEHOUSE_PSEUDONYM_KEY` (at least 32 chars), id from `LAKEHOUSE_PSEUDONYM_KEY_ID` (default `v1`), never in the repo and redacted from `repr`. Same key and domain gives the same token in every table (joins and orphan detection survive); a different key changes every token; domains stop cross-type linking. `tokenise.KeyRing` adds retired keys (`LAKEHOUSE_PSEUDONYM_RETIRED_KEYS`, a JSON object `{"v1": "<key>"}`, verify-only) so tokens of older generations stay checkable. **Wired into the gold job (#71)**: `lakehouse.gold.keys.natural_id` tokenises `member_id`/`provider_id` in every dimension, the fact join and the exclusions detail with a Python UDF built from `Tokeniser.token_fn` (a self-contained closure, so Spark workers need nothing but the stdlib).
- `generalise`: 5-year buckets (`1987` to `1985`, same arithmetic as the Snowflake policy and the DuckDB view), age bands top-coded at 90+, date to year.
- `scrub.scrub_text`: phones, emails, SSN-like, member/provider ids (tokenised to match the structured columns when a tokeniser is passed), dates, bare 10-digit numbers.
- `views.analyst_view_sql`: renders the analyst view for a table from the registry (DuckDB dialect).

## Key decisions per column (ticket #71)

Every identifier-like column in gold was decided on purpose; the registry carries the same text.

| Column | Decision | Why |
|---|---|---|
| `dim_member.member_id`, `dim_provider.provider_id` (and mart copies) | **Keyed HMAC token** `tok_<key_id>_<hex>` made by the gold job | Source ids are sequential, so any unkeyed hash is reversible by enumeration |
| `member_sk`, `provider_sk` (dims and `fact_claim`) | `sha256("<kind>:v1" \| token \| effective_from)` | Derived from the keyed token, so not enumerable without the key; opaque; deterministic so facts need no lookup. **Changes on key rotation** (every SK moves: rebuild gold, below) |
| `diagnosis_sk` | Unkeyed `sha256("diagnosis:v1" \| icd10_code)` | Hash of a public ICD-10 code: not an identifier, nothing to protect, and it stays stable across rotations |
| `dim_member.record_hash`, `dim_provider.record_hash` | Unkeyed `sha256` of the tracked attributes, kept | Change detection for SCD2. Low-entropy input (birth year, sex, state, plan) so it is brute-forceable: **dropped for analysts** (member) and never used as an id. Deliberately key-independent so rotation does not churn SCD2 history |
| `claim_id`, `line_id` | Kept as is | Synthetic opaque event ids, not person identifiers; classified CONFIDENTIAL/quasi with a justification. In real data they would need review (see limitations) |
| Snowflake `HASH()` in `30/31_merge_*.sql` | Kept | Row change detection inside a `MERGE` only; the value is never stored or returned |

## Key management and rotation runbook

**Supply the key.** `LAKEHOUSE_PSEUDONYM_KEY` (the active key, at least 32 chars), `LAKEHOUSE_PSEUDONYM_KEY_ID` (`[a-z0-9]{1,8}`, default `v1`; it is embedded in every token) and, during a rotation, `LAKEHOUSE_PSEUDONYM_RETIRED_KEYS` (`{"v1": "<old key>"}`). In production these come from the secret store of the platform (AWS Secrets Manager / Azure Key Vault injected as environment variables); this repo holds no key and no secret reference. Tests use clearly fake keys named `*-TEST-ONLY-*` / `*-NOT-A-SECRET-*`; `make pipeline-e2e` and the `pipeline e2e` CI job use such a key because their data is synthetic and throwaway. The gold CLI exits with `MissingKeyError` before writing anything when the key is absent or shorter than 32 bytes.

**Why rotation is a rebuild, not an update.** HMAC cannot be reversed, so old tokens cannot be converted to new ones. Silver keeps the raw ids, so gold is rebuilt from it. The surrogate keys of member and provider are derived from the token and move too, which is why facts are rebuilt as well.

1. Generate a new key (`openssl rand -base64 48`), store it in the secret store under a new id (`v2`). Keep the old key available as `v1`.
2. Deploy the job environment with `LAKEHOUSE_PSEUDONYM_KEY=<v2>`, `LAKEHOUSE_PSEUDONYM_KEY_ID=v2`, `LAKEHOUSE_PSEUDONYM_RETIRED_KEYS={"v1": "<v1>"}`. New writes now carry `tok_v2_...`; anything still holding `tok_v1_...` can be checked with `KeyRing.verify` (tested).
3. Rebuild gold from silver into a **new** gold root: `python -m lakehouse.gold --silver <silver> --gold <gold-v2> --batch-date all`. Reconciliation runs as usual. (Rebuilding into the old root would leave stale v1 dimension versions next to v2 ones.)
4. Reload Snowflake from `<gold-v2>/export` into empty tables (or truncate and `COPY INTO`), then run `dbt build` and `92_apply_tags_marts.sql`. Dashboards and saved queries keyed on old tokens or SKs break at this point: announce the switch.
5. Check the old generation is gone: `key_id_counts(...)` on the `member_id` column of the new gold must show only `v2` (a test does this on a rebuild). Then delete the old gold root and remove `v1` from `LAKEHOUSE_PSEUDONYM_RETIRED_KEYS` and the secret store.
6. Emergency rotation (suspected key leak): do steps 1-5 immediately, and treat every export made under the old key as exposed, because tokens made with a leaked key are reversible by enumeration.

Not exercised: a real secret store, a real cluster, a Snowflake reload. What is tested is the rotation behaviour of the library and of the Spark gold on local data (`test_old_version_tokens_stay_verifiable...`, `test_rotation_old_tokens_verify_and_the_rebuild_uses_only_the_new_key`).

## Known limitations of the keyed tokens

| Limitation | Effect | Status |
|---|---|---|
| The key is shipped inside the Spark task closure (no HMAC primitive in Spark SQL) | Anyone who can read job definitions or task binaries on the cluster can read the key | Documented; mitigation (read the key from a secret scope on executors) not built; not run on a real cluster |
| One key for all members/providers | Compromise of the key de-pseudonymises the whole id space; there is no per-tenant key | Accepted for a synthetic portfolio project |
| Tokens are stable across tables and runs | Linkage by token between datasets that share a key is possible by design (joins need it) | By design |
| `claim_id` / `line_id` are not tokenised | Opaque synthetic ids today; with real claim numbers they could be linkable to source systems | Open: review before real data |
| Quasi-identifiers (birth year, state, dates of service) | Re-identification by linkage is not measured | Open (see threat model) |
| `record_hash` and `diagnosis_sk` are unkeyed hashes | Deliberate, see the decision table | Documented |
| Rotation changes all member/provider SKs | Downstream consumers must reload | Runbook above |
| Snowflake never executed | Tag/policy changes are validated offline only (sqlfluff, structural tests, DuckDB model of the analyst view) | Same as #25 |

## Access matrix

Roles are from [snowflake.md](snowflake.md). "Masked" means the policy output for non-`TRANSFORMER` roles. `PII_READER` is a break-glass role that is **designed only** (not in Terraform, granted to nobody).

| Dataset | Columns | `LOADER` | `TRANSFORMER` | `ANALYST` | `PII_READER` (designed) |
|---|---|---|---|---|---|
| `RAW.landing_*`, `load_audit` | all; ids are already tokens (landing is a copy of the gold export) | read/write | read | none | none |
| `STAGING` | dbt staging views | none | read/write | none | none |
| `MARTS` gold dims | `member_id`, `provider_id` (keyed tokens since #71) | none | token, read/write | token (same value) | token |
| | `birth_year` | none | raw | 5-year bucket start | raw |
| | `enrollment_date` | none | raw | January 1 of the year | raw |
| | `record_hash` (member) | none | raw | NULL | raw |
| | `state`, `sex`, `plan_type`, specialty, SCD2 and load columns | none | read/write | as stored (row access policy may filter rows by state) | as stored |
| `MARTS` facts | claim and line ids, surrogate keys (derived from the tokens), service/received date keys, codes, amounts | none | read/write | as stored (opaque keys; day-grain dates retained as limited-data-set style) | as stored |
| dbt marts | `member_id`, `provider_id` (tokens, carried through from gold) | none | token | token | token |
| | `birth_year`, `age_years`, `enrollment_date`, `last_service_date` | none | raw | generalised | raw |
| | counts, amounts, rates, risk score and tier | none | read/write | as stored | as stored |
| `SECURE` | views and entitlement table | none | read/write | read | read |
| Lake storage (S3/ADLS) | parquet in bronze/silver (raw ids) and gold (tokens) | write bronze | read/write silver, read gold | read gold only | none |

Notes (bronze free text) live in the lake, not Snowflake; they are readable only by lake roles above. Scrubbing them before they reach any analyst-facing store is a design rule, not wired in yet.

## HIPAA Security Rule control mapping

Safeguard categories and standards are those of 45 CFR 164.308 (administrative), 164.310 (physical) and 164.312 (technical). This is a **mapping of repo controls to categories for discussion**, not a statement that any requirement is met.

Status: **implemented-and-tested** = runs in CI here; **implemented-not-executed** = code exists and is validated offline, never run on a real cloud account; **designed-only** = described, not built.

| Safeguard | Category / standard | Control in this repo | Status |
|---|---|---|---|
| Technical | Access control: unique user identification and role-based access (164.312(a)(1)) | Functional/access role model, no grants to `PUBLIC`, analyst read-only ([snowflake.md](snowflake.md)); AWS IAM roles and Azure RBAC per layer | implemented-not-executed (static checks and tests pass offline) |
| Technical | Access control: minimum necessary view of identifiers | Tag-based masking by role; registry-driven handling; row access policy example | implemented-not-executed (policies); implemented-and-tested (the DuckDB model of the analyst view and the registry rules) |
| Technical | Access control: encryption and decryption (addressable) | SSE-KMS with CMK and rotation (AWS), double encryption and optional CMK (Azure) | implemented-not-executed |
| Technical | Access control: emergency access, automatic logoff | Break-glass `PII_READER`; session timeouts | designed-only (analyst statement timeout is implemented-not-executed) |
| Technical | Audit controls (164.312(b)) | S3 server access logs, Azure diagnostic settings; `load_audit` table; Snowflake `ACCESS_HISTORY` / `TAG_REFERENCES` review queries | implemented-not-executed (logging); designed-only (review queries, alerting) |
| Technical | Integrity (164.312(c)) | dbt tests and reconcile queries, idempotent `MERGE` with content hash, S3 versioning, ADLS soft delete | implemented-and-tested (dbt on DuckDB fixture, structural tests); versioning implemented-not-executed |
| Technical | Person or entity authentication (164.312(d)) | Snowflake key-pair auth from environment variables, no passwords or access keys in code, OIDC for CI, Entra-only storage auth | implemented-not-executed; MFA and network policies designed-only |
| Technical | Transmission security (164.312(e)) | Bucket policy denies non-TLS, Azure TLS 1.2 and HTTPS-only; Snowflake is TLS by default | implemented-not-executed |
| Administrative | Security management: risk analysis (164.308(a)(1)) | Threat model above | designed-only (informal, not a formal risk analysis) |
| Administrative | Information access management (164.308(a)(4)) | Registry with justifications, PR-reviewed entitlements, ticket-first workflow | implemented-and-tested (registry rules and drift guard); process controls are repo conventions |
| Administrative | Evaluation (164.308(a)(8)) | CI runs registry, masking and structural tests on every PR | implemented-and-tested |
| Administrative | Workforce training, contingency plan, BAAs | none | designed-only / out of scope |
| Physical | Facility, workstation and media controls (164.310) | Inherited from the cloud provider; nothing in this repo | out of scope (inherited) |
| Privacy Rule (not Security Rule) | De-identification (164.514(b)) | Generalised birth year and dates, keyed-token ids, scrubbed notes are in the spirit of Safe Harbor; day-grain dates of service, state and claim ids are retained, so the output is **not** claimed to be de-identified | implemented-and-tested (rules); not a de-identification claim |

## Decisions

- **Registry first, enforcement generated.** One reviewed file drives the tag scripts, the analyst views and the tests, so policy and documentation cannot disagree silently.
- **Tag-based masking over per-column policies.** Tagging a column is the protection step; a new column cannot be forgotten once the registry test forces a decision.
- **SQL scripts as default, Terraform as opt-in mirror.** The scripts work with any role setup and are what the runbook uses; the mirror proves the provider supports the objects, and a test prevents drift.
- **Raw only for `TRANSFORMER`.** Narrowest role that functionally needs raw ids (`MERGE` compares them). A named break-glass role is preferable to widening it, but is deferred.
- **Tokenise before load, not in Snowflake.** The key stays out of the warehouse; Snowflake only ever sees pseudonyms, so there is nothing for an in-warehouse `SHA2` to protect and none is used (#71). Trade-off: the transformer role no longer sees raw ids either; relinking needs silver plus the key.
- **Honesty over coverage.** Unmeasured re-identification risk, the dbt tag-drop window and zero live execution are listed above instead of hidden.
