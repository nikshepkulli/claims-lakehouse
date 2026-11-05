-- GENERATED from governance/data_classification.yaml by
--   python -m lakehouse.governance.snowflake_sql
-- Do not edit by hand; a test fails if this file drifts from the registry.
--
-- Tags every column of the gold tables with its classification and, where the
-- registry says so, the privacy handling that the tag-based masking policies
-- (90_masking_policies.sql) enforce. Run as SYSADMIN (owns the tables and the tags)
-- after 01_gold_ddl.sql and 90_masking_policies.sql, database selected by the
-- connection. Idempotent: SET TAG overwrites. NOT EXECUTED against Snowflake.
USE SCHEMA marts;

-- dim_date
ALTER TABLE dim_date MODIFY COLUMN date_key SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN full_date SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN year SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN quarter SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN month SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN month_name SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN day_of_month SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN day_of_week SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_date MODIFY COLUMN is_weekend SET TAG secure.data_classification = 'PUBLIC';

-- dim_member
ALTER TABLE dim_member MODIFY COLUMN member_sk SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_member MODIFY COLUMN member_id SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE dim_member MODIFY COLUMN member_id SET TAG secure.privacy_handling = 'HASH';
ALTER TABLE dim_member MODIFY COLUMN birth_year SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE dim_member MODIFY COLUMN birth_year SET TAG secure.privacy_handling = 'GENERALISE';
ALTER TABLE dim_member MODIFY COLUMN sex SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE dim_member MODIFY COLUMN state SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE dim_member MODIFY COLUMN plan_type SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_member MODIFY COLUMN enrollment_date SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE dim_member MODIFY COLUMN enrollment_date SET TAG secure.privacy_handling = 'GENERALISE';
ALTER TABLE dim_member MODIFY COLUMN effective_from SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_member MODIFY COLUMN effective_to SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_member MODIFY COLUMN is_current SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_member MODIFY COLUMN record_hash SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE dim_member MODIFY COLUMN record_hash SET TAG secure.privacy_handling = 'DROP';
ALTER TABLE dim_member MODIFY COLUMN load_batch_id SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_member MODIFY COLUMN loaded_at SET TAG secure.data_classification = 'INTERNAL';

-- dim_provider
ALTER TABLE dim_provider MODIFY COLUMN provider_sk SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN provider_id SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE dim_provider MODIFY COLUMN provider_id SET TAG secure.privacy_handling = 'HASH';
ALTER TABLE dim_provider MODIFY COLUMN specialty SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN state SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN network_status SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN cost_tier SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN effective_from SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN effective_to SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN is_current SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN record_hash SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN load_batch_id SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_provider MODIFY COLUMN loaded_at SET TAG secure.data_classification = 'INTERNAL';

-- dim_diagnosis
ALTER TABLE dim_diagnosis MODIFY COLUMN diagnosis_sk SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_diagnosis MODIFY COLUMN icd10_code SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_diagnosis MODIFY COLUMN icd10_chapter SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_diagnosis MODIFY COLUMN description SET TAG secure.data_classification = 'PUBLIC';
ALTER TABLE dim_diagnosis MODIFY COLUMN load_batch_id SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE dim_diagnosis MODIFY COLUMN loaded_at SET TAG secure.data_classification = 'INTERNAL';

-- fact_claim
ALTER TABLE fact_claim MODIFY COLUMN claim_id SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim MODIFY COLUMN member_sk SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN provider_sk SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN primary_diagnosis_sk SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim MODIFY COLUMN service_date_key SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE fact_claim MODIFY COLUMN received_date_key SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE fact_claim MODIFY COLUMN claim_type SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN place_of_service SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN claim_status SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN line_count SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN total_billed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim MODIFY COLUMN total_allowed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim MODIFY COLUMN total_paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim MODIFY COLUMN load_batch_id SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim MODIFY COLUMN loaded_at SET TAG secure.data_classification = 'INTERNAL';

-- fact_claim_line
ALTER TABLE fact_claim_line MODIFY COLUMN line_id SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim_line MODIFY COLUMN claim_id SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim_line MODIFY COLUMN line_number SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim_line MODIFY COLUMN procedure_code SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim_line MODIFY COLUMN units SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim_line MODIFY COLUMN service_date_key SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE fact_claim_line MODIFY COLUMN billed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim_line MODIFY COLUMN allowed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim_line MODIFY COLUMN paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE fact_claim_line MODIFY COLUMN load_batch_id SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE fact_claim_line MODIFY COLUMN loaded_at SET TAG secure.data_classification = 'INTERNAL';
