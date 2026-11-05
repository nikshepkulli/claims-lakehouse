-- GENERATED from governance/data_classification.yaml by
--   python -m lakehouse.governance.snowflake_sql
-- Do not edit by hand; a test fails if this file drifts from the registry.
--
-- Same as 91_apply_tags_gold.sql but for the dbt marts. dbt rebuilds marts with
-- CREATE OR REPLACE TABLE, which drops column tags (and therefore the masking), so
-- run this script after EVERY dbt build, as SYSADMIN (it inherits the TRANSFORMER
-- role that owns the marts). Until it has run, the marts are UNPROTECTED.
-- NOT EXECUTED against Snowflake.
USE SCHEMA marts;

-- mart_claims_monthly
ALTER TABLE mart_claims_monthly MODIFY COLUMN month_key SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN service_month SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN denied_claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN active_member_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN member_months SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN line_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN billed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN allowed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN professional_paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN institutional_paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN pharmacy_paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN readmission_proxy_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN pmpm_paid SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN denial_rate SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_claims_monthly MODIFY COLUMN avg_allowed_per_line SET TAG secure.data_classification = 'CONFIDENTIAL';

-- mart_provider_performance
ALTER TABLE mart_provider_performance MODIFY COLUMN provider_id SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN provider_id SET TAG secure.privacy_handling = 'HASH';
ALTER TABLE mart_provider_performance MODIFY COLUMN service_month SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN specialty SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN state_code SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN network_status SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN cost_tier SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN denied_claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN unique_members SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN line_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN allowed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN readmission_proxy_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN denial_rate SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN avg_allowed_per_line SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN avg_paid_per_claim SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_provider_performance MODIFY COLUMN specialty_cost_index SET TAG secure.data_classification = 'CONFIDENTIAL';

-- mart_member_risk
ALTER TABLE mart_member_risk MODIFY COLUMN member_id SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE mart_member_risk MODIFY COLUMN member_id SET TAG secure.privacy_handling = 'HASH';
ALTER TABLE mart_member_risk MODIFY COLUMN birth_year SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE mart_member_risk MODIFY COLUMN birth_year SET TAG secure.privacy_handling = 'GENERALISE';
ALTER TABLE mart_member_risk MODIFY COLUMN age_years SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE mart_member_risk MODIFY COLUMN age_years SET TAG secure.privacy_handling = 'GENERALISE';
ALTER TABLE mart_member_risk MODIFY COLUMN sex SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN state_code SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN plan_type SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_member_risk MODIFY COLUMN enrollment_date SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE mart_member_risk MODIFY COLUMN enrollment_date SET TAG secure.privacy_handling = 'GENERALISE';
ALTER TABLE mart_member_risk MODIFY COLUMN last_service_date SET TAG secure.data_classification = 'PHI_LIKE';
ALTER TABLE mart_member_risk MODIFY COLUMN last_service_date SET TAG secure.privacy_handling = 'GENERALISE';
ALTER TABLE mart_member_risk MODIFY COLUMN claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN denied_claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN institutional_claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN readmission_proxy_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN distinct_provider_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN allowed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN paid_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN risk_score SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_member_risk MODIFY COLUMN risk_tier SET TAG secure.data_classification = 'CONFIDENTIAL';

-- mart_denials
ALTER TABLE mart_denials MODIFY COLUMN service_month SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_denials MODIFY COLUMN claim_type SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_denials MODIFY COLUMN provider_specialty SET TAG secure.data_classification = 'INTERNAL';
ALTER TABLE mart_denials MODIFY COLUMN claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_denials MODIFY COLUMN denied_claim_count SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_denials MODIFY COLUMN denial_rate SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_denials MODIFY COLUMN billed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
ALTER TABLE mart_denials MODIFY COLUMN denied_billed_amount SET TAG secure.data_classification = 'CONFIDENTIAL';
