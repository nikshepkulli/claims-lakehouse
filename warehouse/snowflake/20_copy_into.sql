-- COPY INTO the RAW landing tables from the external stage, then record the
-- load in raw.load_audit. ALL DATA IS SYNTHETIC.
--
-- Run as: <PREFIX>_<ENV>_LOADER (e.g. CLAIMS_DEV_LOADER) on its own warehouse.
-- Prerequisite: Terraform created stage RAW.GOLD_STAGE and file format
-- RAW.FF_PARQUET (enable_external_stage = true) and the export job wrote
--   <stage>/<table>/export_date=YYYY-MM-DD/*.parquet
-- NOT YET RUN against a live Snowflake account (ticket #14 remaining work).
--
-- Idempotent re-load:
--   * COPY INTO keeps per-table load metadata for 64 days and skips files it has
--     already loaded, so re-running this script loads nothing twice.
--   * ON_ERROR = ABORT_STATEMENT: a bad file fails the whole statement; nothing
--     partial is committed for that table.
--   * To deliberately reload a batch (or after the 64 days expire), see
--     "Reload a batch" in docs/snowflake.md. The MERGE scripts deduplicate on
--     the business key, so a reload cannot create duplicate gold rows.

USE SCHEMA raw;

COPY INTO landing_dim_date (_source_file, _loaded_at, date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, is_weekend, _batch_id)
FROM (
    SELECT
        metadata$filename AS _source_file,
        metadata$start_scan_time AS _loaded_at,
        $1:date_key::INTEGER,
        $1:full_date::DATE,
        $1:year::SMALLINT,
        $1:quarter::SMALLINT,
        $1:month::SMALLINT,
        $1:month_name::VARCHAR(9),
        $1:day_of_month::SMALLINT,
        $1:day_of_week::SMALLINT,
        $1:is_weekend::BOOLEAN,
        COALESCE(
            REGEXP_SUBSTR(metadata$filename, 'export_date=([0-9-]+)', 1, 1, 'e', 1), 'no_export_date'
        ) AS _batch_id
    FROM @gold_stage/dim_date/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_parquet')
ON_ERROR = ABORT_STATEMENT;

COPY INTO landing_dim_member (_source_file, _loaded_at, member_sk, member_id, birth_year, sex, state, plan_type, enrollment_date, effective_from, effective_to, is_current, record_hash, _batch_id)
FROM (
    SELECT
        metadata$filename AS _source_file,
        metadata$start_scan_time AS _loaded_at,
        $1:member_sk::VARCHAR(64),
        $1:member_id::VARCHAR(10),
        $1:birth_year::SMALLINT,
        $1:sex::VARCHAR(1),
        $1:state::VARCHAR(2),
        $1:plan_type::VARCHAR(20),
        $1:enrollment_date::DATE,
        $1:effective_from::TIMESTAMP_NTZ,
        $1:effective_to::TIMESTAMP_NTZ,
        $1:is_current::BOOLEAN,
        $1:record_hash::VARCHAR(64),
        COALESCE(
            REGEXP_SUBSTR(metadata$filename, 'export_date=([0-9-]+)', 1, 1, 'e', 1), 'no_export_date'
        ) AS _batch_id
    FROM @gold_stage/dim_member/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_parquet')
ON_ERROR = ABORT_STATEMENT;

COPY INTO landing_dim_provider (_source_file, _loaded_at, provider_sk, provider_id, specialty, state, network_status, cost_tier, effective_from, effective_to, is_current, record_hash, _batch_id)
FROM (
    SELECT
        metadata$filename AS _source_file,
        metadata$start_scan_time AS _loaded_at,
        $1:provider_sk::VARCHAR(64),
        $1:provider_id::VARCHAR(10),
        $1:specialty::VARCHAR(60),
        $1:state::VARCHAR(2),
        $1:network_status::VARCHAR(20),
        $1:cost_tier::VARCHAR(12),
        $1:effective_from::TIMESTAMP_NTZ,
        $1:effective_to::TIMESTAMP_NTZ,
        $1:is_current::BOOLEAN,
        $1:record_hash::VARCHAR(64),
        COALESCE(
            REGEXP_SUBSTR(metadata$filename, 'export_date=([0-9-]+)', 1, 1, 'e', 1), 'no_export_date'
        ) AS _batch_id
    FROM @gold_stage/dim_provider/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_parquet')
ON_ERROR = ABORT_STATEMENT;

COPY INTO landing_dim_diagnosis (_source_file, _loaded_at, diagnosis_sk, icd10_code, icd10_chapter, description, _batch_id)
FROM (
    SELECT
        metadata$filename AS _source_file,
        metadata$start_scan_time AS _loaded_at,
        $1:diagnosis_sk::VARCHAR(64),
        $1:icd10_code::VARCHAR(8),
        $1:icd10_chapter::VARCHAR(2),
        $1:description::VARCHAR(255),
        COALESCE(
            REGEXP_SUBSTR(metadata$filename, 'export_date=([0-9-]+)', 1, 1, 'e', 1), 'no_export_date'
        ) AS _batch_id
    FROM @gold_stage/dim_diagnosis/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_parquet')
ON_ERROR = ABORT_STATEMENT;

COPY INTO landing_fact_claim (_source_file, _loaded_at, claim_id, member_sk, provider_sk, primary_diagnosis_sk, service_date_key, received_date_key, claim_type, place_of_service, claim_status, line_count, total_billed_amount, total_allowed_amount, total_paid_amount, _batch_id)
FROM (
    SELECT
        metadata$filename AS _source_file,
        metadata$start_scan_time AS _loaded_at,
        $1:claim_id::VARCHAR(15),
        $1:member_sk::VARCHAR(64),
        $1:provider_sk::VARCHAR(64),
        $1:primary_diagnosis_sk::VARCHAR(64),
        $1:service_date_key::INTEGER,
        $1:received_date_key::INTEGER,
        $1:claim_type::VARCHAR(15),
        $1:place_of_service::VARCHAR(4),
        $1:claim_status::VARCHAR(6),
        $1:line_count::SMALLINT,
        $1:total_billed_amount::NUMBER(12, 2),
        $1:total_allowed_amount::NUMBER(12, 2),
        $1:total_paid_amount::NUMBER(12, 2),
        COALESCE(
            REGEXP_SUBSTR(metadata$filename, 'export_date=([0-9-]+)', 1, 1, 'e', 1), 'no_export_date'
        ) AS _batch_id
    FROM @gold_stage/fact_claim/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_parquet')
ON_ERROR = ABORT_STATEMENT;

COPY INTO landing_fact_claim_line (_source_file, _loaded_at, line_id, claim_id, line_number, procedure_code, units, service_date_key, billed_amount, allowed_amount, paid_amount, _batch_id)
FROM (
    SELECT
        metadata$filename AS _source_file,
        metadata$start_scan_time AS _loaded_at,
        $1:line_id::VARCHAR(20),
        $1:claim_id::VARCHAR(15),
        $1:line_number::SMALLINT,
        $1:procedure_code::VARCHAR(5),
        $1:units::SMALLINT,
        $1:service_date_key::INTEGER,
        $1:billed_amount::NUMBER(12, 2),
        $1:allowed_amount::NUMBER(12, 2),
        $1:paid_amount::NUMBER(12, 2),
        COALESCE(
            REGEXP_SUBSTR(metadata$filename, 'export_date=([0-9-]+)', 1, 1, 'e', 1), 'no_export_date'
        ) AS _batch_id
    FROM @gold_stage/fact_claim_line/
)
FILE_FORMAT = (FORMAT_NAME = 'ff_parquet')
ON_ERROR = ABORT_STATEMENT;

-- Audit: copy this run's file-level results into load_audit. NOT EXISTS makes
-- the insert idempotent. COPY_HISTORY can lag by a few minutes, so a file that
-- is not visible yet is picked up by the next run (window: last 2 days).
INSERT INTO load_audit (
    table_name, file_name, last_load_time, status, rows_parsed, rows_loaded, first_error_message
)
SELECT
    h.table_name,
    h.file_name,
    h.last_load_time,
    h.status,
    h.rows_parsed,
    h.rows_loaded,
    h.first_error_message
FROM (
    SELECT
        table_name,
        file_name,
        last_load_time,
        status,
        row_parsed AS rows_parsed,
        row_count AS rows_loaded,
        first_error_message
    FROM
        TABLE(information_schema.copy_history(
            table_name => 'LANDING_DIM_DATE',
            start_time => DATEADD(DAY, -2, CURRENT_TIMESTAMP())
        ))
    UNION ALL
    SELECT
        table_name,
        file_name,
        last_load_time,
        status,
        row_parsed AS rows_parsed,
        row_count AS rows_loaded,
        first_error_message
    FROM
        TABLE(information_schema.copy_history(
            table_name => 'LANDING_DIM_MEMBER',
            start_time => DATEADD(DAY, -2, CURRENT_TIMESTAMP())
        ))
    UNION ALL
    SELECT
        table_name,
        file_name,
        last_load_time,
        status,
        row_parsed AS rows_parsed,
        row_count AS rows_loaded,
        first_error_message
    FROM
        TABLE(information_schema.copy_history(
            table_name => 'LANDING_DIM_PROVIDER',
            start_time => DATEADD(DAY, -2, CURRENT_TIMESTAMP())
        ))
    UNION ALL
    SELECT
        table_name,
        file_name,
        last_load_time,
        status,
        row_parsed AS rows_parsed,
        row_count AS rows_loaded,
        first_error_message
    FROM
        TABLE(information_schema.copy_history(
            table_name => 'LANDING_DIM_DIAGNOSIS',
            start_time => DATEADD(DAY, -2, CURRENT_TIMESTAMP())
        ))
    UNION ALL
    SELECT
        table_name,
        file_name,
        last_load_time,
        status,
        row_parsed AS rows_parsed,
        row_count AS rows_loaded,
        first_error_message
    FROM
        TABLE(information_schema.copy_history(
            table_name => 'LANDING_FACT_CLAIM',
            start_time => DATEADD(DAY, -2, CURRENT_TIMESTAMP())
        ))
    UNION ALL
    SELECT
        table_name,
        file_name,
        last_load_time,
        status,
        row_parsed AS rows_parsed,
        row_count AS rows_loaded,
        first_error_message
    FROM TABLE(information_schema.copy_history(
        table_name => 'LANDING_FACT_CLAIM_LINE',
        start_time => DATEADD(DAY, -2, CURRENT_TIMESTAMP())
    ))
) AS h
WHERE NOT EXISTS (
    SELECT 1
    FROM load_audit AS a
    WHERE
        a.table_name = h.table_name
        AND a.file_name = h.file_name
        AND a.last_load_time = h.last_load_time
);
