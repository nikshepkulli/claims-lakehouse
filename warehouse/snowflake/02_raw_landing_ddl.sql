-- RAW landing tables and load audit. ALL DATA IS SYNTHETIC.
-- Landing tables mirror the gold export 1:1 (all columns nullable: validation
-- happens at MERGE time and upstream in Great Expectations #22) plus lineage
-- columns. They are append-only; the MERGE scripts deduplicate.
--
-- Run as: SYSADMIN, database from the connection. Idempotent.

USE SCHEMA raw;

CREATE TABLE IF NOT EXISTS load_audit (
    table_name VARCHAR(64) NOT NULL,
    file_name VARCHAR(1024) NOT NULL,
    last_load_time TIMESTAMP_LTZ NOT NULL,
    status VARCHAR(40) NOT NULL,
    rows_parsed NUMBER,
    rows_loaded NUMBER,
    first_error_message VARCHAR(4000),
    recorded_at TIMESTAMP_LTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_load_audit PRIMARY KEY (table_name, file_name, last_load_time)
)
COMMENT = 'One row per file per COPY INTO, copied from INFORMATION_SCHEMA.COPY_HISTORY by 20_copy_into.sql. Idempotent on (table_name, file_name, last_load_time). Synthetic-data project.';

CREATE TABLE IF NOT EXISTS landing_dim_date (
    date_key INTEGER,
    full_date DATE,
    year SMALLINT,
    quarter SMALLINT,
    month SMALLINT,
    month_name VARCHAR(9),
    day_of_month SMALLINT,
    day_of_week SMALLINT,
    is_weekend BOOLEAN,
    _source_file VARCHAR(1024),
    _batch_id VARCHAR(64),
    _loaded_at TIMESTAMP_LTZ
)
COMMENT = 'Append-only landing copy of gold dim_date Parquet export. Deduplicated by the MERGE in 3x_merge scripts. Synthetic data.';

CREATE TABLE IF NOT EXISTS landing_dim_member (
    member_sk VARCHAR(64),
    member_id VARCHAR(48),
    birth_year SMALLINT,
    sex VARCHAR(1),
    state VARCHAR(2),
    plan_type VARCHAR(20),
    enrollment_date DATE,
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    record_hash VARCHAR(64),
    _source_file VARCHAR(1024),
    _batch_id VARCHAR(64),
    _loaded_at TIMESTAMP_LTZ
)
COMMENT = 'Append-only landing copy of gold dim_member Parquet export. Deduplicated by the MERGE in 3x_merge scripts. Synthetic data.';

CREATE TABLE IF NOT EXISTS landing_dim_provider (
    provider_sk VARCHAR(64),
    provider_id VARCHAR(48),
    specialty VARCHAR(60),
    state VARCHAR(2),
    network_status VARCHAR(20),
    cost_tier VARCHAR(12),
    effective_from TIMESTAMP_NTZ,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN,
    record_hash VARCHAR(64),
    _source_file VARCHAR(1024),
    _batch_id VARCHAR(64),
    _loaded_at TIMESTAMP_LTZ
)
COMMENT = 'Append-only landing copy of gold dim_provider Parquet export. Deduplicated by the MERGE in 3x_merge scripts. Synthetic data.';

CREATE TABLE IF NOT EXISTS landing_dim_diagnosis (
    diagnosis_sk VARCHAR(64),
    icd10_code VARCHAR(8),
    icd10_chapter VARCHAR(2),
    description VARCHAR(255),
    _source_file VARCHAR(1024),
    _batch_id VARCHAR(64),
    _loaded_at TIMESTAMP_LTZ
)
COMMENT = 'Append-only landing copy of gold dim_diagnosis Parquet export. Deduplicated by the MERGE in 3x_merge scripts. Synthetic data.';

CREATE TABLE IF NOT EXISTS landing_fact_claim (
    claim_id VARCHAR(15),
    member_sk VARCHAR(64),
    provider_sk VARCHAR(64),
    primary_diagnosis_sk VARCHAR(64),
    service_date_key INTEGER,
    received_date_key INTEGER,
    claim_type VARCHAR(15),
    place_of_service VARCHAR(4),
    claim_status VARCHAR(6),
    line_count SMALLINT,
    total_billed_amount NUMBER(12, 2),
    total_allowed_amount NUMBER(12, 2),
    total_paid_amount NUMBER(12, 2),
    _source_file VARCHAR(1024),
    _batch_id VARCHAR(64),
    _loaded_at TIMESTAMP_LTZ
)
COMMENT = 'Append-only landing copy of gold fact_claim Parquet export. Deduplicated by the MERGE in 3x_merge scripts. Synthetic data.';

CREATE TABLE IF NOT EXISTS landing_fact_claim_line (
    line_id VARCHAR(20),
    claim_id VARCHAR(15),
    line_number SMALLINT,
    procedure_code VARCHAR(5),
    units SMALLINT,
    service_date_key INTEGER,
    billed_amount NUMBER(12, 2),
    allowed_amount NUMBER(12, 2),
    paid_amount NUMBER(12, 2),
    _source_file VARCHAR(1024),
    _batch_id VARCHAR(64),
    _loaded_at TIMESTAMP_LTZ
)
COMMENT = 'Append-only landing copy of gold fact_claim_line Parquet export. Deduplicated by the MERGE in 3x_merge scripts. Synthetic data.';
