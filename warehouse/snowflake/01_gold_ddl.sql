-- Gold star schema in Snowflake (schema MARTS).
-- Source of truth is the Delta gold layer (ADR-0002); these tables hold the
-- loaded copy. Column set follows docs/data-generator.md and the planned gold
-- layer (#8). ALL DATA IS SYNTHETIC.
--
-- Run as: SYSADMIN, with the database selected by the connection, e.g.
--   snow sql --database CLAIMS_DEV --role SYSADMIN -f 01_gold_ddl.sql   (docs/snowflake.md)
-- Idempotent: CREATE ... IF NOT EXISTS. Privileges come from future grants
-- defined in Terraform, so no GRANT statements here.
--
-- NOTE: PRIMARY KEY / FOREIGN KEY / UNIQUE are informational in Snowflake (not
-- enforced). Integrity is enforced by the MERGE keys and by tests (#22).
-- NOTE: NOT NULL is enforced.

USE SCHEMA marts;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER NOT NULL,
    full_date DATE NOT NULL,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
COMMENT = 'Calendar dimension, one row per day. date_key is YYYYMMDD. Synthetic-data project.';

-- SCD2: one row per member version. member_sk is the surrogate key assigned in
-- the gold layer (not by Snowflake) so Delta and Snowflake agree on keys.
-- member_id / provider_id hold keyed HMAC tokens, tok_<key_id>_<32 hex> (ticket #71,
-- lakehouse.governance.tokenise), NOT the source ids; VARCHAR(48) fits an 8-char key id.
-- member_sk / provider_sk are sha256 over (kind, token, effective_from), so they are keyed too.
CREATE TABLE IF NOT EXISTS dim_member (
    member_sk VARCHAR(64) NOT NULL,
    member_id VARCHAR(48) NOT NULL,
    birth_year SMALLINT,
    sex VARCHAR(1),
    state VARCHAR(2),
    plan_type VARCHAR(20),
    enrollment_date DATE,
    effective_from TIMESTAMP_NTZ NOT NULL,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN NOT NULL,
    record_hash VARCHAR(64),
    load_batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_member PRIMARY KEY (member_sk)
)
COMMENT = 'Member dimension, SCD type 2 (effective_from/effective_to/is_current). member_id is a keyed token (ticket #71). Fabricated data only, no real PHI. Masking: ticket #25.';

CREATE TABLE IF NOT EXISTS dim_provider (
    provider_sk VARCHAR(64) NOT NULL,
    provider_id VARCHAR(48) NOT NULL,
    specialty VARCHAR(60),
    state VARCHAR(2),
    network_status VARCHAR(20),
    cost_tier VARCHAR(12),
    effective_from TIMESTAMP_NTZ NOT NULL,
    effective_to TIMESTAMP_NTZ,
    is_current BOOLEAN NOT NULL,
    record_hash VARCHAR(64),
    load_batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_provider PRIMARY KEY (provider_sk)
)
COMMENT = 'Provider dimension, SCD type 2. provider_id is a keyed token (ticket #71). cost_tier is STANDARD, ELEVATED or HIGH_COST. Synthetic data.';

CREATE TABLE IF NOT EXISTS dim_diagnosis (
    diagnosis_sk VARCHAR(64) NOT NULL,
    icd10_code VARCHAR(8) NOT NULL,
    icd10_chapter VARCHAR(2),
    description VARCHAR(255),
    load_batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_diagnosis PRIMARY KEY (diagnosis_sk)
)
COMMENT = 'ICD-10 code dimension (type 1, overwritten in place). Only valid codes, invalid ones are quarantined upstream (#7).';

CREATE TABLE IF NOT EXISTS fact_claim (
    claim_id VARCHAR(15) NOT NULL,
    member_sk VARCHAR(64) NOT NULL,
    provider_sk VARCHAR(64) NOT NULL,
    primary_diagnosis_sk VARCHAR(64),
    service_date_key INTEGER NOT NULL,
    received_date_key INTEGER NOT NULL,
    claim_type VARCHAR(15) NOT NULL,
    place_of_service VARCHAR(4),
    claim_status VARCHAR(6) NOT NULL,
    line_count SMALLINT NOT NULL,
    total_billed_amount NUMBER(12, 2) NOT NULL,
    total_allowed_amount NUMBER(12, 2) NOT NULL,
    total_paid_amount NUMBER(12, 2) NOT NULL,
    load_batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_claim PRIMARY KEY (claim_id),
    CONSTRAINT fk_fact_claim_member FOREIGN KEY (member_sk) REFERENCES dim_member (member_sk),
    CONSTRAINT fk_fact_claim_provider FOREIGN KEY (provider_sk) REFERENCES dim_provider (provider_sk),
    CONSTRAINT fk_fact_claim_dx FOREIGN KEY (primary_diagnosis_sk) REFERENCES dim_diagnosis (diagnosis_sk),
    CONSTRAINT fk_fact_claim_svc_date FOREIGN KEY (service_date_key) REFERENCES dim_date (date_key)
)
COMMENT = 'Claim header fact, grain: one row per claim. Amounts are DECIMAL(12,2) (cast from the float64 raw layer). Synthetic data.';

CREATE TABLE IF NOT EXISTS fact_claim_line (
    line_id VARCHAR(20) NOT NULL,
    claim_id VARCHAR(15) NOT NULL,
    line_number SMALLINT NOT NULL,
    procedure_code VARCHAR(5) NOT NULL,
    units SMALLINT NOT NULL,
    service_date_key INTEGER NOT NULL,
    billed_amount NUMBER(12, 2) NOT NULL,
    allowed_amount NUMBER(12, 2) NOT NULL,
    paid_amount NUMBER(12, 2) NOT NULL,
    load_batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_claim_line PRIMARY KEY (line_id),
    CONSTRAINT fk_fact_claim_line_claim FOREIGN KEY (claim_id) REFERENCES fact_claim (claim_id),
    CONSTRAINT fk_fact_claim_line_date FOREIGN KEY (service_date_key) REFERENCES dim_date (date_key)
)
COMMENT = 'Claim line fact, grain: one row per claim line. Rows with invalid/null procedure codes are quarantined upstream, not loaded.';
