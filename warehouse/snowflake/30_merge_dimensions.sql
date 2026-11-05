-- MERGE the landing tables into the gold dimensions. ALL DATA IS SYNTHETIC.
--
-- Run as: <PREFIX>_<ENV>_TRANSFORMER (e.g. CLAIMS_DEV_TRANSFORMER).
-- Dimensions load BEFORE facts (30 before 31).
--
-- Idempotent: the source is deduplicated to the latest landed row per key and a
-- MATCHED row is updated only if its content hash differs, so running this
-- twice, or after a COPY reload, changes nothing the second time.
--
-- SCD2: the SCD2 versioning (effective_from / effective_to / is_current) is
-- computed upstream in the Spark gold job (#8) and exported, one row per
-- version keyed by the version's surrogate key. Snowflake therefore only
-- upserts versions: a new version is an INSERT, closing the previous version
-- (effective_to set, is_current = FALSE) is an UPDATE of that row.

USE SCHEMA marts;

MERGE INTO dim_date AS tgt
USING (
    SELECT
        date_key,
        full_date,
        year,
        quarter,
        month,
        month_name,
        day_of_month,
        day_of_week,
        is_weekend,
        _batch_id
    FROM raw.landing_dim_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY date_key
        ORDER BY _loaded_at DESC, _source_file DESC
    ) = 1
) AS src
    ON tgt.date_key = src.date_key
WHEN MATCHED AND HASH(tgt.full_date, tgt.year, tgt.quarter, tgt.month, tgt.month_name, tgt.day_of_month, tgt.day_of_week, tgt.is_weekend) != HASH(src.full_date, src.year, src.quarter, src.month, src.month_name, src.day_of_month, src.day_of_week, src.is_weekend) THEN
    UPDATE SET
        full_date = src.full_date,
        year = src.year,
        quarter = src.quarter,
        month = src.month,
        month_name = src.month_name,
        day_of_month = src.day_of_month,
        day_of_week = src.day_of_week,
        is_weekend = src.is_weekend,
        load_batch_id = src._batch_id,
        loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (date_key, full_date, year, quarter, month, month_name, day_of_month, day_of_week, is_weekend, load_batch_id)
    VALUES (src.date_key, src.full_date, src.year, src.quarter, src.month, src.month_name, src.day_of_month, src.day_of_week, src.is_weekend, src._batch_id);

MERGE INTO dim_member AS tgt
USING (
    SELECT
        member_sk,
        member_id,
        birth_year,
        sex,
        state,
        plan_type,
        enrollment_date,
        effective_from,
        effective_to,
        is_current,
        record_hash,
        _batch_id
    FROM raw.landing_dim_member
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY member_sk
        ORDER BY _loaded_at DESC, _source_file DESC
    ) = 1
) AS src
    ON tgt.member_sk = src.member_sk
WHEN MATCHED AND HASH(tgt.member_id, tgt.birth_year, tgt.sex, tgt.state, tgt.plan_type, tgt.enrollment_date, tgt.effective_from, tgt.effective_to, tgt.is_current, tgt.record_hash) != HASH(src.member_id, src.birth_year, src.sex, src.state, src.plan_type, src.enrollment_date, src.effective_from, src.effective_to, src.is_current, src.record_hash) THEN
    UPDATE SET
        member_id = src.member_id,
        birth_year = src.birth_year,
        sex = src.sex,
        state = src.state,
        plan_type = src.plan_type,
        enrollment_date = src.enrollment_date,
        effective_from = src.effective_from,
        effective_to = src.effective_to,
        is_current = src.is_current,
        record_hash = src.record_hash,
        load_batch_id = src._batch_id,
        loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (member_sk, member_id, birth_year, sex, state, plan_type, enrollment_date, effective_from, effective_to, is_current, record_hash, load_batch_id)
    VALUES (src.member_sk, src.member_id, src.birth_year, src.sex, src.state, src.plan_type, src.enrollment_date, src.effective_from, src.effective_to, src.is_current, src.record_hash, src._batch_id);

MERGE INTO dim_provider AS tgt
USING (
    SELECT
        provider_sk,
        provider_id,
        specialty,
        state,
        network_status,
        cost_tier,
        effective_from,
        effective_to,
        is_current,
        record_hash,
        _batch_id
    FROM raw.landing_dim_provider
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY provider_sk
        ORDER BY _loaded_at DESC, _source_file DESC
    ) = 1
) AS src
    ON tgt.provider_sk = src.provider_sk
WHEN MATCHED AND HASH(tgt.provider_id, tgt.specialty, tgt.state, tgt.network_status, tgt.cost_tier, tgt.effective_from, tgt.effective_to, tgt.is_current, tgt.record_hash) != HASH(src.provider_id, src.specialty, src.state, src.network_status, src.cost_tier, src.effective_from, src.effective_to, src.is_current, src.record_hash) THEN
    UPDATE SET
        provider_id = src.provider_id,
        specialty = src.specialty,
        state = src.state,
        network_status = src.network_status,
        cost_tier = src.cost_tier,
        effective_from = src.effective_from,
        effective_to = src.effective_to,
        is_current = src.is_current,
        record_hash = src.record_hash,
        load_batch_id = src._batch_id,
        loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (provider_sk, provider_id, specialty, state, network_status, cost_tier, effective_from, effective_to, is_current, record_hash, load_batch_id)
    VALUES (src.provider_sk, src.provider_id, src.specialty, src.state, src.network_status, src.cost_tier, src.effective_from, src.effective_to, src.is_current, src.record_hash, src._batch_id);

MERGE INTO dim_diagnosis AS tgt
USING (
    SELECT
        diagnosis_sk,
        icd10_code,
        icd10_chapter,
        description,
        _batch_id
    FROM raw.landing_dim_diagnosis
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY diagnosis_sk
        ORDER BY _loaded_at DESC, _source_file DESC
    ) = 1
) AS src
    ON tgt.diagnosis_sk = src.diagnosis_sk
WHEN MATCHED AND HASH(tgt.icd10_code, tgt.icd10_chapter, tgt.description) != HASH(src.icd10_code, src.icd10_chapter, src.description) THEN
    UPDATE SET
        icd10_code = src.icd10_code,
        icd10_chapter = src.icd10_chapter,
        description = src.description,
        load_batch_id = src._batch_id,
        loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (diagnosis_sk, icd10_code, icd10_chapter, description, load_batch_id)
    VALUES (src.diagnosis_sk, src.icd10_code, src.icd10_chapter, src.description, src._batch_id);
