-- MERGE the landing tables into the gold facts. ALL DATA IS SYNTHETIC.
--
-- Run as: <PREFIX>_<ENV>_TRANSFORMER, after 30_merge_dimensions.sql.
-- Same idempotency contract as the dimensions: dedupe to the latest landed row
-- per business key, update only when the content hash differs (claim
-- adjustments), insert otherwise. Orphan foreign keys are not blocked here
-- (informational constraints); 40_reconcile.sql reports them.

USE SCHEMA marts;

MERGE INTO fact_claim AS tgt
USING (
    SELECT
        claim_id,
        member_sk,
        provider_sk,
        primary_diagnosis_sk,
        service_date_key,
        received_date_key,
        claim_type,
        place_of_service,
        claim_status,
        line_count,
        total_billed_amount,
        total_allowed_amount,
        total_paid_amount,
        _batch_id
    FROM raw.landing_fact_claim
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY claim_id
        ORDER BY _loaded_at DESC, _source_file DESC
    ) = 1
) AS src
    ON tgt.claim_id = src.claim_id
WHEN MATCHED AND HASH(tgt.member_sk, tgt.provider_sk, tgt.primary_diagnosis_sk, tgt.service_date_key, tgt.received_date_key, tgt.claim_type, tgt.place_of_service, tgt.claim_status, tgt.line_count, tgt.total_billed_amount, tgt.total_allowed_amount, tgt.total_paid_amount) != HASH(src.member_sk, src.provider_sk, src.primary_diagnosis_sk, src.service_date_key, src.received_date_key, src.claim_type, src.place_of_service, src.claim_status, src.line_count, src.total_billed_amount, src.total_allowed_amount, src.total_paid_amount) THEN
    UPDATE SET
        member_sk = src.member_sk,
        provider_sk = src.provider_sk,
        primary_diagnosis_sk = src.primary_diagnosis_sk,
        service_date_key = src.service_date_key,
        received_date_key = src.received_date_key,
        claim_type = src.claim_type,
        place_of_service = src.place_of_service,
        claim_status = src.claim_status,
        line_count = src.line_count,
        total_billed_amount = src.total_billed_amount,
        total_allowed_amount = src.total_allowed_amount,
        total_paid_amount = src.total_paid_amount,
        load_batch_id = src._batch_id,
        loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (claim_id, member_sk, provider_sk, primary_diagnosis_sk, service_date_key, received_date_key, claim_type, place_of_service, claim_status, line_count, total_billed_amount, total_allowed_amount, total_paid_amount, load_batch_id)
    VALUES (src.claim_id, src.member_sk, src.provider_sk, src.primary_diagnosis_sk, src.service_date_key, src.received_date_key, src.claim_type, src.place_of_service, src.claim_status, src.line_count, src.total_billed_amount, src.total_allowed_amount, src.total_paid_amount, src._batch_id);

MERGE INTO fact_claim_line AS tgt
USING (
    SELECT
        line_id,
        claim_id,
        line_number,
        procedure_code,
        units,
        service_date_key,
        billed_amount,
        allowed_amount,
        paid_amount,
        _batch_id
    FROM raw.landing_fact_claim_line
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY line_id
        ORDER BY _loaded_at DESC, _source_file DESC
    ) = 1
) AS src
    ON tgt.line_id = src.line_id
WHEN MATCHED AND HASH(tgt.claim_id, tgt.line_number, tgt.procedure_code, tgt.units, tgt.service_date_key, tgt.billed_amount, tgt.allowed_amount, tgt.paid_amount) != HASH(src.claim_id, src.line_number, src.procedure_code, src.units, src.service_date_key, src.billed_amount, src.allowed_amount, src.paid_amount) THEN
    UPDATE SET
        claim_id = src.claim_id,
        line_number = src.line_number,
        procedure_code = src.procedure_code,
        units = src.units,
        service_date_key = src.service_date_key,
        billed_amount = src.billed_amount,
        allowed_amount = src.allowed_amount,
        paid_amount = src.paid_amount,
        load_batch_id = src._batch_id,
        loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (line_id, claim_id, line_number, procedure_code, units, service_date_key, billed_amount, allowed_amount, paid_amount, load_batch_id)
    VALUES (src.line_id, src.claim_id, src.line_number, src.procedure_code, src.units, src.service_date_key, src.billed_amount, src.allowed_amount, src.paid_amount, src._batch_id);
