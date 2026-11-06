-- Clustering / micro-partition EXPERIMENTS for ticket #16. Not part of the
-- default load. ALL DATA IS SYNTHETIC. NOT YET RUN against a live account.
--
-- Hypotheses (docs/snowflake.md, "Clustering and micro-partitions"):
--   H1: at the target trial size (1M claims, about 2.4M lines) no clustering
--       key beats natural load order, because loads arrive roughly in
--       received_date order, so micro-partitions are already well-clustered
--       on date columns.
--   H2: claims are late-arriving (2% land 31-90 days after service), so
--       clustering on service_date_key degrades first; check overlap depth.
--   H3: automatic clustering would cost more credits than it saves on tables
--       this small (< 1 GB compressed), so leave it off.
-- Measure BEFORE deciding; record numbers in #16.

USE SCHEMA marts;

-- Baseline: how well does natural order cluster on service date?
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_claim_line', '(service_date_key)');
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_claim', '(service_date_key)');
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_claim', '(provider_sk)');

-- Only if the baseline shows high average_overlaps / average_depth AND queries
-- filter on that column, enable a key (this starts billed automatic clustering):
-- ALTER TABLE fact_claim_line CLUSTER BY (service_date_key);
-- ALTER TABLE fact_claim CLUSTER BY (service_date_key);
-- To stop the spend again:
-- ALTER TABLE fact_claim_line SUSPEND RECLUSTER;
-- ALTER TABLE fact_claim_line DROP CLUSTERING KEY;
