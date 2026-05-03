-- =============================================================================
-- NEDBANK DATA TALENT CHALLENGE 2026 — DE TRACK
-- Scoring Validation Queries — Gold Layer
-- =============================================================================
--
-- These three queries are the automated scoring checks applied against your
-- pipeline's Gold layer output. Your pipeline must produce output that satisfies
-- all three to receive full correctness marks.
--
-- EXPECTED VALUES
--   Exact expected row counts and totals are NOT published here. They are
--   verified by the evaluation system from the same source dataset provided to
--   participants. You do not need to hit a specific magic number — you need
--   to build a correct pipeline.
--
-- HOW TO RUN LOCALLY (DuckDB)
--   1. Install DuckDB (https://duckdb.org/docs/installation) and the Delta
--      extension:
--        INSTALL delta; LOAD delta;
--   2. Set the GOLD_PATH variable below to match your output directory, e.g.:
--        SET VARIABLE gold_path = '/data/output/gold';
--   3. Run this file:
--        duckdb < validation_queries.sql
--   Alternatively, open the DuckDB CLI and run:
--        .read validation_queries.sql
--
--   If your output is plain Parquet (not Delta), replace delta_scan() calls
--   with parquet_scan('path/to/table/**/*.parquet').
--
-- NOTE FOR PARTICIPANTS
--   The evaluation system reads tables using the Delta format
--   (delta_scan / delta.load). Ensure your Gold layer output contains valid
--   Delta Lake metadata (_delta_log/) alongside the Parquet part files.
--   See output_schema_spec.md §5 for the required directory structure.
--
-- =============================================================================

-- Set the path to your Gold layer output root.
-- Adjust this to match your local or container mount point.
SET VARIABLE gold_path = '/data/output/gold';

-- =============================================================================
-- QUERY 1: Transaction Volume by Type
-- =============================================================================
--
-- WHAT IT CHECKS
--   Verifies that fact_transactions is fully populated and that all four
--   recognised transaction types are present with correct record counts.
--   Also surfaces total amounts per type as a secondary sanity check.
--
-- EXPECTED OUTPUT SHAPE
--   Exactly 4 rows, one per transaction type:
--     CREDIT   | <count> | <total_amount>
--     DEBIT    | <count> | <total_amount>
--     FEE      | <count> | <total_amount>
--     REVERSAL | <count> | <total_amount>
--
-- FAILURE MODES TO WATCH FOR
--   - Fewer than 4 rows: a transaction type was dropped or filtered out
--   - Row count well below expected: deduplication logic too aggressive
--   - Row count above expected: duplicates were not removed in the Silver layer
--   - NULL in transaction_type: type standardisation failed for some records

SELECT
    transaction_type,
    COUNT(*)                        AS record_count,
    SUM(amount)                     AS total_amount,
    ROUND(COUNT(*) * 100.0
          / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM delta_scan(getvariable('gold_path') || '/fact_transactions')
GROUP BY transaction_type
ORDER BY transaction_type;

-- =============================================================================
-- QUERY 2: Zero Unlinked Accounts
-- =============================================================================
--
-- WHAT IT CHECKS
--   Verifies referential integrity between dim_accounts and dim_customers.
--   Every account in the Gold layer must be linked to a known customer via
--   the customer_id field. An unlinked account indicates either:
--     (a) the Silver layer failed to validate the account-customer join, or
--     (b) dim_accounts.customer_id was not correctly populated from
--         accounts.csv.customer_ref (see output_schema_spec.md §3).
--
-- EXPECTED OUTPUT SHAPE
--   Exactly 1 row:
--     unlinked_accounts
--     -----------------
--     0
--
-- IMPORTANT SCHEMA NOTE
--   This query joins on dim_accounts.customer_id. Your Gold layer dim_accounts
--   table must include this field at position 3 — it is required.
--   See output_schema_spec.md §3 for derivation details (source field:
--   accounts.csv.customer_ref, renamed to customer_id in the Gold layer).

SELECT
    COUNT(*) AS unlinked_accounts
FROM delta_scan(getvariable('gold_path') || '/dim_accounts')   AS a
LEFT JOIN delta_scan(getvariable('gold_path') || '/dim_customers') AS c
       ON a.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- =============================================================================
-- QUERY 3: Province Distribution
-- =============================================================================
--
-- WHAT IT CHECKS
--   Verifies that dim_customers covers all 9 South African provinces and that
--   the distribution of accounts across provinces matches expectations.
--   A missing province indicates either a data generation issue (not your fault)
--   or that province records were dropped during transformation.
--
-- EXPECTED OUTPUT SHAPE
--   Exactly 9 rows, one per SA province (alphabetical order):
--     Eastern Cape          | <account_count>
--     Free State            | <account_count>
--     Gauteng               | <account_count>
--     KwaZulu-Natal         | <account_count>
--     Limpopo               | <account_count>
--     Mpumalanga            | <account_count>
--     North West            | <account_count>
--     Northern Cape         | <account_count>
--     Western Cape          | <account_count>
--
-- FAILURE MODES TO WATCH FOR
--   - Fewer than 9 rows: one or more provinces were dropped or mis-labelled
--   - account_count far below expected: accounts were lost in transformation
--   - Province name mismatch: ensure province values are standardised to the
--     canonical names listed above (title-case, full name, no abbreviations)

SELECT
    c.province,
    COUNT(DISTINCT a.account_id) AS account_count
FROM delta_scan(getvariable('gold_path') || '/dim_accounts')   AS a
JOIN delta_scan(getvariable('gold_path') || '/dim_customers')  AS c
  ON a.customer_id = c.customer_id
GROUP BY c.province
ORDER BY c.province;

-- =============================================================================
-- END OF VALIDATION QUERIES
-- Document: validation_queries.sql
-- =============================================================================
