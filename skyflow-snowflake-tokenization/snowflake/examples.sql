-- ============================================================================
-- Skyflow Tokenize and Detokenize Functions - Usage Examples
-- ============================================================================
-- Examples of how to use the tokenize and detokenize external functions.
-- ============================================================================

USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- ============================================================================
-- TOKENIZATION EXAMPLES
-- ============================================================================

-- ============================================================================
-- Example 1: Tokenize a Single Value
-- ============================================================================

SELECT TOK_NAME('John Doe');
SELECT TOK_SSN('123-45-6789');
SELECT TOK_DOB('1990-01-01');
SELECT TOK_ID('12345');

-- ============================================================================
-- Example 2: Tokenize Multiple Columns
-- ============================================================================

SELECT
    patient_id,
    TOK_NAME(patient_name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(date_of_birth) as dob_token
FROM patients_raw
LIMIT 100;

-- ============================================================================
-- Example 3: Tokenize and Create Protected Table
-- ============================================================================

CREATE OR REPLACE TABLE patients_tokenized AS
SELECT
    patient_id,
    TOK_NAME(patient_name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(date_of_birth) as dob_token,
    TOK_ID(medical_record_number) as mrn_token,
    admission_date,
    department
FROM patients_raw;

-- ============================================================================
-- Example 4: Tokenize with WHERE Clause (Conditional Tokenization)
-- ============================================================================

SELECT
    patient_id,
    CASE
        WHEN requires_protection = TRUE THEN TOK_NAME(patient_name)
        ELSE patient_name
    END as name_value,
    TOK_SSN(ssn) as ssn_token,
    admission_date
FROM patients_raw
WHERE admission_date > '2024-01-01';

-- ============================================================================
-- Example 5: Tokenize with NULL Handling
-- ============================================================================

SELECT
    patient_id,
    CASE
        WHEN patient_name IS NOT NULL THEN TOK_NAME(patient_name)
        ELSE NULL
    END as name_token,
    CASE
        WHEN ssn IS NOT NULL THEN TOK_SSN(ssn)
        ELSE NULL
    END as ssn_token
FROM patients_raw
LIMIT 100;

-- ============================================================================
-- Example 6: Test Upsert Behavior (Same Value = Same Token)
-- ============================================================================

-- First tokenization
SELECT TOK_NAME('Jane Smith') as first_token;

-- Second tokenization (should return the same token due to upsert)
SELECT TOK_NAME('Jane Smith') as second_token;

-- Verify upsert with a query
WITH tokenize_test AS (
    SELECT TOK_NAME('Test User') as token_1
    UNION ALL
    SELECT TOK_NAME('Test User') as token_2
)
SELECT
    token_1,
    token_2,
    CASE WHEN token_1 = token_2 THEN 'SAME (UPSERT WORKING)' ELSE 'DIFFERENT' END as upsert_status
FROM tokenize_test;

-- ============================================================================
-- Example 7: Incremental Tokenization (Delta Load)
-- ============================================================================

-- Tokenize only new records
INSERT INTO patients_tokenized (patient_id, name_token, ssn_token, admission_date)
SELECT
    patient_id,
    TOK_NAME(patient_name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    admission_date
FROM patients_raw
WHERE admission_date > (SELECT MAX(admission_date) FROM patients_tokenized);

-- ============================================================================
-- DETOKENIZATION EXAMPLES
-- ============================================================================

-- ============================================================================
-- Example 8: Detokenize a Single Token
-- ============================================================================

SELECT DETOK_NAME('tok_name_1760648602157302219_d2e662331d4205e039bc17e5a84b28bd');

-- ============================================================================
-- Example 9: Detokenize Multiple Columns
-- ============================================================================

SELECT
    patient_id,
    DETOK_NAME(name_token) as patient_name,
    DETOK_SSN(ssn_token) as ssn,
    DETOK_DOB(dob_token) as date_of_birth
FROM patients_tokenized
LIMIT 100;

-- ============================================================================
-- Example 10: Detokenize with WHERE Clause
-- ============================================================================

SELECT
    patient_id,
    DETOK_NAME(name_token) as patient_name,
    admission_date
FROM patients_tokenized
WHERE admission_date > '2024-01-01'
  AND department = 'CARDIOLOGY';

-- ============================================================================
-- Example 11: Detokenize in JOIN
-- ============================================================================

SELECT
    p.patient_id,
    DETOK_NAME(p.name_token) as patient_name,
    v.visit_date,
    v.diagnosis
FROM patients_tokenized p
JOIN visits v ON p.patient_id = v.patient_id
WHERE v.visit_date > CURRENT_DATE - 30;

-- ============================================================================
-- Example 12: Detokenize with Aggregation
-- ============================================================================

SELECT
    department,
    COUNT(*) as patient_count,
    COUNT(DISTINCT DETOK_SSN(ssn_token)) as unique_patients
FROM patients_tokenized
WHERE admission_date > CURRENT_DATE - 90
GROUP BY department
ORDER BY patient_count DESC;

-- ============================================================================
-- ROUND-TRIP EXAMPLES (TOKENIZE + DETOKENIZE)
-- ============================================================================

-- ============================================================================
-- Example 13: Round-Trip Test (Verify Data Integrity)
-- ============================================================================

SELECT
    'John Doe' as original_value,
    TOK_NAME('John Doe') as token,
    DETOK_NAME(TOK_NAME('John Doe')) as roundtrip_value,
    CASE
        WHEN 'John Doe' = DETOK_NAME(TOK_NAME('John Doe')) THEN 'PASS'
        ELSE 'FAIL'
    END as integrity_check;

-- ============================================================================
-- Example 14: Batch Round-Trip Test
-- ============================================================================

WITH test_data AS (
    SELECT 'Alice Johnson' as name, '111-11-1111' as ssn, '1985-05-15' as dob
    UNION ALL
    SELECT 'Bob Wilson', '222-22-2222', '1990-08-22'
    UNION ALL
    SELECT 'Carol Martinez', '333-33-3333', '1978-12-03'
)
SELECT
    name as original_name,
    ssn as original_ssn,
    dob as original_dob,
    TOK_NAME(name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(dob) as dob_token,
    DETOK_NAME(TOK_NAME(name)) as name_roundtrip,
    DETOK_SSN(TOK_SSN(ssn)) as ssn_roundtrip,
    DETOK_DOB(TOK_DOB(dob)) as dob_roundtrip,
    CASE
        WHEN name = DETOK_NAME(TOK_NAME(name))
         AND ssn = DETOK_SSN(TOK_SSN(ssn))
         AND dob = DETOK_DOB(TOK_DOB(dob))
        THEN 'ALL PASS'
        ELSE 'FAIL'
    END as integrity_status
FROM test_data;

-- ============================================================================
-- ADVANCED USE CASES
-- ============================================================================

-- ============================================================================
-- Example 15: Tokenize Raw Data and Create Protected View
-- ============================================================================

-- Step 1: Create tokenized table
CREATE OR REPLACE TABLE patients_protected AS
SELECT
    patient_id,
    TOK_NAME(patient_name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(date_of_birth) as dob_token,
    -- Non-sensitive fields stay in plaintext
    admission_date,
    department,
    insurance_provider
FROM patients_raw;

-- Step 2: Create detokenized view for authorized users
CREATE OR REPLACE SECURE VIEW patients_detokenized AS
SELECT
    patient_id,
    DETOK_NAME(name_token) as patient_name,
    DETOK_SSN(ssn_token) as ssn,
    DETOK_DOB(dob_token) as date_of_birth,
    admission_date,
    department,
    insurance_provider
FROM patients_protected;

-- Step 3: Grant access
GRANT SELECT ON TABLE patients_protected TO ROLE DATA_ANALYST_ROLE;
GRANT SELECT ON VIEW patients_detokenized TO ROLE HEALTHCARE_ADMIN_ROLE;

-- ============================================================================
-- Example 16: Partial Detokenization (Selective Access)
-- ============================================================================

-- View for customer service (name only, no SSN/DOB)
CREATE OR REPLACE VIEW patients_for_customer_service AS
SELECT
    patient_id,
    DETOK_NAME(name_token) as patient_name,
    -- Show masked SSN (no detokenization)
    '***-**-' || RIGHT(ssn_token, 4) as ssn_masked,
    admission_date,
    department
FROM patients_protected;

-- ============================================================================
-- Example 17: Data Migration (Tokenize Existing Data)
-- ============================================================================

-- Migrate plaintext table to tokenized format
CREATE OR REPLACE TABLE legacy_patients_tokenized AS
SELECT
    patient_id,
    TOK_NAME(name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(birthdate) as dob_token,
    TOK_ID(account_number) as account_token,
    created_date,
    updated_date
FROM legacy_patients
WHERE ssn IS NOT NULL;

-- Verify migration
SELECT
    COUNT(*) as total_rows,
    COUNT(name_token) as tokenized_names,
    COUNT(ssn_token) as tokenized_ssns,
    COUNT(CASE WHEN name_token LIKE 'ERROR:%' THEN 1 END) as errors
FROM legacy_patients_tokenized;

-- ============================================================================
-- Example 18: BI Tool View with Masked/Detokenized Data
-- ============================================================================

CREATE OR REPLACE SECURE VIEW patients_for_tableau AS
SELECT
    patient_id,
    DETOK_NAME(name_token) as patient_name,
    -- Mask SSN except last 4 digits
    '***-**-' || RIGHT(DETOK_SSN(ssn_token), 4) as ssn_masked,
    -- Show year only for DOB
    YEAR(DETOK_DOB(dob_token)::DATE) as birth_year,
    admission_date,
    department
FROM patients_protected
WHERE admission_date > DATEADD(year, -2, CURRENT_DATE());

GRANT SELECT ON VIEW patients_for_tableau TO ROLE BI_ANALYST_ROLE;

-- ============================================================================
-- Example 19: Error Handling
-- ============================================================================

-- Check for tokenization/detokenization errors
SELECT
    patient_id,
    name_token,
    DETOK_NAME(name_token) as detokenized_name,
    CASE
        WHEN name_token IS NULL THEN 'NULL_TOKEN'
        WHEN DETOK_NAME(name_token) LIKE 'ERROR:%' THEN 'DETOKENIZATION_FAILED'
        ELSE 'SUCCESS'
    END as status
FROM patients_protected
LIMIT 100;

-- ============================================================================
-- Example 20: Merge Tokenized Data (Upsert Pattern)
-- ============================================================================

MERGE INTO patients_protected target
USING (
    SELECT
        patient_id,
        TOK_NAME(patient_name) as name_token,
        TOK_SSN(ssn) as ssn_token,
        admission_date
    FROM patients_raw_incoming
) source
ON target.patient_id = source.patient_id
WHEN MATCHED THEN UPDATE SET
    target.name_token = source.name_token,
    target.ssn_token = source.ssn_token
WHEN NOT MATCHED THEN INSERT (patient_id, name_token, ssn_token, admission_date)
    VALUES (source.patient_id, source.name_token, source.ssn_token, source.admission_date);

-- ============================================================================
-- Performance Monitoring
-- ============================================================================

-- Check query performance for tokenize/detokenize operations
SELECT
    query_id,
    LEFT(query_text, 100) as query_preview,
    execution_time,
    bytes_scanned,
    rows_produced
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE (query_text LIKE '%TOK_%' OR query_text LIKE '%DETOK_%')
  AND start_time > DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY execution_time DESC
LIMIT 10;

-- ============================================================================
-- NOTES
-- ============================================================================
-- Tokenization:
-- - Uses upsert mode: Same value always returns the same token
-- - Each data type (NAME, ID, DOB, SSN) uses a separate Skyflow vault/table
-- - Validation is handled by Skyflow (configured vault-side)
-- - NULL values are passed through without API calls
--
-- Detokenization:
-- - Retrieves original plaintext value from token
-- - Each data type routes to the correct vault/table
-- - Invalid tokens return error messages starting with "ERROR:"
--
-- Performance Tips:
-- 1. Use WHERE clauses to limit rows before tokenize/detokenize
-- 2. Process only needed columns
-- 3. For large datasets, consider batching in stored procedures
-- 4. Use CTAS for one-time bulk operations
-- 5. Lambda processes up to 200 records per batch with 10 concurrent batches
--
-- Security Tips:
-- 1. Use secure views to control access to detokenized data
-- 2. Grant function usage carefully (separate roles for tokenize vs detokenize)
-- 3. Audit tokenization/detokenization queries regularly
-- 4. Consider column-level security for sensitive data
-- 5. Store tokens, not plaintext in production tables
--
-- Cost Optimization:
-- 1. Tokenize once, detokenize only when needed
-- 2. Limit result sets with WHERE/LIMIT
-- 3. Use materialized views for frequently accessed detokenized data
-- 4. Monitor AWS Lambda costs in CloudWatch
-- ============================================================================
