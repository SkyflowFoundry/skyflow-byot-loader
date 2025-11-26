-- ============================================================================
-- Create Skyflow Tokenize and Detokenize External Functions
-- ============================================================================
-- Creates external functions for both tokenization and detokenization.
-- Uses HTTP headers for control metadata and CONTEXT_HEADERS for audit trail.
--
-- Prerequisites:
-- 1. API integration created (run setup.sql first)
-- 2. AWS Lambda function deployed
-- 3. API Gateway URL from deployment
--
-- Replace:
--   - YOUR_API_ID: From API Gateway deployment
--   - YOUR_REGION: AWS region (e.g., us-east-1)
--   - YOUR_STAGE: API Gateway stage (e.g., prod)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE YOUR_DATABASE;
USE SCHEMA YOUR_SCHEMA;

-- ============================================================================
-- Detokenization Functions
-- ============================================================================
-- Control metadata (operation, dataType) sent via HEADERS
-- Caller identity (user, role, account) sent via CONTEXT_HEADERS

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_NAME(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize',
        'X-Data-Type' = 'NAME'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_ID(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize',
        'X-Data-Type' = 'ID'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_DOB(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize_partial',
        'X-Data-Type' = 'DOB'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_SSN(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize',
        'X-Data-Type' = 'SSN'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_SSN_PARTIAL(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize_partial',
        'X-Data-Type' = 'SSN'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_EMAIL(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize',
        'X-Data-Type' = 'EMAIL'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

-- ============================================================================
-- Tokenization Functions
-- ============================================================================
-- Control metadata (operation, dataType) sent via HEADERS
-- Caller identity (user, role, account) sent via CONTEXT_HEADERS
-- Uses upsert by default - returns existing token for duplicate values

CREATE OR REPLACE EXTERNAL FUNCTION TOK_NAME(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize',
        'X-Data-Type' = 'NAME'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ID(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize',
        'X-Data-Type' = 'ID'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_DOB(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize_partial',
        'X-Data-Type' = 'DOB'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_SSN(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize',
        'X-Data-Type' = 'SSN'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_SSN_PARTIAL(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize_partial',
        'X-Data-Type' = 'SSN'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_EMAIL(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize',
        'X-Data-Type' = 'EMAIL'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

-- ============================================================================
-- One-Way Tokenization Functions (Token + Delete)
-- ============================================================================
-- WARNING: Records are PERMANENTLY DELETED after tokenization
-- Detokenization will FAIL for one-way tokens (records no longer exist)
-- Use only when you need irreversible tokenization
--
-- Architecture:
-- - Uses SAME vaults as regular tokenization
-- - Separate tables within each vault using naming convention:
--   * Regular: table "name" with column "name"
--   * Oneway: table "name_oneway" with column "name_oneway"
-- - This ensures regular tokenization records are never accidentally deleted
--
-- Setup Required:
-- 1. Create <table>_oneway tables in your Skyflow vaults (e.g., name_oneway, ssn_oneway)
-- 2. Add corresponding <column>_oneway columns (e.g., name_oneway column in name_oneway table)
-- 3. No changes needed to skyflow-config.json (uses existing vault definitions)

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ONEWAY_NAME(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize_oneway',
        'X-Data-Type' = 'NAME'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ONEWAY_ID(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize_oneway',
        'X-Data-Type' = 'ID'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ONEWAY_DOB(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize_oneway',
        'X-Data-Type' = 'DOB'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ONEWAY_SSN(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize_oneway',
        'X-Data-Type' = 'SSN'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

-- ============================================================================
-- BYOT (Bring Your Own Token) Functions
-- ============================================================================
-- Allows you to specify your own custom tokens instead of Skyflow generating them
-- Useful for:
-- - Migrating from another tokenization system
-- - Maintaining existing token values
-- - Custom token format requirements
--
-- Usage: BYOT_<TYPE>(plaintext_value, your_custom_token)
-- Example: BYOT_NAME('John Doe', 'my-custom-token-abc123')

CREATE OR REPLACE EXTERNAL FUNCTION BYOT_NAME(plaintext VARCHAR, custom_token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'byot',
        'X-Data-Type' = 'NAME'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION BYOT_ID(plaintext VARCHAR, custom_token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'byot',
        'X-Data-Type' = 'ID'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION BYOT_DOB(plaintext VARCHAR, custom_token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'byot',
        'X-Data-Type' = 'DOB'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

CREATE OR REPLACE EXTERNAL FUNCTION BYOT_SSN(plaintext VARCHAR, custom_token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'byot',
        'X-Data-Type' = 'SSN'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

-- ============================================================================
-- Query Functions (Privacy-Preserving Analytics)
-- ============================================================================
-- Execute SQL queries directly against vault data without detokenizing to Snowflake
-- Enables privacy-preserving analytics: filtering, aggregation, JOINs run in vault
-- Returns results as JSON array
--
-- Use Cases:
-- - Aggregate sensitive data without exposing individual records
-- - Filter vault data and return only matching records
-- - Perform substring searches and pattern matching
-- - Execute complex queries with JOINs across vault tables
-- - Compute statistics (COUNT, AVG, MIN, MAX) on encrypted data
--
-- Table Names (maps to vaults):
-- - name: NAME vault
-- - id: ID vault
-- - dob: DOB vault (year-preserving)
-- - ssn: SSN vault
-- - email: EMAIL vault
--
-- Limitations (per Skyflow Query API):
-- - Maximum 25 records per query (use LIMIT/OFFSET for pagination)
-- - Cannot return tokens (only plaintext with redaction policies)
-- - Only SELECT statements supported
-- - Cannot modify vault or perform transactions
--
-- Example Usage:
--   SELECT value FROM TABLE(FLATTEN(
--     SKYFLOW_QUERY('SELECT COUNT(*) as count FROM dob WHERE dob >= ''1990-01-01''')
--   ));

CREATE OR REPLACE EXTERNAL FUNCTION SKYFLOW_QUERY(sql_query VARCHAR)
    RETURNS VARIANT
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'query'
    )
    CONTEXT_HEADERS = (CURRENT_USER, CURRENT_ROLE, CURRENT_ACCOUNT, CURRENT_IP_ADDRESS)
    AS 'https://YOUR_API_ID.execute-api.YOUR_REGION.amazonaws.com/prod/process';

-- ============================================================================
-- Grant Execute Permissions
-- ============================================================================

-- Detokenization functions
GRANT USAGE ON FUNCTION DETOK_NAME(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_ID(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_DOB(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_SSN(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_SSN_PARTIAL(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_EMAIL(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- Tokenization functions
GRANT USAGE ON FUNCTION TOK_NAME(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_ID(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_DOB(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_SSN(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_SSN_PARTIAL(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_EMAIL(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- One-way tokenization functions
GRANT USAGE ON FUNCTION TOK_ONEWAY_NAME(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_ONEWAY_ID(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_ONEWAY_DOB(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_ONEWAY_SSN(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- BYOT (Bring Your Own Token) functions
GRANT USAGE ON FUNCTION BYOT_NAME(VARCHAR, VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION BYOT_ID(VARCHAR, VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION BYOT_DOB(VARCHAR, VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION BYOT_SSN(VARCHAR, VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- Query function
GRANT USAGE ON FUNCTION SKYFLOW_QUERY(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- ============================================================================
-- Test the Functions
-- ============================================================================

-- Test regular tokenization
SELECT TOK_NAME('John Doe');
SELECT TOK_ID('12345');
SELECT TOK_DOB('1990-01-01'); -- Returns format with preserved year (e.g., "1990-11-18")
SELECT TOK_SSN('123-45-6789');
SELECT TOK_EMAIL('user@example.com'); -- Skyflow automatically preserves domain (e.g., "abc123def@example.com")

-- Test one-way tokenization (WARNING: Records will be DELETED)
SELECT TOK_ONEWAY_NAME('Jane Smith');
SELECT TOK_ONEWAY_ID('67890');
SELECT TOK_ONEWAY_DOB('1985-06-15'); -- Returns format with preserved year, then deletes record
SELECT TOK_ONEWAY_SSN('987-65-4321');

-- Test detokenization (replace with actual tokens from regular tokenization)
SELECT DETOK_NAME('tok_abc123_your_token_here');
SELECT DETOK_ID('tok_def456_your_token_here');
SELECT DETOK_DOB('1990-11-18'); -- Returns original date "1990-01-01"
SELECT DETOK_SSN('tok_jkl012_your_token_here');
SELECT DETOK_EMAIL('abc123def@example.com'); -- Returns "user@example.com"

-- WARNING: Detokenization of one-way tokens will FAIL (records deleted)
-- SELECT DETOK_NAME('tok_oneway_token_here'); -- This will error: record not found

-- Test BYOT (Bring Your Own Token)
SELECT BYOT_NAME('John Doe', 'my-custom-name-token-123');  -- Returns 'my-custom-name-token-123'
SELECT BYOT_SSN('123-45-6789', 'my-custom-ssn-token-456'); -- Returns 'my-custom-ssn-token-456'
SELECT BYOT_DOB('1990-01-01', '1990-05-16');  -- Returns '1990-05-16' (year preserved format)

-- Verify BYOT tokens work with detokenization
SELECT DETOK_NAME('my-custom-name-token-123');  -- Returns 'John Doe'
SELECT DETOK_SSN('my-custom-ssn-token-456');    -- Returns '123-45-6789'
SELECT DETOK_DOB('1990-05-16');                 -- Returns '1990-01-01'

-- Test with table data
SELECT
    id,
    TOK_NAME(name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_EMAIL(email) as email_token
FROM YOUR_TABLE
LIMIT 5;

-- Test round-trip (tokenize then detokenize)
SELECT DETOK_NAME(TOK_NAME('John Doe')) as should_be_john_doe;
SELECT DETOK_DOB(TOK_DOB('1984-04-25')) as should_be_1984_04_25;
SELECT DETOK_EMAIL(TOK_EMAIL('user@example.com')) as should_be_user_at_example;

-- Test SKYFLOW_QUERY function (privacy-preserving analytics)
-- Note: These examples assume vault data exists

-- Simple query: Count records in dob vault
SELECT value:count::INT as total_dobs
FROM TABLE(FLATTEN(SKYFLOW_QUERY('SELECT COUNT(*) as count FROM dob')));

-- Filter query: Find DOBs in specific range
SELECT
    value:dob::DATE as birth_date,
    value:skyflow_id::VARCHAR as vault_id
FROM TABLE(FLATTEN(
    SKYFLOW_QUERY('SELECT dob, skyflow_id FROM dob WHERE dob >= ''1990-01-01'' AND dob < ''2000-01-01'' LIMIT 25')
));

-- Aggregation: Birth year distribution
SELECT
    value:birth_year::INT as year,
    value:patient_count::INT as count
FROM TABLE(FLATTEN(
    SKYFLOW_QUERY('SELECT EXTRACT(YEAR FROM dob) as birth_year, COUNT(*) as patient_count FROM dob GROUP BY birth_year')
));

-- Substring search: Find names matching pattern (if indexing enabled)
SELECT
    value:name::VARCHAR as name_value,
    value:skyflow_id::VARCHAR as vault_id
FROM TABLE(FLATTEN(
    SKYFLOW_QUERY('SELECT name, skyflow_id FROM name WHERE name ILIKE ''%JOHN%'' LIMIT 25')
));

-- Query emails from specific domain
SELECT
    value:email::VARCHAR as email_value,
    value:skyflow_id::VARCHAR as vault_id
FROM TABLE(FLATTEN(
    SKYFLOW_QUERY('SELECT email, skyflow_id FROM email WHERE email ILIKE ''%@example.com'' LIMIT 25')
));

-- Query with pagination
SELECT value FROM TABLE(FLATTEN(SKYFLOW_QUERY('SELECT * FROM ssn LIMIT 25 OFFSET 0')));  -- First page
SELECT value FROM TABLE(FLATTEN(SKYFLOW_QUERY('SELECT * FROM ssn LIMIT 25 OFFSET 25'))); -- Second page

-- Test year-specific mapping requirement for DOB (year preserved, month-day tokenized)
SELECT
    TOK_DOB('1984-04-25') as token_1984,
    TOK_DOB('1985-04-25') as token_1985;
-- These should produce different tokenized MM-DD values with year preserved
-- e.g., "1984-09-12" vs "1985-10-04"

-- Test domain-specific uniqueness for EMAIL (Skyflow guarantees different tokens)
SELECT
    TOK_EMAIL('john.doe@example.com') as token_example,
    TOK_EMAIL('john.doe@gmail.com') as token_gmail;
-- Skyflow ensures different prefixes for same local part at different domains
-- e.g., "abc123@example.com" vs "xyz789@gmail.com"

-- ============================================================================
-- Verify Function Creation
-- ============================================================================

SHOW FUNCTIONS LIKE 'TOK_%';
SHOW FUNCTIONS LIKE 'DETOK_%';
SHOW FUNCTIONS LIKE 'SKYFLOW_QUERY%';

-- ============================================================================
-- NOTES
-- ============================================================================
-- Function properties:
-- - NULL ON NULL INPUT: Returns NULL if input is NULL (no API call)
-- - IMMUTABLE: Snowflake can cache results (use VOLATILE if tokens change)
--
-- Architecture:
-- - All functions use a single API Gateway endpoint: /process
-- - Control metadata (operation, dataType) passed via HTTP HEADERS (static per function)
-- - Caller identity (user, role, account) passed via CONTEXT_HEADERS (dynamic per call)
-- - This provides audit trail and enables role-based authorization in Lambda
--
-- Headers sent to Lambda (Snowflake prepends 'sf-custom-' and 'sf-context-' prefixes):
-- - sf-custom-x-operation: tokenize | detokenize | tokenize_oneway
-- - sf-custom-x-data-type: NAME | ID | DOB | SSN | EMAIL
-- - sf-context-current-user: <calling Snowflake user>
-- - sf-context-current-role: <calling Snowflake role>
-- - sf-context-current-account: <Snowflake account identifier>
-- - sf-context-current-ip-address: <caller IP address>
--
-- Tokenization:
-- - Uses upsert mode: Same value returns same token (idempotent)
-- - Each data type (NAME, ID, DOB, SSN) uses a separate Skyflow vault/table
-- - Validation is handled by Skyflow (configured vault-side)
--
-- Detokenization:
-- - Retrieves original plaintext value from token
-- - Each data type routes to correct vault/table
-- - Caller context can be used for authorization/audit
--
-- One-Way Tokenization (tokenize_oneway operation):
-- - IRREVERSIBLE tokenization: Records are PERMANENTLY DELETED after token is created
-- - Atomic operation: Token only returned if delete succeeds
-- - Uses naming convention within SAME vault:
--   * Regular: table "name" with column "name"
--   * Oneway: table "name_oneway" with column "name_oneway"
-- - Prevents accidental deletion of regular tokenization records (separate tables)
-- - Detokenization WILL FAIL for one-way tokens (records no longer exist)
-- - Use cases: Compliance requirements for data minimization, right-to-be-forgotten
-- - Setup: Create <table>_oneway tables with <column>_oneway columns in your vaults
-- - Optional: Override defaults with "onewayTable" and "onewayColumn" in config
-- - NOT idempotent: Same value tokenized multiple times returns different tokens
--   (because record is deleted after each tokenization)
--
-- EMAIL (Domain-Preserving Email Tokenization):
-- - Uses Skyflow's built-in email tokenization with automatic domain preservation
-- - Input: "john.doe@example.com"
-- - Output: "abc123def@example.com" (domain preserved, prefix tokenized by Skyflow)
-- - Skyflow automatically ensures same prefix at different domains gets different tokens
-- - Example: "john.doe@example.com" → "abc123@example.com" vs "john.doe@gmail.com" → "xyz789@gmail.com"
--
-- Query Operation (Privacy-Preserving Analytics):
-- - Generic SKYFLOW_QUERY(sql) function executes SQL directly against vault data
-- - Lambda routes queries to appropriate vaults based on table names in SQL:
--   * name → NAME vault
--   * id → ID vault
--   * dob → DOB vault (year-preserving)
--   * ssn → SSN vault
--   * email → EMAIL vault
-- - Enables filtering, aggregation, and JOINs without bringing sensitive data to Snowflake
-- - Returns JSON array of results (use TABLE(FLATTEN(...)) to parse in Snowflake)
-- - Queries run on encrypted data using Skyflow's polymorphic encryption
-- - Supported SQL: SELECT, WHERE, GROUP BY, HAVING, JOIN, LIMIT, OFFSET, aggregations
-- - Limitations (per Skyflow Query API):
--   * Maximum 25 records per query (use LIMIT/OFFSET for pagination)
--   * SELECT statements only (no INSERT/UPDATE/DELETE)
--   * Cannot return tokens (only plaintext values with redaction policies applied)
--   * Cannot modify vault or perform transactions
-- - Use cases:
--   * Age distribution analytics without exposing individual DOBs
--   * Pattern matching searches (e.g., SSNs starting with '123-')
--   * Statistical aggregations (COUNT, AVG, MIN, MAX) on sensitive data
--   * Complex filtering with multiple conditions
-- - Example queries:
--   * COUNT: SKYFLOW_QUERY('SELECT COUNT(*) as count FROM dob')
--   * FILTER: SKYFLOW_QUERY('SELECT dob FROM dob WHERE dob >= ''1990-01-01'' LIMIT 25')
--   * AGGREGATE: SKYFLOW_QUERY('SELECT EXTRACT(YEAR FROM dob) as year, COUNT(*) FROM dob GROUP BY year')
--   * PATTERN: SKYFLOW_QUERY('SELECT name FROM name WHERE name ILIKE ''%JOHN%'' LIMIT 25')
--
-- Performance tips:
-- - Snowflake automatically batches external function calls
-- - Lambda processes records in configurable batch sizes
-- - Configurable concurrency for parallel batch processing
-- - For large datasets, consider using WHERE clauses to limit rows
--
-- Security & Audit:
-- - Lambda receives caller identity for every request
-- - Can implement role-based access control (RBAC) in Lambda
-- - CloudWatch logs include user/role/account for audit trail
-- - Consider logging caller context (without PII) for compliance
--
-- Troubleshooting:
-- - If you get "Function not found", check database/schema context
-- - If calls fail, check CloudWatch logs in AWS Lambda
-- - Verify API Gateway URL points to /process endpoint
-- - Check that vault IDs and table names match your Skyflow configuration
-- - Verify CONTEXT_HEADERS are supported (Snowflake version requirement)
-- ============================================================================
