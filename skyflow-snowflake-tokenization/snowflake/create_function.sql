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
        'X-Operation' = 'detokenize',
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

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_DOB_PRESERVE_YYYY(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'detokenize',
        'X-Data-Type' = 'DOB_PRESERVE_YYYY'
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
        'X-Operation' = 'tokenize',
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

CREATE OR REPLACE EXTERNAL FUNCTION TOK_DOB_PRESERVE_YYYY(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    HEADERS = (
        'X-Operation' = 'tokenize',
        'X-Data-Type' = 'DOB_PRESERVE_YYYY'
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
GRANT USAGE ON FUNCTION DETOK_DOB_PRESERVE_YYYY(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- Tokenization functions
GRANT USAGE ON FUNCTION TOK_NAME(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_ID(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_DOB(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_SSN(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_DOB_PRESERVE_YYYY(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- ============================================================================
-- Test the Functions
-- ============================================================================

-- Test tokenization
SELECT TOK_NAME('John Doe');
SELECT TOK_ID('12345');
SELECT TOK_DOB('1990-01-01');
SELECT TOK_SSN('123-45-6789');
SELECT TOK_DOB_PRESERVE_YYYY('1984-04-25'); -- Should return format-preserving date like "1984-11-18"

-- Test detokenization (replace with actual tokens)
SELECT DETOK_NAME('tok_abc123_your_token_here');
SELECT DETOK_ID('tok_def456_your_token_here');
SELECT DETOK_DOB('tok_ghi789_your_token_here');
SELECT DETOK_SSN('tok_jkl012_your_token_here');
SELECT DETOK_DOB_PRESERVE_YYYY('1984-11-18'); -- Should return original date "1984-04-25"

-- Test with table data
SELECT
    id,
    TOK_NAME(name) as name_token,
    TOK_SSN(ssn) as ssn_token
FROM YOUR_TABLE
LIMIT 5;

-- Test round-trip (tokenize then detokenize)
SELECT DETOK_NAME(TOK_NAME('John Doe')) as should_be_john_doe;
SELECT DETOK_DOB_PRESERVE_YYYY(TOK_DOB_PRESERVE_YYYY('1984-04-25')) as should_be_1984_04_25;

-- Test year-specific mapping requirement
SELECT
    TOK_DOB_PRESERVE_YYYY('1984-04-25') as token_1984,
    TOK_DOB_PRESERVE_YYYY('1985-04-25') as token_1985;
-- These should produce different tokenized MM-DD values
-- e.g., "1984-09-12" vs "1985-10-04"

-- ============================================================================
-- Verify Function Creation
-- ============================================================================

SHOW FUNCTIONS LIKE 'TOK_%';
SHOW FUNCTIONS LIKE 'DETOK_%';

-- ============================================================================
-- NOTES
-- ============================================================================
-- Function properties:
-- - NULL ON NULL INPUT: Returns NULL if input is NULL (no API call)
-- - IMMUTABLE: Snowflake can cache results (use VOLATILE if tokens change)
--
-- Architecture:
-- - All 10 functions use a single API Gateway endpoint: /process
-- - Control metadata (operation, dataType) passed via HTTP HEADERS (static per function)
-- - Caller identity (user, role, account) passed via CONTEXT_HEADERS (dynamic per call)
-- - This provides audit trail and enables role-based authorization in Lambda
--
-- Headers sent to Lambda (Snowflake prepends 'sf-custom-' and 'sf-context-' prefixes):
-- - sf-custom-x-operation: tokenize | detokenize
-- - sf-custom-x-data-type: NAME | ID | DOB | SSN | DOB_PRESERVE_YYYY
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
-- DOB_PRESERVE_YYYY (Format-Preserving Date Tokenization):
-- - Special implementation for year-preserving date tokenization
-- - Input: "1984-04-25" (YYYY-MM-DD format)
-- - Output: "1984-11-18" (year preserved, MM-DD tokenized to valid date)
-- - Year-specific mapping: Same MM-DD in different years gets different tokens
--   Example: "1984-04-25" → "1984-09-12" vs "1985-04-25" → "1985-10-04"
-- - Skyflow vault schema (month_day table):
--   * dob_full: Original date (plaintext) - "1984-04-25"
--   * dob_year: Extracted year (plaintext, queryable) - "1984"
--   * month_day_token: Tokenized MM-DD (plaintext, queryable) - "09-12"
-- - Tokenization flow (2 API calls):
--   1. Insert dob_full with FPT enabled to get format-preserving token
--   2. Extract month_day_token from FPT result and upsert back to record
-- - Detokenization flow (1 API call):
--   1. Parse FPT token to extract year and month_day_token
--   2. Query Skyflow: WHERE month_day_token='09-12' AND dob_year='1984'
--   3. Return original dob_full value
-- - Skyflow query capability: SELECT * FROM month_day WHERE dob_year = '1984'
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
