-- ============================================================================
-- Create Skyflow Tokenize and Detokenize External Functions
-- ============================================================================
-- Creates external functions for both tokenization and detokenization.
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
-- Data-type specific detokenization (each uses specific vault and table)

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_NAME(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_ID(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_DOB(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_SSN(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

-- ============================================================================
-- Tokenization Functions
-- ============================================================================
-- Data-type specific tokenization (each uses specific vault and table)
-- Uses upsert by default - returns existing token for duplicate values

CREATE OR REPLACE EXTERNAL FUNCTION TOK_NAME(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ID(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_DOB(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_SSN(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS 'https://23fv2q9z4i.execute-api.us-east-1.amazonaws.com/prod/detokenize';

-- ============================================================================
-- Grant Execute Permissions
-- ============================================================================

-- Detokenization functions
GRANT USAGE ON FUNCTION DETOK_NAME(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_ID(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_DOB(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION DETOK_SSN(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- Tokenization functions
GRANT USAGE ON FUNCTION TOK_NAME(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_ID(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_DOB(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;
GRANT USAGE ON FUNCTION TOK_SSN(VARCHAR) TO ROLE YOUR_APPLICATION_ROLE;

-- ============================================================================
-- Test the Functions
-- ============================================================================

-- Test tokenization
SELECT TOK_NAME('John Doe');
SELECT TOK_ID('12345');
SELECT TOK_DOB('1990-01-01');
SELECT TOK_SSN('123-45-6789');

-- Test detokenization (replace with actual tokens)
SELECT DETOK_NAME('tok_abc123_your_token_here');
SELECT DETOK_ID('tok_def456_your_token_here');
SELECT DETOK_DOB('tok_ghi789_your_token_here');
SELECT DETOK_SSN('tok_jkl012_your_token_here');

-- Test with table data
SELECT
    id,
    TOK_NAME(name) as name_token,
    TOK_SSN(ssn) as ssn_token
FROM YOUR_TABLE
LIMIT 5;

-- Test round-trip (tokenize then detokenize)
SELECT DETOK_NAME(TOK_NAME('John Doe')) as should_be_john_doe;

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
-- Tokenization:
-- - Uses upsert mode: Same value returns same token (idempotent)
-- - Each data type (NAME, ID, DOB, SSN) uses a separate Skyflow vault/table
-- - Validation is handled by Skyflow (configured vault-side)
--
-- Detokenization:
-- - Retrieves original plaintext value from token
-- - Each data type routes to correct vault/table
--
-- Performance tips:
-- - Snowflake automatically batches external function calls
-- - Lambda processes up to 200 records per batch
-- - Processes up to 10 batches concurrently
-- - For large datasets, consider using WHERE clauses to limit rows
--
-- Troubleshooting:
-- - If you get "Function not found", check database/schema context
-- - If calls fail, check CloudWatch logs in AWS Lambda
-- - Verify API Gateway URL is correct (including stage)
-- - Check that vault IDs and table names match your Skyflow configuration
-- ============================================================================
