-- ============================================================================
-- Skyflow Snowflake Tokenization and Detokenization Setup
-- ============================================================================
-- This script sets up the API integration and external functions for
-- tokenizing and detokenizing Skyflow data directly in Snowflake queries.
--
-- Prerequisites:
-- 1. ACCOUNTADMIN role or appropriate permissions
-- 2. AWS API Gateway URL from Lambda deployment
-- 3. AWS IAM role ARN for Snowflake
--
-- Usage:
--   snowsql -f setup.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 1: Create API Integration
-- ============================================================================
-- Replace the following values:
--   - API_AWS_ROLE_ARN: IAM role ARN that Snowflake will assume
--   - API_ALLOWED_PREFIXES: Your API Gateway URL prefix

CREATE OR REPLACE API INTEGRATION skyflow_api_integration
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = 'YOUR_IAM_ROLE_ARN'
    ENABLED = TRUE
    API_ALLOWED_PREFIXES = ('https://YOUR_API_ID.execute-api.us-east-1.amazonaws.com/');

-- ============================================================================
-- STEP 2: Get Trust Policy for AWS IAM Role
-- ============================================================================
-- Run this to get the trust policy you need to configure in AWS IAM:

DESC API INTEGRATION skyflow_api_integration;

-- Look for these fields in the output:
--   API_AWS_IAM_USER_ARN: Copy this ARN
--   API_AWS_EXTERNAL_ID: Copy this External ID
--
-- Then in AWS IAM, create/update the role with this trust policy:
/*
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<API_AWS_IAM_USER_ARN from above>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<API_AWS_EXTERNAL_ID from above>"
        }
      }
    }
  ]
}
*/

-- ============================================================================
-- STEP 3: Test API Integration
-- ============================================================================

SHOW API INTEGRATIONS LIKE 'skyflow_api_integration';

-- ============================================================================
-- STEP 4: Grant Usage to Appropriate Roles
-- ============================================================================
-- Grant to roles that need to use the detokenize function

GRANT USAGE ON INTEGRATION skyflow_api_integration TO ROLE SYSADMIN;
GRANT USAGE ON INTEGRATION skyflow_api_integration TO ROLE YOUR_APPLICATION_ROLE;

-- ============================================================================
-- NOTES
-- ============================================================================
-- After completing these steps:
-- 1. Run create_function.sql to create the external functions (8 functions total)
--    - 4 detokenization functions: DETOK_NAME, DETOK_ID, DETOK_DOB, DETOK_SSN
--    - 4 tokenization functions: TOK_NAME, TOK_ID, TOK_DOB, TOK_SSN
-- 2. Test with examples.sql
--
-- Troubleshooting:
-- - If you get "API Integration not found", check ACCOUNTADMIN role
-- - If function calls fail, verify the trust policy in AWS IAM
-- - Check CloudWatch logs in AWS for Lambda errors
-- - Verify API Gateway has both /tokenize and /detokenize routes
-- ============================================================================
