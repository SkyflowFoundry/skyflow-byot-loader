# Skyflow for Snowflake - Tokenization & Detokenization

Integration for tokenizing and detokenizing sensitive data directly in Snowflake queries using Skyflow's privacy vault, AWS Lambda, and API Gateway.

## Overview

This project provides both **tokenization** and **detokenization** capabilities within Snowflake, allowing you to protect sensitive data (PII, PHI, PCI) using Skyflow's data privacy vault while maintaining SQL query flexibility.

```sql
-- Tokenize sensitive data
SELECT TOK_NAME('John Doe'), TOK_SSN('123-45-6789');

-- Detokenize for authorized access
SELECT DETOK_NAME(name_token), DETOK_SSN(ssn_token) FROM patients;

-- Round-trip verification
SELECT DETOK_NAME(TOK_NAME('Jane Smith')) as should_be_jane_smith;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Snowflake Query                                             │
│ SELECT TOK_NAME(name), DETOK_SSN(ssn_token) FROM MY_TABLE  │
└────────────────────┬────────────────────────────────────────┘
                     │ HTTPS (IAM Auth)
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ AWS API Gateway                                             │
│ - Routes: /tokenize/{datatype}, /detokenize/{datatype}    │
│ - IAM Role Authentication                                   │
│ - Rate Limiting (10K requests/second)                       │
└────────────────────┬────────────────────────────────────────┘
                     │ Invoke
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ AWS Lambda Function (Node.js 20.x or Go 1.x)               │
│ - Official Skyflow Node.js SDK v2.0.0                      │
│ - Batch processing (SDK-managed)                           │
│ - Upsert mode for tokenization (idempotent)                │
│ - Data-type specific vault routing                         │
└────────────────────┬────────────────────────────────────────┘
                     │ HTTPS
                     ▼
┌─────────────────────────────────────────────────────────────┐
│ Skyflow APIs                                                │
│ - Tokenize: POST /v1/vaults/{vault_id}/{table}             │
│ - Detokenize: POST /v1/vaults/{vault_id}/detokenize        │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Options

Choose the implementation that best fits your requirements:

### Node.js Implementation (Recommended)

**Best for:** Rapid deployment, AWS Secrets Manager integration, easy maintenance

**Features:**
- ✅ Official Skyflow Node.js SDK v2.0.0
- ✅ Simplified codebase with official support
- ✅ AWS Secrets Manager integration (secure credential storage)
- ✅ Automatic batch processing via SDK
- ✅ Data-type specific vault routing
- ✅ Easy to modify and extend
- ✅ **Distribution package available for customers**

**Setup Methods:**
1. **Automated (Recommended)**: Use `deploy.sh` script for end-to-end deployment
2. **Manual**: Follow `lambda/Skyflow-for-Snowflake-Deployment-Guide.md` for AWS CLI commands

**Performance:** ~3-5 seconds for 1,000 tokens

**Files:** `lambda/` directory (Node.js implementation)

### Go Implementation

**Best for:** Maximum performance, minimal cold start time, production scale

**Features:**
- ✅ Custom HTTP client implementation
- ✅ Minimal cold start (~50ms vs ~150ms for Node.js)
- ✅ Lower memory footprint
- ✅ Compiled binary (no runtime dependencies)
- ✅ Direct Skyflow API integration

**Setup:** See `../skyflow-snowflake-tokenization-go-minimal/` directory

**Performance:** ~2-3 seconds for 1,000 tokens

**Files:** Separate Go repository with modular architecture

---

## Quick Start (Node.js - Automated)

Get up and running in 10 minutes with the automated deployment script:

### Prerequisites
- AWS Account with CLI configured
- Snowflake account with ACCOUNTADMIN role
- Skyflow vault with credentials
- Node.js 18+ installed
- `jq` installed (`brew install jq` on Mac)
- `snowsql` CLI installed (for automated Snowflake setup)

### Step 1: Configure Credentials

```bash
# Copy example configuration
cp config.example.json config.json

# Edit with your credentials
vim config.json
```

Required configuration:
```json
{
  "skyflow": {
    "vault_url": "https://YOUR_VAULT.vault.skyflowapis.com",
    "bearer_token": "YOUR_BEARER_TOKEN",
    "vaults": { "default": "YOUR_VAULT_ID" }
  },
  "aws": {
    "AWS_S3_KEY_ID": "YOUR_AWS_KEY",
    "AWS_S3_SECRET_ACCESS_KEY": "YOUR_AWS_SECRET",
    "AWS_DEFAULT_REGION": "us-east-1"
  },
  "snowflake": {
    "account": "ABC12345.us-east-1",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "database": "YOUR_DATABASE",
    "schema": "YOUR_SCHEMA",
    "warehouse": "YOUR_WAREHOUSE",
    "role": "ACCOUNTADMIN"
  }
}
```

### Step 2: Setup AWS Permissions (One-Time)

```bash
# Grant required AWS permissions to your IAM user
./deploy.sh --setup-permissions your-iam-username
```

### Step 3: Deploy Everything

```bash
# Option A: Deploy AWS + Snowflake in one command
./deploy.sh --deploy-e2e

# Option B: Step-by-step deployment
./deploy.sh --deploy              # Deploy Lambda + API Gateway
./deploy.sh --setup-snowflake     # Setup Snowflake integration
```

### Step 4: Test

```bash
# Automated test
./deploy.sh --test

# Or test directly in Snowflake
SELECT TOK_NAME('John Doe');
SELECT DETOK_NAME(TOK_NAME('John Doe'));
```

**That's it!** 🎉 You're now ready to tokenize and detokenize data in Snowflake.

---

## Quick Start (Node.js - Manual)

For customers who prefer AWS CLI commands over automation scripts:

### Get the Distribution Package

Download `skyflow-snowflake-tokenization-customer.zip` containing:
- Lambda implementation files (`config.js`, `skyflow-client.js`, `handler.js`)
- Configuration template (`secrets-manager-config.json`)
- Complete deployment guide with step-by-step AWS CLI commands

### Follow the Guide

Extract the zip and follow `Skyflow-for-Snowflake-Deployment-Guide.md` for:
- AWS CLI commands for Lambda, API Gateway, IAM roles
- Secrets Manager setup
- Snowflake SQL commands for external functions
- Testing and troubleshooting

**No scripts required** - just copy-paste AWS CLI commands.

---

## Features

### Core Capabilities
- ✅ **Bidirectional Operations**: Both tokenization and detokenization
- ✅ **Batch Processing**: SDK-managed batch operations
- ✅ **Upsert Mode**: Same plaintext always returns same token (idempotent)
- ✅ **Multi-Vault Support**: Route different data types to separate vaults
- ✅ **Error Handling**: Comprehensive error handling with continueOnError
- ✅ **Official SDK**: Built on Skyflow Node.js SDK v2.0.0

### Security & Compliance
- ✅ **AWS Secrets Manager**: Secure credential storage with rotation support
- ✅ **IAM Authentication**: Snowflake-to-AWS trust with external ID
- ✅ **CloudWatch Logging**: Audit trail for all operations
- ✅ **Least Privilege IAM**: Minimal required permissions

### Deployment Options
- ✅ **Automated Script**: One-command deployment with `deploy.sh`
- ✅ **Manual Deployment**: AWS CLI commands for full control
- ✅ **Environment Variables**: Alternative to Secrets Manager for testing

---

## Performance

### Throughput Estimates

| Dataset Size | Node.js Time | Go Time | Cost (approx) |
|-------------|-------------|---------|---------------|
| 100 tokens | ~1-2 seconds | ~0.5-1 second | $0.0001 |
| 1,000 tokens | ~3-5 seconds | ~2-3 seconds | $0.001 |
| 10,000 tokens | ~20-30 seconds | ~15-20 seconds | $0.01 |
| 100,000 tokens | ~3-5 minutes | ~2-3 minutes | $0.10 |
| 1,000,000 tokens | ~30-50 minutes | ~20-30 minutes | $1.00 |

### SDK Integration Benefits
- **Official Support**: Maintained by Skyflow engineering
- **Simplified Codebase**: ~50% fewer lines of code
- **Automatic Updates**: Bug fixes and features from SDK updates
- **Type Safety**: TypeScript definitions included

---

## Project Structure

```
skyflow-snowflake-tokenization/          # Node.js implementation (this directory)
├── README.md                            # This file
├── QUICKSTART.md                        # 5-minute setup guide (automated)
├── deploy.sh                            # Deployment automation script
├── config.example.json                  # Configuration template
│
├── lambda/                              # Node.js Lambda implementation
│   ├── config.js                        # AWS Secrets Manager integration
│   ├── skyflow-client.js                # Skyflow SDK client wrapper
│   ├── handler.js                       # Lambda entry point
│   ├── package.json                     # Node.js dependencies (includes skyflow-node SDK)
│   ├── secrets-manager-config.json      # Secrets Manager template
│   └── Skyflow-for-Snowflake-Deployment-Guide.md  # Manual setup guide
│
├── snowflake/                           # Snowflake SQL scripts
│   ├── setup.sql                        # API integration setup
│   ├── create_function.sql              # External function definitions (8 functions)
│   └── examples.sql                     # Usage examples
│
└── skyflow-snowflake-tokenization-customer.zip  # Customer distribution package

../skyflow-snowflake-tokenization-go-minimal/   # Go implementation (separate)
├── README.md                            # Go-specific documentation
├── cmd/lambda/main.go                   # Go Lambda entry point
├── internal/                            # Go modules (handler, skyflow, config)
└── build/                               # Compiled binaries
```

---

## Usage Examples

### Basic Tokenization

```sql
-- Tokenize single values
SELECT TOK_NAME('John Doe') as name_token;
SELECT TOK_SSN('123-45-6789') as ssn_token;
SELECT TOK_DOB('1990-01-01') as dob_token;
SELECT TOK_ID('12345') as id_token;
```

### Basic Detokenization

```sql
-- Detokenize single tokens
SELECT DETOK_NAME('tok_abc123') as name;
SELECT DETOK_SSN('tok_def456') as ssn;
```

### Tokenize Table Data

```sql
-- Create tokenized table from plaintext
CREATE OR REPLACE TABLE patients_tokenized AS
SELECT
    patient_id,
    TOK_NAME(patient_name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(date_of_birth) as dob_token,
    admission_date,
    department
FROM patients_raw;
```

### Detokenize for Analysis

```sql
-- Detokenize for authorized users only
SELECT
    patient_id,
    DETOK_NAME(name_token) as patient_name,
    DETOK_SSN(ssn_token) as ssn,
    DETOK_DOB(dob_token) as date_of_birth
FROM patients_tokenized
WHERE admission_date > '2024-01-01'
LIMIT 100;
```

### Round-Trip Testing

```sql
-- Verify data integrity
SELECT
    'John Doe' as original,
    TOK_NAME('John Doe') as token,
    DETOK_NAME(TOK_NAME('John Doe')) as roundtrip,
    CASE
        WHEN 'John Doe' = DETOK_NAME(TOK_NAME('John Doe')) THEN '✓ PASS'
        ELSE '✗ FAIL'
    END as test_result;
```

### Batch Processing

```sql
-- Snowflake automatically batches external function calls
SELECT
    customer_id,
    TOK_NAME(name) as name_token,
    TOK_SSN(ssn) as ssn_token,
    TOK_DOB(dob) as dob_token
FROM customers
LIMIT 10000;  -- Processes ~100-200 at a time internally
```

**See `snowflake/examples.sql` for 20+ comprehensive examples.**

---

## Configuration

### AWS Secrets Manager (Recommended for Production)

Store credentials securely in AWS Secrets Manager:

```json
{
  "credentials": {
    "apiKey": "sky-xyz-your-token"
  },
  "vaults": [
    {
      "vaultId": "vault-id-for-names",
      "clusterId": "your-cluster-id",
      "table": "persons",
      "column": "name",
      "dataType": "NAME"
    },
    {
      "vaultId": "vault-id-for-ssns",
      "clusterId": "your-cluster-id",
      "table": "persons",
      "column": "ssn",
      "dataType": "SSN"
    }
  ],
  "logLevel": "INFO"
}
```

### Environment Variables (Alternative)

For testing or non-Secrets Manager deployments:

```bash
export SKYFLOW_API_KEY="your-api-key-or-bearer-token"
export VAULT_ID_NAME="vault-id-for-names"
export CLUSTER_ID_NAME="your-cluster-id"
export TABLE_NAME="persons"
export COLUMN_NAME="name"
# Repeat for ID, DOB, SSN data types
```

### SDK Configuration

The Skyflow Node.js SDK manages batching and performance internally. Configuration focuses on vault routing:

```json
{
  "logLevel": "INFO"  // DEBUG, INFO, WARN, ERROR
}
```

**Note:** Batch size, concurrency, and retry logic are now managed by the SDK for optimal performance.

---

## Monitoring

### CloudWatch Metrics

Monitor Lambda performance:
- Invocations
- Errors
- Duration (p50, p99)
- Throttles
- Concurrent executions

### CloudWatch Logs

```bash
# View Lambda logs in real-time
aws logs tail /aws/lambda/skyflow-tokenization --follow --region us-east-1

# Look for indicators:
# - "SkyflowClient initialized with SDK"
# - "SDK insert completed in Xms"
# - Request/response timing
```

### Snowflake Query History

```sql
-- Check external function performance
SELECT
    query_id,
    query_text,
    execution_time,
    error_message
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text LIKE '%TOK_%' OR query_text LIKE '%DETOK_%'
ORDER BY start_time DESC
LIMIT 100;
```

---

## Troubleshooting

### Common Issues

**1. "Function not found" error**
```sql
-- Check if functions exist
SHOW FUNCTIONS LIKE 'TOK_%';
SHOW FUNCTIONS LIKE 'DETOK_%';

-- Recreate if needed
@snowflake/create_function.sql
```

**2. "Missing credentials.apiKey in configuration"**
```bash
# Check Lambda environment variables
aws lambda get-function-configuration \
    --function-name skyflow-tokenization \
    --region us-east-1 \
    --query 'Environment.Variables'

# Should show: SECRETS_MANAGER_SECRET_NAME=skyflow-tokenization-config
```

**3. "HTTP 401 Unauthorized"**
- Verify API key is valid in Secrets Manager
- Check key has required permissions in Skyflow
- Ensure vaultId and clusterId are correct

**4. "Access denied" or IAM errors**
- Update Snowflake IAM role trust policy with correct ARN and External ID
- Verify API Gateway has invoke permissions on Lambda
- Check Lambda role has Secrets Manager read permissions

**5. Slow performance**
- Increase Lambda memory (512MB → 1024MB)
- Check CloudWatch logs for SDK timing
- Verify data is being batched properly

### Debug Commands

```bash
# Test Lambda directly
aws lambda invoke \
    --function-name skyflow-tokenization \
    --payload '{"path":"/tokenize/name","body":"{\"data\":[[0,\"John Doe\"]]}"}' \
    --region us-east-1 \
    response.json

# Check Lambda policy
aws lambda get-policy \
    --function-name skyflow-tokenization \
    --region us-east-1

# Check IAM role trust policy
aws iam get-role \
    --role-name SnowflakeAPIRole \
    --query 'Role.AssumeRolePolicyDocument'
```

---

## Security Considerations

1. **Use AWS Secrets Manager** for credentials (avoid hard-coding or using environment variables for sensitive data)
2. **Enable CloudWatch Logs encryption** with KMS keys
3. **Implement least-privilege IAM roles** (use provided policies as starting point)
4. **Use VPC endpoints** if Snowflake is in private VPC
5. **Enable API Gateway request validation** to prevent malformed requests
6. **Set Snowflake role permissions** to restrict who can use external functions
7. **Monitor CloudWatch Logs** for suspicious activity or errors

---

## Cost Optimization

### Lambda Optimization
1. **Right-size memory**: Start at 512MB, measure, adjust (more memory = faster = potentially cheaper)
2. **Enable Lambda Insights**: Monitor memory utilization and cold starts
3. **Use reserved concurrency**: Prevents runaway costs from unexpected load

### API Gateway Optimization
1. **Set throttling limits**: Prevent abuse (start at 10,000 requests/second)
2. **Enable caching**: Cache tokenization results for frequently accessed data (advanced)
3. **Monitor usage**: Set CloudWatch alarms for cost thresholds

### Skyflow Optimization
1. **Use upsert mode**: Prevents duplicate tokens (already enabled)
2. **Batch operations**: Always process in batches (already optimized)
3. **Monitor API quotas**: Check Skyflow dashboard for rate limits

**Typical monthly cost for moderate usage (10M tokens/month):** $10-50 (Lambda + API Gateway + Skyflow API calls)

---

## Deployment Commands Reference

### Automated Deployment (deploy.sh)

```bash
# Setup (one-time)
./deploy.sh --setup-permissions <iam-username>

# Deploy everything
./deploy.sh --deploy-e2e              # AWS + Snowflake
./deploy.sh --deploy-e2e --database MY_DB --schema MY_SCHEMA

# Step-by-step
./deploy.sh --deploy                  # AWS only
./deploy.sh --setup-snowflake         # Snowflake only
./deploy.sh --test                    # Test functions

# Updates
./deploy.sh --redeploy                # AWS only (faster)
./deploy.sh --redeploy-e2e            # AWS + Snowflake (full)

# Cleanup
./deploy.sh --destroy                 # Remove all resources
```

### Manual Deployment (AWS CLI)

See `lambda/Skyflow-for-Snowflake-Deployment-Guide.md` for complete AWS CLI commands.

---

## Support & Resources

### Documentation
- **Quick Start**: See [QUICKSTART.md](QUICKSTART.md) for 5-minute automated setup
- **Manual Setup**: See [lambda/Skyflow-for-Snowflake-Deployment-Guide.md](lambda/Skyflow-for-Snowflake-Deployment-Guide.md)
- **SQL Examples**: See [snowflake/examples.sql](snowflake/examples.sql) for 20+ usage examples
- **Go Implementation**: See `../skyflow-snowflake-tokenization-go-minimal/README.md`

### Getting Help
- **Skyflow Support**: https://support.skyflow.com
- **AWS Support**: https://aws.amazon.com/support
- **Snowflake Support**: https://support.snowflake.com

### Common Questions

**Q: Should I use Node.js or Go?**
A: Node.js for rapid deployment and ease of maintenance. Go for maximum performance and minimal cold starts.

**Q: Can I use environment variables instead of Secrets Manager?**
A: Yes, but Secrets Manager is recommended for secure credential storage and rotation support.

**Q: How do I update the Lambda code?**
A: Run `./deploy.sh --redeploy` or use `aws lambda update-function-code` with new ZIP.

**Q: Can I customize the data type mappings?**
A: Yes, edit the `data_type_mappings` in Secrets Manager configuration or config.json.

**Q: What's the difference between the automated and manual setup?**
A: Automated uses `deploy.sh` for one-command deployment. Manual uses AWS CLI commands for full control. Same end result.

---

**Built with ❤️ for Skyflow customers**

Ready to protect your sensitive data in Snowflake? Get started with the [Quick Start](#quick-start-nodejs---automated) guide above.
