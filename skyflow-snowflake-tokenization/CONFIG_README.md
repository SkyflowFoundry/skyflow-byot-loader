# Configuration Files

The deploy script uses **two separate config files**:

## 1. `../config.json` (Optional for AWS, Required for Snowflake setup)

Located at: `/Users/samsternberg/Code/skyflow-byot-loader/config.json`

```json
{
  "aws": {
    "AWS_S3_KEY_ID": "",
    "AWS_S3_SECRET_ACCESS_KEY": "",
    "AWS_DEFAULT_REGION": "us-east-1"
  },
  "snowflake": {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "PUBLIC",
    "role": "YOUR_ROLE"
  }
}
```

**Note:** The `skyflow` section is NOT needed in this file. Skyflow configuration for Lambda comes from `lambda/skyflow-config.json`. The `skyflow` section is only used by the Go loader (`main.go`) in the parent directory.

**When is this file required?**

- **AWS credentials**: Optional if you have AWS CLI configured (`aws configure`)
  - Deploy script will use AWS CLI credentials by default
  - Only needed if AWS CLI is not configured

- **Snowflake credentials**: Required ONLY for:
  - `--deploy-e2e` or `--deploy-e2e-secrets` (end-to-end deployment with Snowflake setup)
  - `--setup-snowflake` (manual Snowflake setup)
  - `--test` (testing Snowflake integration)
  - `--destroy` (cleanup Snowflake resources)

**When is this file NOT required?**

- `--deploy` or `--deploy-secrets` (AWS-only deployment)
  - If AWS CLI is configured
  - Lambda will use `lambda/skyflow-config.json` for Skyflow credentials

## 2. `lambda/skyflow-config.json` (Required for Lambda)

Located at: `skyflow-snowflake-tokenization/lambda/skyflow-config.json`

This file contains Skyflow vault configuration used by the Lambda function.

```json
{
  "credentials": {
    "apiKey": "YOUR_SKYFLOW_API_KEY"
  },
  "vaults": {
    "vaultUrl": "https://YOUR_CLUSTER_ID.vault.skyflowapis.com",
    "definitions": [
      {
        "vaultId": "YOUR_VAULT_ID",
        "table": "name",
        "column": "name",
        "dataType": "NAME",
        "transformations": { ... }
      }
    ]
  },
  "tokenizeBatchSize": 5,
  "tokenizeMaxConcurrency": 400,
  "detokenizeBatchSize": 100,
  "detokenizeMaxConcurrency": 500,
  "logLevel": "ERROR"
}
```

**When is this file used?**

- Always required for Lambda deployment
- With `--deploy` or `--deploy-e2e`: Loaded as environment variables in Lambda
- With `--deploy-secrets` or `--deploy-e2e-secrets`: Uploaded to AWS Secrets Manager

## Recommended Setup

**For development (file-based config):**
```bash
# Configure AWS CLI (so you don't need AWS credentials in config.json)
aws configure

# Create minimal config.json with just Snowflake credentials
cat > /Users/samsternberg/Code/skyflow-byot-loader/config.json << 'EOF'
{
  "snowflake": {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "PUBLIC",
    "role": "YOUR_ROLE"
  }
}
EOF

# Deploy
./deploy.sh --deploy-e2e
```

**For production (Secrets Manager):**
```bash
# Configure AWS CLI
aws configure

# Create config.json with Snowflake credentials (for setup)
# Create lambda/skyflow-config.json (will be uploaded to Secrets Manager)

# Deploy
./deploy.sh --deploy-e2e-secrets
```
