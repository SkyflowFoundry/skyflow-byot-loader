#!/bin/bash

# ============================================================================
# Skyflow Snowflake Detokenization - Deployment Script
# ============================================================================
# Deploys Lambda function and API Gateway using AWS CLI
#
# Usage:
#   ./deploy.sh --deploy    Deploy all resources
#   ./deploy.sh --destroy   Destroy all resources
# ============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="skyflow-tokenization"
LAMBDA_FUNCTION_NAME="${PROJECT_NAME}"
API_NAME="${PROJECT_NAME}-api"
IAM_ROLE_NAME="SnowflakeAPIRole"
LAMBDA_ROLE_NAME="${PROJECT_NAME}-lambda-role"
SECRET_NAME="${PROJECT_NAME}-config"

# Load configuration
CONFIG_FILE="../config.json"
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${RED}Error: config.json not found at $CONFIG_FILE${NC}"
    exit 1
fi

# Extract AWS credentials from config
AWS_ACCESS_KEY_ID=$(jq -r '.aws.AWS_S3_KEY_ID' "$CONFIG_FILE")
AWS_SECRET_ACCESS_KEY=$(jq -r '.aws.AWS_S3_SECRET_ACCESS_KEY' "$CONFIG_FILE")
AWS_REGION=$(jq -r '.aws.AWS_DEFAULT_REGION // "us-east-1"' "$CONFIG_FILE")

# Extract Skyflow configuration
SKYFLOW_VAULT_URL=$(jq -r '.skyflow.vault_url' "$CONFIG_FILE")
SKYFLOW_BEARER_TOKEN=$(jq -r '.skyflow.bearer_token' "$CONFIG_FILE")
# Get first vault ID from array (if array) or object (if object)
DEFAULT_VAULT_ID=$(jq -r 'if .skyflow.vaults | type == "array" then .skyflow.vaults[0].id else .skyflow.vaults.default // (.skyflow.vaults | to_entries | first | .value) end' "$CONFIG_FILE")

# Validate configuration
if [ "$AWS_ACCESS_KEY_ID" == "null" ] || [ "$AWS_SECRET_ACCESS_KEY" == "null" ]; then
    echo -e "${RED}Error: AWS credentials not found in config.json${NC}"
    exit 1
fi

if [ "$SKYFLOW_VAULT_URL" == "null" ] || [ "$SKYFLOW_BEARER_TOKEN" == "null" ]; then
    echo -e "${RED}Error: Skyflow credentials not found in config.json${NC}"
    exit 1
fi

# Set AWS credentials
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_REGION

# Get AWS account ID and user info
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_USER_ARN=$(aws sts get-caller-identity --query Arn --output text)

echo -e "${BLUE}============================================================================${NC}"
echo -e "${BLUE}Skyflow Snowflake Tokenization - Deployment${NC}"
echo -e "${BLUE}============================================================================${NC}"
echo -e "AWS Account: ${GREEN}${AWS_ACCOUNT_ID}${NC}"
echo -e "AWS Region: ${GREEN}${AWS_REGION}${NC}"
echo -e "AWS User: ${GREEN}${AWS_USER_ARN}${NC}"
echo -e "Project: ${GREEN}${PROJECT_NAME}${NC}"
echo ""

# Check AWS permissions before proceeding
echo -e "${BLUE}Checking AWS permissions...${NC}"

MISSING_PERMISSIONS=()

# Check Lambda permissions
aws lambda list-functions --max-items 1 > /dev/null 2>&1 || MISSING_PERMISSIONS+=("lambda:ListFunctions")

# Check API Gateway permissions
aws apigateway get-rest-apis --limit 1 > /dev/null 2>&1 || MISSING_PERMISSIONS+=("apigateway:GET")

# Check IAM permissions
aws iam list-roles --max-items 1 > /dev/null 2>&1 || MISSING_PERMISSIONS+=("iam:ListRoles")

if [ ${#MISSING_PERMISSIONS[@]} -gt 0 ]; then
    echo -e "${RED}✗ Missing required AWS permissions:${NC}"
    for perm in "${MISSING_PERMISSIONS[@]}"; do
        echo -e "  ${RED}- ${perm}${NC}"
    done
    echo ""
    echo -e "${YELLOW}Required AWS permissions for this script:${NC}"
    echo -e "  IAM:"
    echo -e "    - iam:CreateRole"
    echo -e "    - iam:GetRole"
    echo -e "    - iam:AttachRolePolicy"
    echo -e "    - iam:PutRolePolicy"
    echo -e "    - iam:DeleteRole"
    echo -e "    - iam:DeleteRolePolicy"
    echo -e "    - iam:DetachRolePolicy"
    echo -e "    - iam:ListRoles"
    echo ""
    echo -e "  Lambda:"
    echo -e "    - lambda:CreateFunction"
    echo -e "    - lambda:GetFunction"
    echo -e "    - lambda:UpdateFunctionCode"
    echo -e "    - lambda:UpdateFunctionConfiguration"
    echo -e "    - lambda:DeleteFunction"
    echo -e "    - lambda:AddPermission"
    echo -e "    - lambda:ListFunctions"
    echo ""
    echo -e "  API Gateway:"
    echo -e "    - apigateway:*"
    echo ""
    echo -e "${YELLOW}Please add these permissions to your IAM user/role:${NC}"
    echo -e "  ${GREEN}${AWS_USER_ARN}${NC}"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ AWS permissions verified${NC}"
echo ""

# ============================================================================
# Deploy Function
# ============================================================================
deploy() {
    echo -e "${YELLOW}Starting deployment...${NC}"
    echo ""

    # Step 1: Create/Update AWS Secrets Manager secret
    echo -e "${BLUE}[1/8]${NC} Creating/Updating AWS Secrets Manager secret..."

    # Check if secrets-manager-config.json exists
    if [ ! -f "lambda/secrets-manager-config.json" ]; then
        echo -e "${RED}✗ Error: lambda/secrets-manager-config.json not found${NC}"
        echo -e "${YELLOW}Please create lambda/secrets-manager-config.json with your Skyflow credentials${NC}"
        echo -e "${YELLOW}See lambda/secrets-manager-config.example.json for format${NC}"
        exit 1
    fi

    # Check if secret exists and get its status
    SECRET_INFO=$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$AWS_REGION" 2>/dev/null || echo "")
    SECRET_DELETED=$(echo "$SECRET_INFO" | grep -i "DeletedDate" || echo "")

    if [ -z "$SECRET_INFO" ] || [ -n "$SECRET_DELETED" ]; then
        # Secret doesn't exist or is deleted - create new one
        # If recently deleted, this might fail, so retry with delay
        MAX_RETRIES=3
        RETRY_COUNT=0

        while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
            if aws secretsmanager create-secret \
                --name "$SECRET_NAME" \
                --description "Skyflow configuration for Snowflake tokenization Lambda" \
                --secret-string file://lambda/secrets-manager-config.json \
                --region "$AWS_REGION" > /dev/null 2>&1; then
                echo -e "${GREEN}✓ Secret created: ${SECRET_NAME}${NC}"
                break
            else
                RETRY_COUNT=$((RETRY_COUNT + 1))
                if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                    echo -e "${YELLOW}Secret creation failed (attempt $RETRY_COUNT/$MAX_RETRIES), retrying in 3s...${NC}"
                    sleep 3
                else
                    echo -e "${RED}✗ Failed to create secret after $MAX_RETRIES attempts${NC}"
                    exit 1
                fi
            fi
        done
    else
        # Secret exists and is not deleted - update it
        aws secretsmanager put-secret-value \
            --secret-id "$SECRET_NAME" \
            --secret-string file://lambda/secrets-manager-config.json \
            --region "$AWS_REGION" > /dev/null
        echo -e "${GREEN}✓ Secret updated: ${SECRET_NAME}${NC}"
    fi
    echo ""

    # Step 2: Create Lambda execution role
    echo -e "${BLUE}[2/8]${NC} Creating Lambda execution role..."

    LAMBDA_ROLE_ARN=$(aws iam get-role --role-name "$LAMBDA_ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || echo "")

    if [ -z "$LAMBDA_ROLE_ARN" ]; then
        # Create trust policy
        cat > /tmp/lambda-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

        aws iam create-role \
            --role-name "$LAMBDA_ROLE_NAME" \
            --assume-role-policy-document file:///tmp/lambda-trust-policy.json \
            --description "Execution role for Skyflow tokenization and detokenization Lambda"

        # Attach basic execution policy
        aws iam attach-role-policy \
            --role-name "$LAMBDA_ROLE_NAME" \
            --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

        # Add Secrets Manager read permissions for V2 (if using Secrets Manager)
        cat > /tmp/lambda-secrets-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:${AWS_REGION}:${AWS_ACCOUNT_ID}:secret:skyflow-tokenization-*"
    }
  ]
}
EOF

        aws iam put-role-policy \
            --role-name "$LAMBDA_ROLE_NAME" \
            --policy-name "SecretsManagerReadPolicy" \
            --policy-document file:///tmp/lambda-secrets-policy.json

        rm -f /tmp/lambda-secrets-policy.json

        # Wait for role to be available
        echo "Waiting for IAM role to propagate..."
        sleep 10

        LAMBDA_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${LAMBDA_ROLE_NAME}"
    fi

    echo -e "${GREEN}✓ Lambda role ready: ${LAMBDA_ROLE_ARN}${NC}"
    echo ""

    # Step 3: Package Lambda function
    echo -e "${BLUE}[3/8]${NC} Packaging Lambda function..."

    cd lambda

    # Install dependencies (if any)
    if [ -f package.json ]; then
        npm install --production --silent 2>/dev/null || true
    fi

    # Create deployment package with modular files
    zip -r function.zip config.js skyflow-client.js handler.js package.json node_modules/ 2>/dev/null || \
    zip -r function.zip config.js skyflow-client.js handler.js package.json

    echo -e "${GREEN}✓ Lambda package created: lambda/function.zip${NC}"
    echo ""

    # Step 4: Create or update Lambda function
    echo -e "${BLUE}[4/8]${NC} Deploying Lambda function..."

    FUNCTION_EXISTS=$(aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" 2>/dev/null || echo "")

    if [ -z "$FUNCTION_EXISTS" ]; then
        # Create new function with Secrets Manager environment variable
        LAMBDA_ARN=$(aws lambda create-function \
            --function-name "$LAMBDA_FUNCTION_NAME" \
            --runtime nodejs20.x \
            --role "$LAMBDA_ROLE_ARN" \
            --handler handler.handler \
            --zip-file fileb://function.zip \
            --timeout 60 \
            --memory-size 512 \
            --description "Skyflow tokenization and detokenization for Snowflake" \
            --environment "Variables={SECRETS_MANAGER_SECRET_NAME=${SECRET_NAME}}" \
            --query 'FunctionArn' \
            --output text)
    else
        # Update existing function
        aws lambda update-function-code \
            --function-name "$LAMBDA_FUNCTION_NAME" \
            --zip-file fileb://function.zip > /dev/null

        aws lambda update-function-configuration \
            --function-name "$LAMBDA_FUNCTION_NAME" \
            --timeout 60 \
            --memory-size 512 \
            --environment "Variables={SECRETS_MANAGER_SECRET_NAME=${SECRET_NAME}}" > /dev/null

        LAMBDA_ARN=$(aws lambda get-function --function-name "$LAMBDA_FUNCTION_NAME" --query 'Configuration.FunctionArn' --output text)
    fi

    cd ..

    echo -e "${GREEN}✓ Lambda function deployed: ${LAMBDA_ARN}${NC}"
    echo ""

    # Step 5: Create API Gateway REST API
    echo -e "${BLUE}[5/8]${NC} Creating API Gateway..."

    # Check if API already exists
    API_ID=$(aws apigateway get-rest-apis --query "items[?name=='${API_NAME}'].id" --output text)

    if [ -z "$API_ID" ]; then
        API_ID=$(aws apigateway create-rest-api \
            --name "$API_NAME" \
            --description "API for Skyflow tokenization and detokenization" \
            --endpoint-configuration types=REGIONAL \
            --query 'id' \
            --output text)
    fi

    echo -e "${GREEN}✓ API Gateway created: ${API_ID}${NC}"
    echo ""

    # Step 6: Configure API Gateway
    echo -e "${BLUE}[6/8]${NC} Configuring API Gateway resources..."

    # Get root resource ID
    ROOT_RESOURCE_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --query 'items[?path==`/`].id' --output text)

    # Create /detokenize resource
    RESOURCE_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --query "items[?pathPart=='detokenize'].id" --output text)

    if [ -z "$RESOURCE_ID" ]; then
        RESOURCE_ID=$(aws apigateway create-resource \
            --rest-api-id "$API_ID" \
            --parent-id "$ROOT_RESOURCE_ID" \
            --path-part "detokenize" \
            --query 'id' \
            --output text)
    fi

    # Create POST method
    aws apigateway put-method \
        --rest-api-id "$API_ID" \
        --resource-id "$RESOURCE_ID" \
        --http-method POST \
        --authorization-type NONE \
        --no-api-key-required 2>/dev/null || true

    # Set up Lambda integration
    aws apigateway put-integration \
        --rest-api-id "$API_ID" \
        --resource-id "$RESOURCE_ID" \
        --http-method POST \
        --type AWS_PROXY \
        --integration-http-method POST \
        --uri "arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" 2>/dev/null || true

    # Create data-type specific paths: /detokenize/name, /detokenize/id, /detokenize/dob, /detokenize/ssn
    for DATA_TYPE in name id dob ssn; do
        # Check if resource exists
        SUB_RESOURCE_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --query "items[?pathPart=='${DATA_TYPE}'].id" --output text)

        if [ -z "$SUB_RESOURCE_ID" ]; then
            SUB_RESOURCE_ID=$(aws apigateway create-resource \
                --rest-api-id "$API_ID" \
                --parent-id "$RESOURCE_ID" \
                --path-part "${DATA_TYPE}" \
                --query 'id' \
                --output text)
        fi

        # Create POST method
        aws apigateway put-method \
            --rest-api-id "$API_ID" \
            --resource-id "$SUB_RESOURCE_ID" \
            --http-method POST \
            --authorization-type NONE \
            --no-api-key-required 2>/dev/null || true

        # Set up Lambda integration
        aws apigateway put-integration \
            --rest-api-id "$API_ID" \
            --resource-id "$SUB_RESOURCE_ID" \
            --http-method POST \
            --type AWS_PROXY \
            --integration-http-method POST \
            --uri "arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" 2>/dev/null || true
    done

    # Create /tokenize resource
    TOKENIZE_RESOURCE_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --query "items[?pathPart=='tokenize'].id" --output text)

    if [ -z "$TOKENIZE_RESOURCE_ID" ]; then
        TOKENIZE_RESOURCE_ID=$(aws apigateway create-resource \
            --rest-api-id "$API_ID" \
            --parent-id "$ROOT_RESOURCE_ID" \
            --path-part "tokenize" \
            --query 'id' \
            --output text)
    fi

    # Create POST method for /tokenize
    aws apigateway put-method \
        --rest-api-id "$API_ID" \
        --resource-id "$TOKENIZE_RESOURCE_ID" \
        --http-method POST \
        --authorization-type NONE \
        --no-api-key-required 2>/dev/null || true

    # Set up Lambda integration for /tokenize
    aws apigateway put-integration \
        --rest-api-id "$API_ID" \
        --resource-id "$TOKENIZE_RESOURCE_ID" \
        --http-method POST \
        --type AWS_PROXY \
        --integration-http-method POST \
        --uri "arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" 2>/dev/null || true

    # Create data-type specific paths: /tokenize/name, /tokenize/id, /tokenize/dob, /tokenize/ssn
    for DATA_TYPE in name id dob ssn; do
        # Check if resource exists
        TOKENIZE_SUB_RESOURCE_ID=$(aws apigateway get-resources --rest-api-id "$API_ID" --query "items[?pathPart=='${DATA_TYPE}' && parentId=='${TOKENIZE_RESOURCE_ID}'].id" --output text)

        if [ -z "$TOKENIZE_SUB_RESOURCE_ID" ]; then
            TOKENIZE_SUB_RESOURCE_ID=$(aws apigateway create-resource \
                --rest-api-id "$API_ID" \
                --parent-id "$TOKENIZE_RESOURCE_ID" \
                --path-part "${DATA_TYPE}" \
                --query 'id' \
                --output text)
        fi

        # Create POST method
        aws apigateway put-method \
            --rest-api-id "$API_ID" \
            --resource-id "$TOKENIZE_SUB_RESOURCE_ID" \
            --http-method POST \
            --authorization-type NONE \
            --no-api-key-required 2>/dev/null || true

        # Set up Lambda integration
        aws apigateway put-integration \
            --rest-api-id "$API_ID" \
            --resource-id "$TOKENIZE_SUB_RESOURCE_ID" \
            --http-method POST \
            --type AWS_PROXY \
            --integration-http-method POST \
            --uri "arn:aws:apigateway:${AWS_REGION}:lambda:path/2015-03-31/functions/${LAMBDA_ARN}/invocations" 2>/dev/null || true
    done

    echo -e "${GREEN}✓ API Gateway configured (with tokenize and detokenize data-type paths)${NC}"
    echo ""

    # Step 7: Grant API Gateway permission to invoke Lambda
    echo -e "${BLUE}[7/8]${NC} Granting API Gateway permissions..."

    aws lambda add-permission \
        --function-name "$LAMBDA_FUNCTION_NAME" \
        --statement-id apigateway-invoke \
        --action lambda:InvokeFunction \
        --principal apigateway.amazonaws.com \
        --source-arn "arn:aws:execute-api:${AWS_REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*" 2>/dev/null || true

    echo -e "${GREEN}✓ Permissions granted${NC}"
    echo ""

    # Step 8: Deploy API
    echo -e "${BLUE}[8/8]${NC} Deploying API to prod stage..."

    aws apigateway create-deployment \
        --rest-api-id "$API_ID" \
        --stage-name prod \
        --description "Production deployment" > /dev/null

    API_URL="https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/prod/detokenize"

    echo -e "${GREEN}✓ API deployed${NC}"
    echo ""

    # Create Snowflake IAM role for API integration
    echo -e "${BLUE}Creating Snowflake IAM role...${NC}"

    SNOWFLAKE_ROLE_ARN=$(aws iam get-role --role-name "$IAM_ROLE_NAME" --query 'Role.Arn' --output text 2>/dev/null || echo "")

    if [ -z "$SNOWFLAKE_ROLE_ARN" ]; then
        # Create trust policy (placeholder, will be updated after Snowflake integration)
        cat > /tmp/snowflake-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "PLACEHOLDER_UPDATE_AFTER_SNOWFLAKE_INTEGRATION"
        }
      }
    }
  ]
}
EOF

        aws iam create-role \
            --role-name "$IAM_ROLE_NAME" \
            --assume-role-policy-document file:///tmp/snowflake-trust-policy.json \
            --description "IAM role for Snowflake API integration"

        # Create and attach policy for API Gateway invoke
        cat > /tmp/api-invoke-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "execute-api:Invoke",
      "Resource": "arn:aws:execute-api:${AWS_REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*"
    }
  ]
}
EOF

        aws iam put-role-policy \
            --role-name "$IAM_ROLE_NAME" \
            --policy-name "APIGatewayInvokePolicy" \
            --policy-document file:///tmp/api-invoke-policy.json

        SNOWFLAKE_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${IAM_ROLE_NAME}"
    fi

    echo -e "${GREEN}✓ Snowflake IAM role created: ${SNOWFLAKE_ROLE_ARN}${NC}"
    echo ""

    # Update setup.sql with deployment values
    echo -e "${BLUE}Updating Snowflake SQL files with deployment values...${NC}"

    # Update setup.sql - use more robust sed patterns
    # Match either empty string or existing ARN
    sed -i.bak "s|API_AWS_ROLE_ARN = '[^']*'|API_AWS_ROLE_ARN = '${SNOWFLAKE_ROLE_ARN}'|g" snowflake/setup.sql
    sed -i.bak "s|API_ALLOWED_PREFIXES = ('[^']*')|API_ALLOWED_PREFIXES = ('https://${API_ID}.execute-api.${AWS_REGION}.amazonaws.com/')|g" snowflake/setup.sql

    # Update create_function.sql - match any existing URL
    sed -i.bak "s|AS 'https://[^']*'|AS '${API_URL}'|g" snowflake/create_function.sql

    # Clean up backup files
    rm -f snowflake/setup.sql.bak snowflake/create_function.sql.bak

    echo -e "${GREEN}✓ SQL files updated${NC}"

    # Verify the update worked
    if grep -q "${SNOWFLAKE_ROLE_ARN}" snowflake/setup.sql && grep -q "${API_ID}" snowflake/setup.sql; then
        echo -e "${GREEN}✓ Verified: SQL files contain correct values${NC}"
    else
        echo -e "${RED}✗ Warning: SQL files may not have been updated correctly${NC}"
        echo -e "${YELLOW}Check snowflake/setup.sql manually${NC}"
    fi
    echo ""

    # Summary
    echo -e "${GREEN}============================================================================${NC}"
    echo -e "${GREEN}Deployment Complete!${NC}"
    echo -e "${GREEN}============================================================================${NC}"
    echo ""
    echo -e "${YELLOW}Lambda Function:${NC}"
    echo -e "  Name: ${GREEN}${LAMBDA_FUNCTION_NAME}${NC}"
    echo -e "  ARN:  ${GREEN}${LAMBDA_ARN}${NC}"
    echo ""
    echo -e "${YELLOW}API Gateway:${NC}"
    echo -e "  URL:  ${GREEN}${API_URL}${NC}"
    echo -e "  ID:   ${GREEN}${API_ID}${NC}"
    echo ""
    echo -e "${YELLOW}Snowflake IAM Role:${NC}"
    echo -e "  ARN:  ${GREEN}${SNOWFLAKE_ROLE_ARN}${NC}"
    echo ""
    echo -e "${YELLOW}Next Steps (Snowflake Setup):${NC}"
    echo ""
    echo -e "${GREEN}🚀 Automated Setup (Recommended):${NC}"
    echo -e "  ${BLUE}$0 --setup-snowflake${NC}"
    echo -e "  (Runs all Snowflake steps automatically - no manual copying!)"
    echo ""
    echo -e "${YELLOW}Then test:${NC}"
    echo -e "  ${BLUE}$0 --test${NC}"
    echo ""

    # Save deployment info
    cat > deployment-info.txt <<EOF
Skyflow Snowflake Tokenization - Deployment Info
==================================================

Deployed: $(date)
AWS Region: ${AWS_REGION}
AWS Account: ${AWS_ACCOUNT_ID}

Lambda Function:
  Name: ${LAMBDA_FUNCTION_NAME}
  ARN: ${LAMBDA_ARN}

API Gateway:
  URL: ${API_URL}
  ID: ${API_ID}

Snowflake IAM Role:
  ARN: ${SNOWFLAKE_ROLE_ARN}

Configuration:
  Vault URL: ${SKYFLOW_VAULT_URL}
  Default Vault ID: ${DEFAULT_VAULT_ID}
EOF

    echo -e "${GREEN}Deployment info saved to: deployment-info.txt${NC}"
}

# ============================================================================
# Destroy Function
# ============================================================================
destroy() {
    echo -e "${RED}Starting destruction...${NC}"
    echo ""
    echo -e "${YELLOW}This will delete:${NC}"
    echo -e "  - Lambda function: ${LAMBDA_FUNCTION_NAME}"
    echo -e "  - API Gateway: ${API_NAME}"
    echo -e "  - IAM roles: ${LAMBDA_ROLE_NAME}, ${IAM_ROLE_NAME}"
    echo -e "  - AWS Secrets Manager secret: ${SECRET_NAME}"
    echo -e "  - Snowflake: API integration and external functions (if configured)"
    echo ""
    read -p "Are you sure? (yes/no): " -r
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        echo "Aborted."
        exit 0
    fi
    echo ""

    # Check if we should clean up Snowflake resources
    CLEANUP_SNOWFLAKE=false
    SF_USER=$(jq -r '.snowflake.user' "$CONFIG_FILE" 2>/dev/null)
    if [ "$SF_USER" != "null" ] && [ -n "$SF_USER" ]; then
        CLEANUP_SNOWFLAKE=true
    fi

    # Delete AWS Secrets Manager secret
    echo -e "${BLUE}[1/5]${NC} Deleting AWS Secrets Manager secret..."
    aws secretsmanager delete-secret \
        --secret-id "$SECRET_NAME" \
        --force-delete-without-recovery \
        --region "$AWS_REGION" 2>/dev/null || echo "  (not found)"
    echo -e "${GREEN}✓ Secret deleted (or scheduled for deletion)${NC}"
    echo ""

    # Delete Lambda function
    echo -e "${BLUE}[2/5]${NC} Deleting Lambda function..."
    aws lambda delete-function --function-name "$LAMBDA_FUNCTION_NAME" 2>/dev/null || echo "  (not found)"
    echo -e "${GREEN}✓ Lambda function deleted${NC}"
    echo ""

    # Delete API Gateway
    echo -e "${BLUE}[3/5]${NC} Deleting API Gateway..."
    API_ID=$(aws apigateway get-rest-apis --query "items[?name=='${API_NAME}'].id" --output text)
    if [ -n "$API_ID" ]; then
        aws apigateway delete-rest-api --rest-api-id "$API_ID"
    fi
    echo -e "${GREEN}✓ API Gateway deleted${NC}"
    echo ""

    # Delete IAM roles
    echo -e "${BLUE}[4/5]${NC} Deleting IAM roles..."

    # Delete Lambda role
    aws iam detach-role-policy --role-name "$LAMBDA_ROLE_NAME" --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" 2>/dev/null || true
    aws iam delete-role-policy --role-name "$LAMBDA_ROLE_NAME" --policy-name "SecretsManagerReadPolicy" 2>/dev/null || true
    aws iam delete-role --role-name "$LAMBDA_ROLE_NAME" 2>/dev/null || echo "  (Lambda role not found)"

    # Delete Snowflake role
    aws iam delete-role-policy --role-name "$IAM_ROLE_NAME" --policy-name "APIGatewayInvokePolicy" 2>/dev/null || true
    aws iam delete-role --role-name "$IAM_ROLE_NAME" 2>/dev/null || echo "  (Snowflake role not found)"

    echo -e "${GREEN}✓ IAM roles deleted${NC}"
    echo ""

    # Clean up Snowflake resources
    if [ "$CLEANUP_SNOWFLAKE" = true ]; then
        echo -e "${BLUE}[5/6]${NC} Cleaning up Snowflake resources..."

        SF_PASSWORD=$(jq -r '.snowflake.password' "$CONFIG_FILE")
        SF_ACCOUNT=$(jq -r '.snowflake.account' "$CONFIG_FILE")
        SF_DATABASE=$(jq -r '.snowflake.database' "$CONFIG_FILE")
        SF_SCHEMA=$(jq -r '.snowflake.schema' "$CONFIG_FILE")
        SF_WAREHOUSE=$(jq -r '.snowflake.warehouse' "$CONFIG_FILE")
        SF_ROLE=$(jq -r '.snowflake.role' "$CONFIG_FILE")

        # Create cleanup SQL
        cat > /tmp/snowflake-cleanup.sql <<EOF
USE ROLE ${SF_ROLE};
USE DATABASE ${SF_DATABASE};
USE SCHEMA ${SF_SCHEMA};

-- Show existing functions before cleanup
SELECT 'Before cleanup:' as status;
SHOW FUNCTIONS LIKE 'TOK_%';
SHOW FUNCTIONS LIKE 'DETOK_%';

-- Drop detokenization functions
DROP FUNCTION IF EXISTS DETOK_NAME(VARCHAR);
DROP FUNCTION IF EXISTS DETOK_ID(VARCHAR);
DROP FUNCTION IF EXISTS DETOK_DOB(VARCHAR);
DROP FUNCTION IF EXISTS DETOK_SSN(VARCHAR);

-- Drop tokenization functions
DROP FUNCTION IF EXISTS TOK_NAME(VARCHAR);
DROP FUNCTION IF EXISTS TOK_ID(VARCHAR);
DROP FUNCTION IF EXISTS TOK_DOB(VARCHAR);
DROP FUNCTION IF EXISTS TOK_SSN(VARCHAR);

-- Drop API integration
DROP API INTEGRATION IF EXISTS skyflow_api_integration;

-- Show what's left after cleanup
SELECT 'After cleanup:' as status;
SHOW FUNCTIONS LIKE 'TOK_%';
SHOW FUNCTIONS LIKE 'DETOK_%';
SHOW API INTEGRATIONS LIKE 'skyflow_detokenize%';
EOF

        echo -e "${BLUE}Running Snowflake cleanup SQL...${NC}"
        echo -e "Database: ${SF_DATABASE}.${SF_SCHEMA}"
        echo -e "Role: ${SF_ROLE}"
        echo ""

        set +e  # Temporarily disable exit on error
        SNOWFLAKE_CLEANUP_OUTPUT=$(SNOWSQL_ACCOUNT="$SF_ACCOUNT" \
            SNOWSQL_USER="$SF_USER" \
            SNOWSQL_PWD="$SF_PASSWORD" \
            SNOWSQL_DATABASE="$SF_DATABASE" \
            SNOWSQL_SCHEMA="$SF_SCHEMA" \
            SNOWSQL_WAREHOUSE="$SF_WAREHOUSE" \
            SNOWSQL_ROLE="$SF_ROLE" \
            snowsql -f /tmp/snowflake-cleanup.sql -o friendly=true -o header=true -o timing=false 2>&1)
        CLEANUP_EXIT_CODE=$?
        set -e  # Re-enable exit on error

        # Show output for debugging
        if [ -n "$SNOWFLAKE_CLEANUP_OUTPUT" ]; then
            echo -e "${BLUE}Snowflake cleanup output:${NC}"
            echo "$SNOWFLAKE_CLEANUP_OUTPUT"
            echo ""
        fi

        # Keep the SQL file for debugging if cleanup failed
        if [ $CLEANUP_EXIT_CODE -eq 0 ]; then
            rm -f /tmp/snowflake-cleanup.sql
            echo -e "${GREEN}✓ Snowflake resources cleaned${NC}"
        else
            echo -e "${YELLOW}⚠ Snowflake cleanup failed (exit code: $CLEANUP_EXIT_CODE)${NC}"
            echo -e "${YELLOW}SQL file saved to: /tmp/snowflake-cleanup.sql${NC}"
            echo ""
            echo -e "${YELLOW}You can manually clean up in Snowflake:${NC}"
            echo -e "  DROP FUNCTION IF EXISTS DETOK_NAME(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS DETOK_ID(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS DETOK_DOB(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS DETOK_SSN(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS TOK_NAME(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS TOK_ID(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS TOK_DOB(VARCHAR);"
            echo -e "  DROP FUNCTION IF EXISTS TOK_SSN(VARCHAR);"
            echo -e "  DROP API INTEGRATION IF EXISTS skyflow_api_integration;"
        fi
        echo ""
    else
        echo -e "${YELLOW}⚠ Skipping Snowflake cleanup (no credentials in config.json)${NC}"
        echo ""
    fi

    # Clean up local files
    echo -e "${BLUE}[6/6]${NC} Cleaning up local files..."
    rm -rf lambda/node_modules lambda/function.zip
    rm -f /tmp/lambda-trust-policy.json /tmp/snowflake-trust-policy.json /tmp/api-invoke-policy.json
    rm -f /tmp/snowflake-cleanup.sql
    rm -f deployment-info.txt
    echo -e "${GREEN}✓ Local files cleaned${NC}"
    echo ""

    echo -e "${GREEN}============================================================================${NC}"
    echo -e "${GREEN}Destruction Complete!${NC}"
    echo -e "${GREEN}============================================================================${NC}"
}

# ============================================================================
# Setup AWS Permissions Function
# ============================================================================
setup_permissions() {
    local IAM_USERNAME="$1"
    local POLICY_NAME="SkyflowSnowflakeTokenizationPolicy"

    if [ -z "$IAM_USERNAME" ]; then
        echo -e "${RED}Error: No IAM username provided${NC}"
        echo ""
        echo "Usage: $0 --setup-permissions <iam-username>"
        echo ""
        echo "Example:"
        echo "  $0 --setup-permissions nimbus-user-s3-access"
        exit 1
    fi

    echo -e "${BLUE}============================================================================${NC}"
    echo -e "${BLUE}Setup AWS Permissions${NC}"
    echo -e "${BLUE}============================================================================${NC}"
    echo ""
    echo -e "Target IAM User: ${GREEN}${IAM_USERNAME}${NC}"
    echo ""

    # Verify user exists
    if ! aws iam get-user --user-name "$IAM_USERNAME" > /dev/null 2>&1; then
        echo -e "${RED}✗ IAM user '${IAM_USERNAME}' not found${NC}"
        exit 1
    fi

    # Create policy
    cat > /tmp/skyflow-tokenization-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "IAMPermissions",
      "Effect": "Allow",
      "Action": [
        "iam:CreateRole", "iam:GetRole", "iam:DeleteRole",
        "iam:AttachRolePolicy", "iam:DetachRolePolicy",
        "iam:PutRolePolicy", "iam:DeleteRolePolicy",
        "iam:ListRoles", "iam:PassRole", "iam:UpdateAssumeRolePolicy"
      ],
      "Resource": [
        "arn:aws:iam::*:role/skyflow-tokenization-lambda-role",
        "arn:aws:iam::*:role/SnowflakeAPIRole"
      ]
    },
    {
      "Sid": "LambdaPermissions",
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction", "lambda:GetFunction", "lambda:DeleteFunction",
        "lambda:UpdateFunctionCode", "lambda:UpdateFunctionConfiguration",
        "lambda:AddPermission", "lambda:RemovePermission", "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:*:*:function:skyflow-tokenization"
    },
    {
      "Sid": "LambdaListPermissions",
      "Effect": "Allow",
      "Action": ["lambda:ListFunctions"],
      "Resource": "*"
    },
    {
      "Sid": "APIGatewayPermissions",
      "Effect": "Allow",
      "Action": ["apigateway:GET", "apigateway:POST", "apigateway:PUT", "apigateway:DELETE", "apigateway:PATCH"],
      "Resource": ["arn:aws:apigateway:*::/restapis", "arn:aws:apigateway:*::/restapis/*"]
    },
    {
      "Sid": "CloudWatchLogsPermissions",
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups"],
      "Resource": ["arn:aws:logs:*:*:log-group:/aws/lambda/skyflow-tokenization:*"]
    },
    {
      "Sid": "SecretsManagerPermissions",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:CreateSecret",
        "secretsmanager:GetSecretValue",
        "secretsmanager:PutSecretValue",
        "secretsmanager:UpdateSecret",
        "secretsmanager:DeleteSecret",
        "secretsmanager:DescribeSecret"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:skyflow-tokenization-*"
    },
    {
      "Sid": "STSPermissions",
      "Effect": "Allow",
      "Action": ["sts:GetCallerIdentity"],
      "Resource": "*"
    }
  ]
}
EOF

    # Delete old policy if exists
    POLICY_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${POLICY_NAME}"
    if aws iam get-policy --policy-arn "$POLICY_ARN" > /dev/null 2>&1; then
        aws iam detach-user-policy --user-name "$IAM_USERNAME" --policy-arn "$POLICY_ARN" 2>/dev/null || true
        VERSIONS=$(aws iam list-policy-versions --policy-arn "$POLICY_ARN" --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text)
        for VERSION in $VERSIONS; do
            aws iam delete-policy-version --policy-arn "$POLICY_ARN" --version-id "$VERSION" 2>/dev/null || true
        done
        aws iam delete-policy --policy-arn "$POLICY_ARN" 2>/dev/null || true
        sleep 2
    fi

    # Create policy
    POLICY_ARN=$(aws iam create-policy \
        --policy-name "$POLICY_NAME" \
        --policy-document file:///tmp/skyflow-tokenization-policy.json \
        --description "Permissions for Skyflow Snowflake tokenization and detokenization deployment" \
        --query 'Policy.Arn' \
        --output text)

    # Attach to user
    aws iam attach-user-policy --user-name "$IAM_USERNAME" --policy-arn "$POLICY_ARN"

    rm -f /tmp/skyflow-tokenization-policy.json

    echo -e "${GREEN}✓ Permissions granted to ${IAM_USERNAME}${NC}"
    echo ""
    echo -e "Now run: ${BLUE}$0 --deploy${NC}"
    echo ""
}

# ============================================================================
# Snowflake Setup Functions
# ============================================================================
setup_snowflake() {
    local OVERRIDE_DATABASE=""
    local OVERRIDE_SCHEMA=""

    # Parse optional arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --database)
                OVERRIDE_DATABASE="$2"
                shift 2
                ;;
            --schema)
                OVERRIDE_SCHEMA="$2"
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done

    if [ ! -f "deployment-info.txt" ]; then
        echo -e "${RED}Error: deployment-info.txt not found${NC}"
        echo -e "${YELLOW}Please run: $0 --deploy${NC}"
        exit 1
    fi

    # Extract Snowflake config
    SF_USER=$(jq -r '.snowflake.user' "$CONFIG_FILE")
    SF_PASSWORD=$(jq -r '.snowflake.password' "$CONFIG_FILE")
    SF_ACCOUNT=$(jq -r '.snowflake.account' "$CONFIG_FILE")
    SF_DATABASE=$(jq -r '.snowflake.database' "$CONFIG_FILE")
    SF_SCHEMA=$(jq -r '.snowflake.schema' "$CONFIG_FILE")
    SF_WAREHOUSE=$(jq -r '.snowflake.warehouse' "$CONFIG_FILE")
    SF_ROLE=$(jq -r '.snowflake.role' "$CONFIG_FILE")

    # Apply overrides if provided
    if [ -n "$OVERRIDE_DATABASE" ]; then
        SF_DATABASE="$OVERRIDE_DATABASE"
        echo -e "${YELLOW}Using database override: ${GREEN}${SF_DATABASE}${NC}"
    fi
    if [ -n "$OVERRIDE_SCHEMA" ]; then
        SF_SCHEMA="$OVERRIDE_SCHEMA"
        echo -e "${YELLOW}Using schema override: ${GREEN}${SF_SCHEMA}${NC}"
    fi

    # Extract deployment info
    # Find the line after "Snowflake IAM Role:" that contains "ARN:"
    SNOWFLAKE_ROLE_ARN=$(grep -A 1 "Snowflake IAM Role:" deployment-info.txt | grep "ARN:" | awk '{print $2}')
    # Find the line after "API Gateway:" that contains "URL:"
    API_URL=$(grep -A 2 "API Gateway:" deployment-info.txt | grep "URL:" | awk '{print $2}')
    API_PREFIX=$(echo "$API_URL" | sed 's|/detokenize$|/|')

    echo -e "${BLUE}============================================================================${NC}"
    echo -e "${BLUE}Snowflake Setup${NC}"
    echo -e "${BLUE}============================================================================${NC}"
    echo -e "Account: ${GREEN}${SF_ACCOUNT}${NC}"
    echo -e "Database: ${GREEN}${SF_DATABASE}.${SF_SCHEMA}${NC}"
    echo ""

    echo -e "${YELLOW}[1/4]${NC} Creating API integration..."

    # Validate we have the required values
    if [ -z "$SNOWFLAKE_ROLE_ARN" ] || [ "$SNOWFLAKE_ROLE_ARN" == "null" ]; then
        echo -e "${RED}✗ Missing Snowflake Role ARN${NC}"
        echo -e "${YELLOW}Please run: $0 --deploy first${NC}"
        exit 1
    fi

    echo -e "${BLUE}Using ARN: ${GREEN}${SNOWFLAKE_ROLE_ARN}${NC}"
    echo -e "${BLUE}Using API Prefix: ${GREEN}${API_PREFIX}${NC}"
    echo ""

    # Generate and run setup SQL
    cat > .snowflake-setup.sql <<EOF
USE ROLE ${SF_ROLE};
CREATE OR REPLACE API INTEGRATION skyflow_api_integration
    API_PROVIDER = aws_api_gateway
    API_AWS_ROLE_ARN = '${SNOWFLAKE_ROLE_ARN}'
    ENABLED = TRUE
    API_ALLOWED_PREFIXES = ('${API_PREFIX}');
DESC API INTEGRATION skyflow_api_integration;
EOF

    # Verify the SQL file has correct values
    echo -e "${BLUE}Verifying SQL file...${NC}"
    if grep -q "API_AWS_ROLE_ARN = ''" .snowflake-setup.sql; then
        echo -e "${RED}✗ SQL file has empty ARN!${NC}"
        cat .snowflake-setup.sql
        exit 1
    fi
    if grep -q "${SNOWFLAKE_ROLE_ARN}" .snowflake-setup.sql; then
        echo -e "${GREEN}✓ SQL file verified${NC}"
    else
        echo -e "${RED}✗ SQL file missing ARN${NC}"
        cat .snowflake-setup.sql
        exit 1
    fi
    echo ""

    set +e  # Temporarily disable exit on error
    SNOWSQL_OUTPUT=$(SNOWSQL_ACCOUNT="$SF_ACCOUNT" \
        SNOWSQL_USER="$SF_USER" \
        SNOWSQL_PWD="$SF_PASSWORD" \
        SNOWSQL_DATABASE="$SF_DATABASE" \
        SNOWSQL_SCHEMA="$SF_SCHEMA" \
        SNOWSQL_WAREHOUSE="$SF_WAREHOUSE" \
        SNOWSQL_ROLE="$SF_ROLE" \
        snowsql -f .snowflake-setup.sql -o friendly=false -o header=true -o timing=false -o output_format=csv 2>&1)
    SNOWSQL_EXIT_CODE=$?
    set -e  # Re-enable exit on error

    if [ $SNOWSQL_EXIT_CODE -ne 0 ]; then
        echo -e "${RED}✗ Failed to create API integration${NC}"
        echo ""
        echo -e "${YELLOW}Snowsql output:${NC}"
        echo "$SNOWSQL_OUTPUT"
        echo ""
        echo -e "${YELLOW}Troubleshooting:${NC}"
        echo "  1. Check Snowflake credentials in config.json"
        echo "  2. Verify database/schema exist: ${SF_DATABASE}.${SF_SCHEMA}"
        echo "  3. Verify role has ACCOUNTADMIN or sufficient privileges"
        echo "  4. Check snowsql is installed: which snowsql"
        echo -e "${YELLOW}SQL file saved to: .snowflake-setup.sql${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ API integration created${NC}"
    echo ""

    echo -e "${YELLOW}[2/4]${NC} Extracting trust policy values..."

    # Parse Snowflake CSV output - format: property,property_type,property_value,property_default
    API_AWS_IAM_USER_ARN=$(echo "$SNOWSQL_OUTPUT" | grep -i "API_AWS_IAM_USER_ARN" | awk -F',' '{print $3}' | tr -d '"' | sed 's/^[ \t]*//;s/[ \t]*$//')
    API_AWS_EXTERNAL_ID=$(echo "$SNOWSQL_OUTPUT" | grep -i "API_AWS_EXTERNAL_ID" | awk -F',' '{print $3}' | tr -d '"' | sed 's/^[ \t]*//;s/[ \t]*$//')

    # Validate the extracted values
    if [ -z "$API_AWS_IAM_USER_ARN" ] || [ -z "$API_AWS_EXTERNAL_ID" ]; then
        echo -e "${RED}✗ Failed to extract values${NC}"
        echo ""
        echo -e "${YELLOW}Run manually:${NC}"
        echo -e "  DESC API INTEGRATION skyflow_api_integration;"
        exit 1
    fi

    # Validate ARN format
    if [[ ! "$API_AWS_IAM_USER_ARN" =~ ^arn:aws:iam::[0-9]+: ]]; then
        echo -e "${RED}✗ Invalid ARN format: ${API_AWS_IAM_USER_ARN}${NC}"
        echo ""
        echo -e "${YELLOW}Full Snowflake output for debugging:${NC}"
        echo "$SNOWSQL_OUTPUT" | grep -A 2 -B 2 "API_AWS_IAM_USER_ARN"
        echo ""
        exit 1
    fi

    echo -e "${GREEN}✓ Values extracted and validated${NC}"
    echo ""

    echo -e "${YELLOW}[3/4]${NC} Updating AWS IAM trust policy..."

    # Update IAM trust policy
    cat > .snowflake-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "${API_AWS_IAM_USER_ARN}"},
    "Action": "sts:AssumeRole",
    "Condition": {"StringEquals": {"sts:ExternalId": "${API_AWS_EXTERNAL_ID}"}}
  }]
}
EOF

    aws iam update-assume-role-policy \
        --role-name "$IAM_ROLE_NAME" \
        --policy-document file://.snowflake-trust-policy.json

    echo -e "${GREEN}✓ Trust policy updated${NC}"
    echo ""

    echo -e "${YELLOW}[4/4]${NC} Creating external functions..."

    # Get API Gateway details
    API_BASE_URL=$(echo "${API_URL}" | sed 's|/detokenize$||')

    # Create functions - 4 detokenize + 4 tokenize (8 total)
    cat > .snowflake-create-function.sql <<EOF
USE ROLE ${SF_ROLE};
USE DATABASE ${SF_DATABASE};
USE SCHEMA ${SF_SCHEMA};

-- Detokenization functions
CREATE OR REPLACE EXTERNAL FUNCTION DETOK_NAME(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/detokenize/name';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_ID(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/detokenize/id';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_DOB(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/detokenize/dob';

CREATE OR REPLACE EXTERNAL FUNCTION DETOK_SSN(token VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/detokenize/ssn';

-- Tokenization functions
CREATE OR REPLACE EXTERNAL FUNCTION TOK_NAME(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/tokenize/name';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_ID(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/tokenize/id';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_DOB(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/tokenize/dob';

CREATE OR REPLACE EXTERNAL FUNCTION TOK_SSN(plaintext VARCHAR)
    RETURNS VARCHAR
    API_INTEGRATION = skyflow_api_integration
    AS '${API_BASE_URL}/tokenize/ssn';
EOF

    set +e  # Temporarily disable exit on error
    FUNCTION_OUTPUT=$(SNOWSQL_ACCOUNT="$SF_ACCOUNT" \
        SNOWSQL_USER="$SF_USER" \
        SNOWSQL_PWD="$SF_PASSWORD" \
        SNOWSQL_DATABASE="$SF_DATABASE" \
        SNOWSQL_SCHEMA="$SF_SCHEMA" \
        SNOWSQL_WAREHOUSE="$SF_WAREHOUSE" \
        SNOWSQL_ROLE="$SF_ROLE" \
        snowsql -f .snowflake-create-function.sql -o friendly=false -o header=true -o timing=false -o output_format=csv 2>&1)
    FUNCTION_EXIT_CODE=$?
    set -e  # Re-enable exit on error

    # Check for any errors in output
    if echo "$FUNCTION_OUTPUT" | grep -qi "error\|failed"; then
        echo -e "${RED}✗ Failed to create functions${NC}"
        echo ""
        echo -e "${YELLOW}SQL file saved to: .snowflake-create-function.sql${NC}"
        echo -e "${YELLOW}Error output:${NC}"
        echo "$FUNCTION_OUTPUT"
        echo ""
        echo -e "${YELLOW}Troubleshooting:${NC}"
        echo "  1. Check that API integration exists: SHOW API INTEGRATIONS;"
        echo "  2. Verify role has USAGE on integration"
        echo "  3. Check Snowflake query history for detailed errors"
        exit 1
    fi

    if [ $FUNCTION_EXIT_CODE -ne 0 ]; then
        echo -e "${RED}✗ Failed to create functions (exit code: $FUNCTION_EXIT_CODE)${NC}"
        echo ""
        echo -e "${YELLOW}SQL file saved to: .snowflake-create-function.sql${NC}"
        echo -e "${YELLOW}Output:${NC}"
        echo "$FUNCTION_OUTPUT"
        exit 1
    fi

    # Check if all CREATE statements succeeded (expect 8: 4 DETOK_* + 4 TOK_*)
    SUCCESS_COUNT=$(echo "$FUNCTION_OUTPUT" | grep -c "Statement executed successfully")
    if [ "$SUCCESS_COUNT" -ge 8 ]; then
        echo -e "${GREEN}✓ All 8 functions created successfully${NC}"
        echo -e "${GREEN}  - DETOK_NAME, DETOK_ID, DETOK_DOB, DETOK_SSN${NC}"
        echo -e "${GREEN}  - TOK_NAME, TOK_ID, TOK_DOB, TOK_SSN${NC}"
        rm -f .snowflake-create-function.sql
    else
        echo -e "${YELLOW}⚠ Unexpected output (only $SUCCESS_COUNT success messages, expected 8)${NC}"
        echo -e "${YELLOW}SQL file saved to: .snowflake-create-function.sql${NC}"
        echo ""
        echo -e "${BLUE}Output:${NC}"
        echo "$FUNCTION_OUTPUT"
    fi
    echo ""

    # Cleanup
    rm -f .snowflake-setup.sql .snowflake-trust-policy.json

    echo -e "${GREEN}============================================================================${NC}"
    echo -e "${GREEN}Setup Complete! 🎉${NC}"
    echo -e "${GREEN}============================================================================${NC}"
    echo ""
    echo -e "Test with: ${BLUE}$0 --test${NC}"
    echo ""
}

# ============================================================================
# Test Function
# ============================================================================
test_snowflake() {
    SF_USER=$(jq -r '.snowflake.user' "$CONFIG_FILE")
    SF_PASSWORD=$(jq -r '.snowflake.password' "$CONFIG_FILE")
    SF_ACCOUNT=$(jq -r '.snowflake.account' "$CONFIG_FILE")
    SF_DATABASE=$(jq -r '.snowflake.database' "$CONFIG_FILE")
    SF_SCHEMA=$(jq -r '.snowflake.schema' "$CONFIG_FILE")
    SF_WAREHOUSE=$(jq -r '.snowflake.warehouse' "$CONFIG_FILE")
    SF_ROLE=$(jq -r '.snowflake.role' "$CONFIG_FILE")

    echo -e "${BLUE}Testing Snowflake integration...${NC}"
    echo ""

    cat > /tmp/snowflake-test.sql <<EOF
USE ROLE ${SF_ROLE};
USE DATABASE ${SF_DATABASE};
USE SCHEMA ${SF_SCHEMA};
SHOW FUNCTIONS LIKE 'DETOK_%';
SHOW FUNCTIONS LIKE 'TOK_%';
SELECT '✓ Functions are available' as status;
EOF

    SNOWSQL_ACCOUNT="$SF_ACCOUNT" \
        SNOWSQL_USER="$SF_USER" \
        SNOWSQL_PWD="$SF_PASSWORD" \
        SNOWSQL_DATABASE="$SF_DATABASE" \
        SNOWSQL_SCHEMA="$SF_SCHEMA" \
        SNOWSQL_WAREHOUSE="$SF_WAREHOUSE" \
        SNOWSQL_ROLE="$SF_ROLE" \
        snowsql -f /tmp/snowflake-test.sql -o friendly=true -o header=true

    rm -f /tmp/snowflake-test.sql

    echo ""
    echo -e "${YELLOW}Test with your data:${NC}"
    echo -e "  ${BLUE}SELECT TOK_NAME('John Doe');${NC}"
    echo -e "  ${BLUE}SELECT DETOK_NAME(token_column) FROM your_table LIMIT 10;${NC}"
    echo ""
}

# ============================================================================
# Main
# ============================================================================

case "${1}" in
    --deploy)
        deploy
        ;;
    --destroy)
        destroy
        ;;
    --redeploy)
        echo -e "${YELLOW}Redeploying (destroy + deploy)...${NC}"
        echo ""
        # Run destroy without confirmation prompt
        REPLY="yes"
        destroy
        echo ""
        echo -e "${YELLOW}Starting fresh deployment...${NC}"
        echo ""
        deploy
        echo ""
        echo -e "${YELLOW}Running tests...${NC}"
        echo ""
        test_snowflake
        ;;
    --redeploy-e2e)
        echo -e "${YELLOW}End-to-end redeploy (destroy + deploy + setup + test)...${NC}"
        echo ""
        # Run destroy without confirmation prompt
        REPLY="yes"
        destroy
        echo ""
        echo -e "${YELLOW}Starting fresh deployment...${NC}"
        echo ""
        deploy
        echo ""
        echo -e "${YELLOW}Proceeding to Snowflake setup...${NC}"
        echo ""
        shift  # Remove --redeploy-e2e from arguments
        setup_snowflake "$@"  # Pass any remaining arguments (like --database, --schema)
        echo ""
        echo -e "${YELLOW}Running tests...${NC}"
        echo ""
        test_snowflake
        ;;
    --setup-permissions)
        setup_permissions "$2"
        ;;
    --setup-snowflake)
        shift  # Remove --setup-snowflake from arguments
        setup_snowflake "$@"  # Pass remaining arguments
        ;;
    --test)
        test_snowflake
        ;;
    --deploy-e2e)
        echo -e "${YELLOW}Running end-to-end deployment...${NC}"
        echo ""
        deploy
        echo ""
        echo -e "${YELLOW}Proceeding to Snowflake setup...${NC}"
        echo ""
        shift  # Remove --deploy-e2e from arguments
        setup_snowflake "$@"  # Pass any remaining arguments (like --database, --schema)
        ;;
    *)
        echo "Usage: $0 {--deploy|--destroy|--redeploy|--redeploy-e2e|--setup-permissions|--setup-snowflake|--test|--deploy-e2e}"
        echo ""
        echo "  --deploy                             Deploy Lambda and API Gateway to AWS"
        echo "  --destroy                            Destroy all AWS resources"
        echo "  --redeploy                           Destroy, redeploy AWS, and test"
        echo "  --redeploy-e2e [options]             Destroy, redeploy AWS, setup Snowflake, and test"
        echo "      --database <name>                   Override database from config.json"
        echo "      --schema <name>                     Override schema from config.json"
        echo "  --setup-permissions <user>           Grant AWS permissions to IAM user"
        echo "  --setup-snowflake [options]          Complete Snowflake setup (automated)"
        echo "      --database <name>                   Override database from config.json"
        echo "      --schema <name>                     Override schema from config.json"
        echo "  --test                               Test Snowflake integration"
        echo "  --deploy-e2e [options]               Deploy AWS + Snowflake setup (automated)"
        echo "      --database <name>                   Override database from config.json"
        echo "      --schema <name>                     Override schema from config.json"
        echo ""
        echo "Complete workflow:"
        echo "  1. $0 --setup-permissions your-iam-username"
        echo "  2. $0 --deploy-e2e                       # Deploy AWS + Snowflake in one command"
        echo "  3. $0 --test"
        echo ""
        echo "Or step-by-step:"
        echo "  1. $0 --setup-permissions your-iam-username"
        echo "  2. $0 --deploy"
        echo "  3. $0 --setup-snowflake"
        echo "  4. $0 --test"
        echo ""
        echo "Quick redeploy (AWS only):"
        echo "  $0 --redeploy"
        echo ""
        echo "Quick redeploy (AWS + Snowflake):"
        echo "  $0 --redeploy-e2e"
        echo ""
        echo "Examples:"
        echo "  # Use config.json database/schema"
        echo "  $0 --deploy-e2e"
        echo "  $0 --redeploy-e2e"
        echo ""
        echo "  # Override database and schema"
        echo "  $0 --deploy-e2e --database MY_DB --schema MY_SCHEMA"
        echo "  $0 --redeploy-e2e --database MY_DB --schema MY_SCHEMA"
        exit 1
        ;;
esac
