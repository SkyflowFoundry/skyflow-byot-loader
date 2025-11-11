# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains two main implementations for Skyflow BYOT (Bring Your Own Token) integration:

1. **Go Loader (`main.go`)**: High-performance bulk data loader for migrating large datasets to Skyflow vaults
2. **Snowflake Integration (`skyflow-snowflake-tokenization/`)**: Lambda-based tokenization/detokenization for in-query Snowflake operations

## Build and Run Commands

### Go Loader (Main Application)

```bash
# Install dependencies
./install_dependencies.sh

# Build the loader
go build -o skyflow-loader main.go

# Build mock data generator
go build -o generate_mock_data generate_mock_data.go

# Build vault clearing utility
go build -o clear-vaults clear_vaults.go

# Run loader with CSV source (default)
./skyflow-loader -token "YOUR_TOKEN" -source csv

# Run loader with Snowflake source
./skyflow-loader -token "YOUR_TOKEN" -source snowflake

# Run with specific vault only (for parallel processing)
./skyflow-loader -token "YOUR_TOKEN" -vault name

# Reprocess failed records from error log
./skyflow-loader -error-log error_log_NAME_20251016_143052.json -upsert

# Generate mock test data
./generate_mock_data -count=10000 -output=csv
```

### Snowflake Integration (Lambda)

```bash
# Change to project directory
cd skyflow-snowflake-tokenization

# Deploy everything (AWS + Snowflake) in one command
./deploy.sh --deploy-e2e

# Deploy AWS resources only
./deploy.sh --deploy

# Deploy Snowflake functions only
./deploy.sh --setup-snowflake

# Test deployment
./deploy.sh --test

# Destroy all resources
./deploy.sh --destroy

# View deployment help
./deploy.sh --help
```

### Monitoring and Testing

```bash
# Monitor loader in real-time (run in separate terminal)
./monitor.sh

# View Lambda logs
aws logs tail /aws/lambda/skyflow-tokenization --follow --region us-east-1
```

## Configuration

Both implementations use `config.json` (copy from `config.example.json`):

- **Skyflow**: vault URLs, bearer tokens, vault IDs
- **Snowflake**: credentials, database, warehouse, schema
- **Performance**: batch sizes, concurrency limits
- **AWS**: region and credentials (for Snowflake integration)

**Important**: Credentials can be provided via:
1. Interactive prompts (most secure, recommended)
2. CLI flags (`-token`, `-sf-user`, `-sf-password`)
3. `config.json` file (convenient but less secure)

## Architecture

### Go Loader Architecture

**Data Flow**: Data Source → Read → Batch → Worker Pool → Skyflow API

- **Concurrency Model**: Go goroutines with worker pools (configurable via `-concurrency`)
- **Batch Processing**: Groups records into batches (default 300) before API calls
- **Retry Logic**: Exponential backoff with 3 retries for failed batches
- **Error Recovery**: Failed batches logged to `error_log_<vault>_<timestamp>.json` for reprocessing

**Key Components**:
- `main.go`: Core loader with HTTP/2 connection pooling, worker pools, metrics
- `generate_mock_data.go`: Test data generator with realistic PII
- `clear_vaults.go`: Utility to clear vault data between test runs

**Vault Processing Modes**:
- **Sequential** (default): Processes all 4 vaults (NAME, ID, DOB, SSN) one after another
- **Parallel** (recommended): Use `-vault` flag to run separate processes per vault

**Data Sources**:
- **CSV**: Vault-specific files (`name_data.csv`, `name_tokens.csv`, etc.) in `data/` directory
- **Snowflake**: Direct database queries with two modes:
  - `simple`: Single table query (`PATIENTS` table)
  - `union`: Complex multi-table queries with UDF detokenization functions

**Performance Tuning**:
- Increase `-concurrency` for higher throughput (32-128 workers typical)
- Use `-vault` flag to run parallel processes (one per vault)
- Adjust `-batch-size` based on record size (default 300)
- Use `-offline` mode for long-running loads that survive SSH disconnects

**Error Handling**:
- Failed batches automatically logged to JSON with complete record data
- Use `-error-log` flag to reprocess failures
- Always use `-upsert` when reprocessing to avoid "already exists" errors

### Snowflake Integration Architecture

**Request Flow**: Snowflake Query → API Gateway → Lambda → Skyflow API

- **Unified Gateway Endpoint**: Single `/process` endpoint with operation/data-type headers
- **Header-Based Routing**:
  - `X-Operation`: `tokenize` or `detokenize`
  - `X-Data-Type`: `NAME`, `SSN`, `DOB`, or `ID`
  - Note: Snowflake prepends `sf-custom-` to custom headers, `sf-context-` to context headers
- **Multi-Vault Support**: Routes different data types to separate Skyflow vaults
- **SDK Integration**: Uses official Skyflow Node.js SDK v2.0.0 with built-in batch processing
- **Credential Management**: AWS Secrets Manager (production) or environment variables (dev)

**Key Files**:
- `lambda/handler.js`: Routes requests, manages SDK client lifecycle, 5-minute config cache
- `lambda/skyflow-client.js`: Wraps Skyflow SDK, handles vault routing, lazy client initialization
- `lambda/config.js`: Loads config from Secrets Manager or file
- `snowflake/setup.sql`: Creates API integration with IAM trust policy
- `snowflake/create_function.sql`: Defines 8 external functions (TOK_NAME, DETOK_NAME, etc.)
- `deploy.sh`: End-to-end deployment automation

**Deployment Process**:
1. Lambda function deployment with dependencies
2. API Gateway creation with IAM role authentication
3. Snowflake API integration with trust policy
4. External function definitions

## Common Development Workflows

### Testing Go Loader Changes

```bash
# Build and test with small dataset
go build -o skyflow-loader main.go
./generate_mock_data -count=1000 -output=csv
./skyflow-loader -token "TOKEN" -source csv -max-records 1000 -vault name

# Test Snowflake integration
./skyflow-loader -token "TOKEN" -source snowflake -max-records 100 -sf-query-mode simple
```

### Testing Snowflake Integration Changes

```bash
cd skyflow-snowflake-tokenization

# Make code changes to lambda/*.js files

# Redeploy Lambda only (fast)
./deploy.sh --redeploy

# Test from Snowflake
./deploy.sh --test

# Or test directly with AWS CLI
aws lambda invoke \
  --function-name skyflow-tokenization \
  --payload '{"headers":{"sf-custom-x-operation":"tokenize","sf-custom-x-data-type":"NAME"},"body":"{\"data\":[[0,\"John Doe\"]]}"}' \
  --region us-east-1 \
  response.json
```

### Debugging Failed Batches

```bash
# Review error log structure
cat error_log_NAME_20251016_143052.json | jq '.'

# Reprocess with upsert enabled
./skyflow-loader -error-log error_log_NAME_20251016_143052.json -upsert -concurrency 64

# If still failing, try smaller batches
./skyflow-loader -error-log error_log_NAME_20251016_143052.json -upsert -batch-size 100
```

### Monitoring Production Loads

```bash
# Start long-running load in offline mode
./skyflow-loader -source snowflake -max-records 0 -offline

# Monitor from another terminal
tail -f skyflow-loader-*.log
./monitor.sh

# Check progress
ps -p $(cat skyflow-loader.pid)
```

## Important Implementation Details

### Go Loader

- **HTTP Client**: Shared `http.Client` with HTTP/2, connection pooling (10 max idle conns)
- **Metrics**: Real-time tracking of workers, throughput, latency, retry counts
- **Progress Reporting**: Updates every 3 seconds (live) and every 1% or 10k records (progress)
- **Graceful Shutdown**: Handles SIGINT/SIGTERM, waits for in-flight batches
- **Memory Management**: Uses `strings.Builder`, per-goroutine RNG, cached URLs
- **Upsert Mode**: Enable with `-upsert` flag for idempotent operations (same value = same token)

### Snowflake Integration

- **Config Caching**: Lambda config cached for 5 minutes to reduce Secrets Manager calls
- **Client Reuse**: Singleton SDK client reused across warm Lambda invocations
- **Lazy Loading**: SDK clients initialized on-demand per data type (NAME, SSN, etc.)
- **Error Context**: Includes Snowflake caller info (user, role, account) in CloudWatch logs
- **Batch Processing**: SDK handles batching internally (configurable batch sizes)
- **Header Normalization**: All headers converted to lowercase for case-insensitive lookup

## Git Workflow Notes

The repository currently has staged changes for unified gateway endpoint implementation:
- Modified: `skyflow-snowflake-tokenization/lambda/skyflow-client.js` (header-based routing)
- Modified: `skyflow-snowflake-tokenization/snowflake/create_function.sql` (updated function signatures)
- Modified: `skyflow-snowflake-tokenization/snowflake/setup.sql` (API integration updates)
- Added: `skyflow-snowflake-tokenization/lambda/skyflow-config-old.json` (backup)

Main branch: `main`
Current branch: `unified-gateway-endpoint`

## Testing and Quality Assurance

- **Unit Testing**: No formal test framework currently; manual testing via CLI
- **Integration Testing**: Use `./deploy.sh --test` for Snowflake integration
- **Load Testing**: Use `generate_mock_data` to create test datasets of various sizes
- **Error Log Validation**: Always test error log reprocessing after changes to batch handling
- **Round-Trip Testing**: SQL examples in `snowflake/examples.sql` verify tokenize→detokenize integrity

## Performance Baselines

- **Go Loader**: 9,800+ records/sec with 32 workers, batch size 300
- **Snowflake Integration**: 100-200 records/batch (Snowflake-managed), ~3-5 mins per 100k tokens
- **Scaling**: 500M records in ~10 hours with parallel vault processing
