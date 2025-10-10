# Skyflow BYOT Loader

High-performance Go implementation of the Skyflow BYOT (Bring Your Own Token) loader with support for multiple data sources.

## Table of Contents
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration File](#configuration-file)
- [Quick Start](#quick-start)
- [Data Sources](#data-sources)
- [Usage Examples](#usage-examples)
- [Command-Line Reference](#command-line-reference)
- [Performance Optimization](#performance-optimization)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)

---

## Features

### Core Capabilities
- ✅ **True concurrency** with Go goroutines (no GIL limitations)
- ✅ **Multiple data sources**: CSV files and Snowflake database
- ✅ **Flexible Snowflake queries**: Simple table mode and complex UNION mode
- ✅ **Concurrent batch processing** with configurable worker pools
- ✅ **Automatic retry logic** with exponential backoff
- ✅ **Single-vault mode** for process-level parallelism
- ✅ **Real-time progress reporting** with live metrics
- ✅ **Error logging** for failed batches with retry capability
- ✅ **Detailed performance metrics** with timing breakdown

### Performance Benefits
- 🚀 **High-throughput processing** with optimized concurrency (9,800+ rec/s)
- 🔥 **True parallel execution** with Go goroutines
- ⚡ **HTTP/2 support** with connection pooling
- 💾 **Memory-efficient** streaming with buffer pools
- 📊 **Handles millions of records** with ease
- ⚡ **Performance optimizations**: String builders, per-goroutine RNG, cached URLs

---

## Prerequisites

- **Go 1.21 or higher** - [Download Go](https://go.dev/doc/install)
- **Skyflow bearer token** - For authentication (can be provided via CLI, config.json, or interactive prompt)
- **Data source**: Either:
  - CSV files (patient_data.csv and patient_tokens.csv), OR
  - Snowflake connection (credentials can be provided via CLI, config.json, or interactive prompts)

---

## Installation

### 1. Install dependencies
```bash
./install_dependencies.sh
```

This will:
- Verify Go installation
- Download the Snowflake Go driver (`github.com/snowflakedb/gosnowflake`)
- Initialize Go modules

### 2. Configure the application
```bash
cp config.example.json config.json
```

Edit `config.json` with your Skyflow vault IDs, Snowflake credentials, and performance settings. See [Configuration File](#configuration-file) for details.

### 3. Build the loader
```bash
go build -o skyflow-loader main.go
```

You now have a compiled binary `skyflow-loader` ready to use!

---

## Configuration File

The loader uses a `config.json` file to store all configuration parameters. This makes it easy to manage different environments and avoid hardcoding sensitive information.

### File Location

By default, the loader looks for `config.json` in the current directory. You can specify a different location:

```bash
./skyflow-loader -config /path/to/myconfig.json -token "TOKEN"
```

### Configuration Structure

```json
{
  "skyflow": {
    "vault_url": "https://your_vault.vault.skyflowapis.com",
    "bearer_token": "",
    "vaults": [
      {
        "name": "NAME",
        "id": "your_vault_id",
        "column": "name"
      }
    ]
  },
  "snowflake": {
    "user": "",
    "password": "",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "PUBLIC",
    "role": "YOUR_ROLE",
    "fetch_size": 100000,
    "query_mode": "simple"
  },
  "csv": {
    "data_directory": "data"
  },
  "performance": {
    "batch_size": 300,
    "max_concurrency": 32,
    "max_records": 100000,
    "append_suffix": true,
    "base_delay_ms": 0
  }
}
```

### Configuration Sections

#### Skyflow
- `vault_url` - Your Skyflow vault URL
- `bearer_token` - Bearer token for authentication (optional - can use CLI flag or interactive prompt)
- `vaults` - Array of vault configurations:
  - `name` - Vault name (NAME, ID, DOB, SSN)
  - `id` - Skyflow vault ID
  - `column` - Column name in the vault

#### Snowflake
- `user` - Snowflake username (optional - can use CLI flag or interactive prompt)
- `password` - Snowflake password or PAT token (optional - can use CLI flag or interactive prompt)
- `authenticator` - Authentication method (optional):
  - `""` (empty/default) - Standard username/password
  - `"programmatic_access_token"` - Use Programmatic Access Token (PAT)
  - `"SNOWFLAKE_JWT"` - Use key-pair authentication
- `account` - Account identifier (e.g., `ORG-ACCOUNT`)
- `warehouse` - Warehouse name
- `database` - Database name
- `schema` - Schema name (typically `PUBLIC`)
- `role` - Role name
- `fetch_size` - Number of rows to fetch per batch
- `query_mode` - `simple` or `union` (see [Data Sources](#data-sources))

#### CSV
- `data_directory` - Path to directory containing vault-specific CSV files (e.g., `name_data.csv`, `name_tokens.csv`, etc.)

#### Performance
- `batch_size` - Records per API call (default: 300)
- `max_concurrency` - Concurrent workers per vault (default: 32)
- `max_records` - Max records to process, 0 = unlimited (default: 100000)
- `append_suffix` - Add unique suffix to records (default: true)
- `base_delay_ms` - Delay between requests in ms (default: 0)

### Command-Line Overrides

All config file values can be overridden via command-line flags:

```bash
# Override Snowflake database
./skyflow-loader -token "TOKEN" -sf-database "PROD_DB"

# Override concurrency
./skyflow-loader -token "TOKEN" -concurrency 64

# Override vault URL
./skyflow-loader -token "TOKEN" -vault-url "https://different.vault.skyflowapis.com"
```

### Best Practices

1. **Keep config.json out of version control**
   ```bash
   echo "config.json" >> .gitignore
   ```

2. **Use config.example.json as a template**
   ```bash
   cp config.example.json config.json
   # Edit config.json with your values
   ```

3. **Credentials can be provided in three ways (in priority order):**
   ```bash
   # Option 1: Interactive prompts (most secure - recommended)
   ./skyflow-loader
   # Will prompt: 🔑 Enter Skyflow bearer token:
   # Will prompt: ❄️  Enter Snowflake username:
   # Will prompt: ❄️  Enter Snowflake password:

   # Option 2: Via CLI flags (good for automation)
   ./skyflow-loader -token "YOUR_TOKEN" -sf-user "user" -sf-password "pass"

   # Option 3: In config.json (less secure, but convenient)
   # Set "bearer_token", "user", and "password" in config.json
   ./skyflow-loader
   ```

4. **Use different configs for different environments**
   ```bash
   ./skyflow-loader -config config.dev.json
   ./skyflow-loader -config config.prod.json -token "PROD_TOKEN"
   ```

5. **For CI/CD, use CLI flags for secrets**
   - Store credentials in secret management system
   - Pass via CLI flags at runtime
   - Leave credentials empty in config.json

---

## Quick Start

Once you've configured `config.json`, running the loader is simple:

### Run with Interactive Prompts (Recommended)
```bash
# Will prompt for Skyflow bearer token
./skyflow-loader

# With Snowflake - will prompt for bearer token, username, and password
./skyflow-loader -source snowflake
```

### Run with Credentials via CLI
```bash
# Skyflow bearer token via command line
./skyflow-loader -token "YOUR_BEARER_TOKEN"

# Snowflake with all credentials via CLI
./skyflow-loader -source snowflake -token "TOKEN" -sf-user "user" -sf-password "pass"
```

### Run with Credentials in config.json
```bash
# If bearer_token, user, and password are set in config.json
./skyflow-loader -source snowflake
```

### Single Vault (Faster for Large Datasets)
```bash
# Process only the NAME vault
./skyflow-loader -vault name
```

### With Config File Overrides
```bash
# Use different config file
./skyflow-loader -config config.prod.json

# Override specific settings (token optional if in config)
./skyflow-loader -concurrency 64 -max-records 50000
```

---

## Snowflake Authentication Methods

The loader supports multiple Snowflake authentication methods:

### 1. Username/Password (Default)
Standard authentication with username and password.

**Config:**
```json
{
  "snowflake": {
    "user": "your_user",
    "password": "your_password",
    "authenticator": ""
  }
}
```

**CLI:**
```bash
./skyflow-loader -source snowflake -sf-user "user" -sf-password "pass"
```

### 2. Programmatic Access Token (PAT) - Recommended for EC2/SSH
Use Snowflake's Programmatic Access Tokens for secure, long-lived authentication without exposing passwords.

**How to generate PAT:**
1. Log into Snowflake UI
2. Go to your user profile → "Programmatic Access Tokens"
3. Click "Generate new token"
4. Copy the token

**Config:**
```json
{
  "snowflake": {
    "user": "your_user",
    "password": "YOUR_PAT_TOKEN_HERE",
    "authenticator": "programmatic_access_token"
  }
}
```

**CLI:**
```bash
./skyflow-loader -source snowflake \
  -sf-user "user" \
  -sf-password "YOUR_PAT_TOKEN" \
  -sf-authenticator "programmatic_access_token"
```

**Interactive (recommended):**
```bash
./skyflow-loader -source snowflake -sf-authenticator "programmatic_access_token"
# Will prompt: ❄️  Enter Snowflake username: your_user
# Will prompt: ❄️  Enter Snowflake password (or PAT token): [paste PAT token]
```

### 3. Key-Pair Authentication (JWT)
Use RSA key pairs for the most secure authentication method.

**Config:**
```json
{
  "snowflake": {
    "user": "your_user",
    "password": "",
    "authenticator": "SNOWFLAKE_JWT"
  }
}
```

> **Note:** Key-pair authentication requires additional configuration (private key file). See [Snowflake documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth) for setup.

---

## Data Sources

### CSV Files (Local)

**Structure Required:**
The loader expects vault-specific CSV files in a data directory:

**Data Files:**
- `name_data.csv` - Column: `full_name`
- `id_data.csv` - Column: `id`
- `dob_data.csv` - Column: `dob`
- `ssn_data.csv` - Column: `ssn`

**Token Files:**
- `name_tokens.csv` - Column: `full_name_token`
- `id_tokens.csv` - Column: `id_token`
- `dob_tokens.csv` - Column: `dob_token`
- `ssn_tokens.csv` - Column: `ssn_token`

**Example:**
```bash
./skyflow-loader \
  -token "YOUR_TOKEN" \
  -source csv \
  -data-dir "./data" \
  -max-records 10000
```

**Generate Test Data:**
Use the included mock data generator:
```bash
cd generator
./generate_mock_data 10000  # Creates 10k records per vault
cd ..
./skyflow-loader -source csv
```

### Snowflake Database

**Note:** Snowflake source defaults to **100 records** unless `-max-records` is specified. This prevents accidentally pulling millions of rows during testing.

#### Simple Mode (Default)
Queries a single table: `ELEVANCE.PUBLIC.PATIENTS`

```bash
# Test with default 100 records
./skyflow-loader \
  -token "YOUR_TOKEN" \
  -source snowflake \
  -sf-query-mode simple \
  -sf-database "SKYFLOW_DEMO" \
  -sf-schema "PUBLIC"

# Pull all records
./skyflow-loader \
  -token "YOUR_TOKEN" \
  -source snowflake \
  -sf-query-mode simple \
  -max-records 0
```

**Queries executed:**
```sql
SELECT DISTINCT UPPER(full_name) AS full_name, full_name_token
FROM ELEVANCE.PUBLIC.PATIENTS
WHERE full_name IS NOT NULL AND full_name_token IS NOT NULL
```

#### Union Mode (Advanced)
For complex schemas with multiple tables and UDF detokenization.

```bash
./skyflow-loader \
  -token "YOUR_TOKEN" \
  -source snowflake \
  -sf-query-mode union \
  -sf-database "D01_SKYFLOW_POC" \
  -sf-schema "SKYFLOW_POC"
```

**Queries executed:**
```sql
-- Example: SSN from CLM and MBR tables
SELECT SSN, SKFL_SSN_DETOK(SSN) AS ssn_token
FROM (
  SELECT SRC_MBR_SSN AS SSN FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
  UNION
  SELECT SSN FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
) DT
GROUP BY 1, 2
```

**Supported UDF Functions:**
- `SKFL_SSN_DETOK()`
- `SKFL_BIRTHDATE_DETOK()`
- `SKFL_MBR_NAME_DETOK()`
- `SKFL_MBR_IDENTIFIERS_DETOK()`

---

## Usage Examples

### Basic Examples

#### All 4 vaults from CSV
```bash
# With token in config.json
./skyflow-loader -source csv

# With token via CLI
./skyflow-loader -token "YOUR_TOKEN" -source csv
```

#### All 4 vaults from Snowflake
```bash
# With token in config.json
./skyflow-loader -source snowflake

# With token via CLI
./skyflow-loader -token "YOUR_TOKEN" -source snowflake
```

#### Single vault with custom settings
```bash
./skyflow-loader \
  -source snowflake \
  -vault ssn \
  -concurrency 64 \
  -batch-size 300 \
  -max-records 50000
```

### Advanced Examples

#### Maximum Performance - 4 Parallel Processes
Run each vault in a separate process for true parallelism (token from config.json):

```bash
./skyflow-loader -source snowflake -vault name &
./skyflow-loader -source snowflake -vault id &
./skyflow-loader -source snowflake -vault dob &
./skyflow-loader -source snowflake -vault ssn &
wait
```

#### High-Performance Configuration (Large Instances)
For large compute instances (32+ vCPU), use high concurrency with parallel processes:

```bash
./skyflow-loader -source snowflake -vault name -concurrency 128 &
./skyflow-loader -source snowflake -vault id -concurrency 128 &
./skyflow-loader -source snowflake -vault dob -concurrency 128 &
./skyflow-loader -source snowflake -vault ssn -concurrency 128 &
wait
```

#### Test with Limited Records
```bash
# Test with 1,000 records before full run
./skyflow-loader \
  -source snowflake \
  -max-records 1000 \
  -vault name
```

#### Generate Mock Data
```bash
# Create 10,000 mock records for testing
./skyflow-loader -generate 10000
```

#### Clear Vaults (TEST ONLY)
```bash
./skyflow-loader -clear
```

---

## Command-Line Reference

### Core Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-token` | *(from config)* | Skyflow bearer token (overrides config.json if provided) |
| `-source` | `csv` | Data source: `csv` or `snowflake` |
| `-vault` | *(all)* | Process specific vault: `name`, `id`, `dob`, or `ssn` |
| `-vault-url` | *(from config)* | Skyflow vault URL (overrides config.json) |

### CSV Source Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-data-dir` | `data` | Path to directory containing vault-specific CSV files |

### Snowflake Source Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-sf-user` | *(from config or prompt)* | Snowflake username |
| `-sf-password` | *(from config or prompt)* | Snowflake password or PAT token |
| `-sf-authenticator` | *(from config, default: snowflake)* | Auth method: `snowflake`, `programmatic_access_token`, `SNOWFLAKE_JWT` |
| `-sf-account` | `JYSROBN-PROVIDER_1` | Snowflake account identifier |
| `-sf-warehouse` | `APP_WH` | Snowflake warehouse |
| `-sf-database` | `SKYFLOW_DEMO` | Snowflake database |
| `-sf-schema` | `PUBLIC` | Snowflake schema |
| `-sf-role` | `ACCOUNTADMIN` | Snowflake role |
| `-sf-fetch-size` | `100000` | Snowflake fetch batch size |
| `-sf-query-mode` | `simple` | Query mode: `simple` or `union` |

### Performance Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-batch-size` | `300` | Records per API batch |
| `-concurrency` | `32` | Concurrent workers per vault |
| `-max-records` | `100000` (CSV) / `100` (Snowflake) | Max records per vault (0=unlimited) |
| `-append-suffix` | `true` | Append unique suffix to data/tokens |
| `-base-delay-ms` | `0` | Delay between requests (milliseconds) |

### Utility Flags

| Flag | Description |
|------|-------------|
| `-generate N` | Generate N mock records and exit |
| `-clear` | Clear all vault data before loading |
| `-help` | Display all available flags |

---

## Performance Optimization

### Tuning Concurrency

The `-concurrency` flag controls how many parallel API requests are made per vault.

**Guidelines:**
- **Small datasets (<10K)**: 16-32 workers
- **Medium datasets (10K-100K)**: 32-64 workers
- **Large datasets (>100K)**: 64-128 workers
- **Snowflake union queries**: Start with 32, monitor Snowflake load

**Example:**
```bash
# High concurrency for large dataset
./skyflow-loader -source snowflake -concurrency 128
```

### Batch Size

The `-batch-size` flag controls records per API call.

**Recommendations:**
- Default `300` works well for most cases
- Increase to `500` for very small records
- Decrease to `100-200` for large records or high error rates

### Process-Level Parallelism

For maximum throughput, run 4 separate processes (one per vault):

```bash
for vault in name id dob ssn; do
  ./skyflow-loader -source snowflake -vault $vault &
done
wait
```

**Why this works:**
- Each process has its own connection pool
- Bypasses any single-process bottlenecks
- Scales linearly on multi-core systems

### Snowflake Performance

**Query Mode Selection:**
- Use `simple` mode for single-table schemas
- Use `union` mode for complex multi-table schemas with UDFs

**Fetch Size:**
```bash
# Larger fetch size = fewer round trips to Snowflake
./skyflow-loader -sf-fetch-size 200000 ...
```

**Connection Pooling:**
- The loader uses 10 max open connections by default
- Pooling is automatic for sequential vault processing

---

## Architecture

### Data Flow

```
┌─────────────────┐
│  Data Source    │
│  (CSV/Snowflake)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Read Records   │
│  (Streaming)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Create Batches │
│  (300 records)  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│   Worker Pool (32 concurrent)   │
│  ┌──────┐ ┌──────┐ ┌──────┐    │
│  │Worker│ │Worker│ │Worker│ ...│
│  └──┬───┘ └──┬───┘ └──┬───┘    │
└─────┼────────┼────────┼─────────┘
      │        │        │
      ▼        ▼        ▼
┌──────────────────────────┐
│   Skyflow API            │
│   (HTTP/2 with pooling)  │
└──────────────────────────┘
```

### Vault Processing Modes

**Sequential (default):**
```
NAME → ID → DOB → SSN
```
All vaults share one data source connection.

**Parallel (recommended for large datasets):**
```
NAME ─┐
ID ───┼─→ All run simultaneously
DOB ──┤
SSN ──┘
```
Each vault has its own process and connection.

### Real-Time Metrics

The loader provides live monitoring during execution:

**Live Metrics (updates every 3 seconds):**
```
[LIVE] Workers: 32/32 | HTTP: 28 in-flight | Req: 15/s | Rec: 1450/s | Latency: avg=1280ms min=108ms max=3800ms | 429s: 0
```
- **Workers**: Active workers / Total workers
- **HTTP**: In-flight HTTP requests
- **Req/s**: API requests per second
- **Rec/s**: Records processed per second
- **Latency**: Average, minimum, and maximum API response times
- **429s**: Rate limit responses encountered

**Progress Updates (every 1% or 10k records):**
```
Progress: 50000/100000 records (50.0%) - 1298 records/sec | Batches: 500✅ (498 immediate, 2 retried) 0❌ (100% success) | 429s: 0
```

**Final Performance Summary:**
```
NAME VAULT PERFORMANCE:
  Records Uploaded:      100,000 (successfully processed)
  Processing Time:       77.1 seconds
  Throughput:            1,298 records/sec (successful only)
  Batch Success Rate:    1000/1000 batches (100.0%)

  API RESPONSE SUMMARY:
    ✅ Immediate Successes: 998 (99.8% of batches)
    🔄 Retried Successes:   2 (0.2% of batches)

  DETAILED TIMING BREAKDOWN (Cumulative across all parallel workers):
    Component                   Cumulative % of Total  Est. Wall Clock
    ------------------------- ------------ ---------- ----------------
    Data Source Read                0.0s       0.0%           0.0s
    Record Creation                 0.0s       0.0%           0.0s
    Suffix Generation               0.0s       0.0%           0.0s
    Payload Creation                0.1s       0.0%           0.0s
    JSON Serialization              0.0s       0.0%           0.0s
    BASE_REQUEST_DELAY              0.0s       0.0%           0.0s
    Skyflow API Calls             212.1s      99.9%           7.7s
    Retry Delays                    0.0s       0.0%           0.0s
    ------------------------- ------------ ---------- ----------------
    TOTAL (Cumulative)            212.3s     100.0%           7.7s (actual)

    Average Concurrency: 27.5x (concurrent workers executing simultaneously)
```

### Error Logging

When batches fail permanently (after all retries), the loader automatically creates detailed error logs:

**Error Log File:** `error_log_<vault>_<timestamp>.json`

**Example: `error_log_ID_20251009_153045.json`**
```json
{
  "vault_name": "ID",
  "vault_id": "abc123",
  "column": "id",
  "timestamp": "2025-10-09T15:30:45Z",
  "total_errors": 3,
  "failed_records": 300,
  "errors": [
    {
      "batch_number": 42,
      "records": [
        {"Value": "123", "Token": "tok_456"},
        ...
      ],
      "error": "API request failed with status 500 after 5 retries",
      "status_code": 500,
      "timestamp": "2025-10-09T15:30:12Z"
    }
  ]
}
```

**Key Features:**
- ✅ Only logs permanent failures (successful retries are NOT logged)
- ✅ Contains complete record data (values + tokens) for re-processing
- ✅ Includes error details and HTTP status codes
- ✅ Automatic summary at end of run
- ✅ One file per vault with failures

**Re-running Failed Records:**
Error logs contain all data needed to identify and re-run failed batches separately.

---

## Performance Testing & Monitoring Utilities

### Benchmark Scripts

**Quick Benchmark (5 configs, ~5-10 minutes):**
```bash
./benchmark_quick.sh
```
Tests: 32/100, 32/300, 50/100, 100/300, 200/50

**Full Benchmark (13 configs, ~20-30 minutes):**
```bash
./benchmark_configs.sh
```
Comprehensive testing across multiple concurrency and batch size combinations.

**Output:** `benchmark_results_<timestamp>.csv`

### Scaling Analysis

Analyze benchmark results to find optimal configuration for large-scale loads:

```bash
python3 analyze_scaling.py benchmark_results_20251009_185739.csv
```

**Provides:**
- Projected times for 500M records
- Efficiency scores for each configuration
- Risk assessment for long-running jobs
- Top 3 recommended configurations

### Live Monitoring Dashboard

Monitor loader performance in real-time (run in separate SSH window):

```bash
./monitor_live.sh
```

**Displays:**
- EC2 system resources (CPU, memory, network, disk)
- Loader process stats (PID, runtime, memory, threads)
- Real-time analysis and warnings
- Active TCP connections

### Vault Clearing Utility

Clear vault data between test runs:

```bash
# Build the utility
go build -o clear-vaults clear_vaults.go

# Clear all vaults
./clear-vaults
```

---

## Troubleshooting

### Common Issues

#### "Failed to connect to Snowflake"
- Verify account identifier format: `ACCOUNT-LOCATOR` or `ORG-ACCOUNT`
- Check network connectivity to Snowflake
- Ensure credentials are correct
- Try with `-sf-query-mode simple` first

#### "Column not found" (CSV mode)
- Verify CSV headers match expected column names
- Data file: `full_name`, `id`, `dob`, `ssn`
- Token file: `full_name_token`, `id_token`, `dob_token`, `ssn_token`

#### "Too many failed batches"
- Reduce `-concurrency` to lower API load
- Check Skyflow API rate limits
- Verify bearer token is valid and not expired
- **Review error log file** (`error_log_<vault>_<timestamp>.json`) for specific API errors and failed records

#### Slow performance
- Increase `-concurrency` (try 64 or 128)
- Use process-level parallelism (one process per vault)
- For Snowflake, increase `-sf-fetch-size`
- Ensure adequate network bandwidth

#### Out of memory
- Reduce `-batch-size` to 100-200
- Reduce `-concurrency` to 16-32
- Use `-max-records` to process in chunks
- Use single-vault mode and process vaults separately

### Debug Tips

1. **Test with small dataset first:**
   ```bash
   ./skyflow-loader -max-records 1000 -vault name
   ```

2. **Check Snowflake queries:**
   Add `-sf-query-mode simple` and verify connectivity before using union mode

3. **Monitor system resources:**
   ```bash
   # Run loader and monitor
   ./skyflow-loader ... &
   top -pid $!
   ```

---

## Performance Characteristics

**100K records:**
- Sequential: ~45 seconds (all 4 vaults)
- Parallel: ~12 seconds (4 processes)

**1M records:**
- Sequential: ~7.5 minutes (all 4 vaults)
- Parallel: ~2 minutes (4 processes)

**10M+ records:**
- Use parallel processing for best results
- Consider `-concurrency 128` on large instances

---

## License

Same as parent project.

---

## Additional Resources

- **Go Documentation**: [https://pkg.go.dev](https://pkg.go.dev)
- **Snowflake Go Driver**: [github.com/snowflakedb/gosnowflake](https://github.com/snowflakedb/gosnowflake)
- **Skyflow Documentation**: [https://docs.skyflow.com](https://docs.skyflow.com)
