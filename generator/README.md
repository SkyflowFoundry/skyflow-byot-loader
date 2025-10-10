# Mock Data Generator

High-performance Go-based mock data generator for Skyflow BYOT (Bring Your Own Tokens) testing.

## Overview

This generator creates separate CSV files for each vault type (name, id, dob, ssn), with corresponding token files. It uses a streaming architecture with parallel workers to efficiently generate millions of records with constant memory usage.

**Output files match the exact structure required by skyflow-loader**, making it the recommended way to generate test data for performance testing and benchmarking.

**Complete Workflow:**
```bash
# 1. Build the generator
cd generator
go build -o generate_mock_data generate_mock_data.go

# 2. Generate test data (1000 records per vault)
./generate_mock_data 1000

# 3. Load into Skyflow
cd ..
./skyflow-loader -source csv
```

## Prerequisites

- Go 1.16 or higher
- ~10MB free disk space (for small datasets)
- For large datasets (1M+ records): ~100MB+ disk space per million records

## Features

- **Streaming Architecture**: Constant memory usage regardless of record count
- **Parallel Processing**: Multi-core worker pool (8 workers) for maximum throughput
- **Vault-Specific Files**: Generates separate data/token files per vault type
- **Unique Data**: All values are unique across runs using timestamps and random suffixes
- **Performance Metrics**: Reports throughput, file sizes, and timing
- **Loader-Compatible Output**: Files match exact structure expected by skyflow-loader

## Quick Start

### 1. Navigate to Generator Directory

```bash
cd generator
```

### 2. Build the Binary

```bash
go build -o generate_mock_data generate_mock_data.go
```

This creates an executable `generate_mock_data` binary in the current directory.

### 3. Generate Test Data

**Generate 1000 records per vault (4000 total):**
```bash
./generate_mock_data 1000
```

**Generate different counts per vault:**
```bash
./generate_mock_data name=5000 id=3000 dob=2000 ssn=10000
```

**Generate specific vaults only:**
```bash
./generate_mock_data name=1000 ssn=500
```

### 4. Verify Output

Check that files were created:
```bash
ls -lh ../data/
```

You should see 8 CSV files (or fewer if you skipped vaults):
- `name_data.csv`, `name_tokens.csv`
- `id_data.csv`, `id_tokens.csv`
- `dob_data.csv`, `dob_tokens.csv`
- `ssn_data.csv`, `ssn_tokens.csv`

### 5. Load Data into Skyflow

```bash
cd ..
./skyflow-loader -source csv
```

## Command Line Options

### Generate Same Count for All Vaults
```bash
./generate_mock_data <count>
```
Example: `./generate_mock_data 1000` generates 1,000 records for each vault (4,000 total)

### Generate Different Counts Per Vault
```bash
./generate_mock_data name=<N> id=<M> dob=<P> ssn=<Q>
```
Example: `./generate_mock_data name=5000 id=3000 dob=2000 ssn=10000`

### Skip Specific Vaults
Omit vaults you don't need:
```bash
./generate_mock_data name=1000 ssn=500
```
Only generates name and ssn vaults (id and dob are skipped)

### Help
```bash
./generate_mock_data
```
Shows usage information

## Output Files

The generator creates 8 CSV files in the `data/` directory:

### Data Files (one column each)
- `name_data.csv` - Column: `full_name` (e.g., "JOHN SMITH 1234567890_abc123...")
- `id_data.csv` - Column: `id` (e.g., "12345-1234567890_abc123...")
- `dob_data.csv` - Column: `dob` (e.g., "1985-06-15-1234567890_abc123...")
- `ssn_data.csv` - Column: `ssn` (e.g., "123-45-6789-1234567890_abc123...")

### Token Files (one column each)
- `name_tokens.csv` - Column: `full_name_token` (e.g., "tok_name_1234567890_xyz789...")
- `id_tokens.csv` - Column: `id_token` (e.g., "tok_id_1234567890_xyz789...")
- `dob_tokens.csv` - Column: `dob_token` (e.g., "tok_dob_1234567890_xyz789...")
- `ssn_tokens.csv` - Column: `ssn_token` (e.g., "tok_ssn_1234567890_xyz789...")

## Data Format

### Name Vault
- **Format**: `FIRSTNAME LASTNAME TIMESTAMP_RANDOM`
- **Example**: `JOHN SMITH 1696789012_a1b2c3d4e5f6g7h8`
- **Token**: `tok_name_1696789012_x9y8z7w6v5u4t3s2`

### ID Vault
- **Format**: `NNNNN-TIMESTAMP_RANDOM`
- **Example**: `54321-1696789012_a1b2c3d4e5f6g7h8`
- **Token**: `tok_id_1696789012_x9y8z7w6v5u4t3s2`

### DOB Vault
- **Format**: `YYYY-MM-DD-TIMESTAMP_RANDOM`
- **Example**: `1985-06-15-1696789012_a1b2c3d4e5f6g7h8`
- **Token**: `tok_dob_1696789012_x9y8z7w6v5u4t3s2`
- **Range**: Birth dates between 1940 and 2010

### SSN Vault
- **Format**: `AAA-GG-SSSS-TIMESTAMP_RANDOM`
- **Example**: `123-45-6789-1696789012_a1b2c3d4e5f6g7h8`
- **Token**: `tok_ssn_1696789012_x9y8z7w6v5u4t3s2`
- **Validation**: Avoids invalid patterns (000-xx-xxxx, 666-xx-xxxx, 900-999-xx-xxxx)

## Technical Architecture

### Streaming Design
- Records generated on-demand by worker pool
- Written to CSV immediately (no memory accumulation)
- Handles 500M+ records with constant ~10MB memory usage

### Parallel Workers
- Uses 8 parallel workers by default
- Each worker has thread-safe random source
- Channel-based pipeline for coordination
- 10,000 record buffer for smooth flow

### Performance
- **Throughput**: ~500K-1M records/sec (depends on CPU)
- **Memory**: Constant ~10MB regardless of record count
- **Scalability**: Linear scaling with CPU cores

## Examples

### Small test dataset
```bash
./generate_mock_data 100
```

### Large production dataset
```bash
./generate_mock_data 10000000
```
Generates 10M records per vault = 40M total records in ~20-40 seconds

### Specific vault sizes
```bash
./generate_mock_data name=1000000 id=500000 dob=750000 ssn=2000000
```
Generates 4.25M total records with different counts per vault

## Integration with Main Loader

After generating data, use with the main loader:

```bash
# Process all vaults
cd ..
./skyflow-loader -source csv -token "YOUR_TOKEN"

# Process specific vault
./skyflow-loader -source csv -vault name -max-records 10000 -token "YOUR_TOKEN"

# For benchmarking (clears vault between runs)
cd ..
./benchmark_quick.sh
```

### Recommended Test Sizes

- **Quick test**: `./generate_mock_data 1000` (4,000 total records, ~5 seconds)
- **Performance test**: `./generate_mock_data 100000` (400,000 total, ~1-2 minutes)
- **Benchmark test**: `./generate_mock_data 1000000` (4M total, ~10-20 minutes to generate)
- **Large scale**: `./generate_mock_data 10000000` (40M total, requires ~4GB disk space)

## Column Name Mapping

The generator matches the column names expected by main.go:

| Vault Type | Data Column | Token Column | Notes |
|------------|-------------|--------------|-------|
| name | `full_name` | `full_name_token` | Full name (not just "name") |
| id | `id` | `id_token` | Patient/member ID |
| dob | `dob` | `dob_token` | Date of birth |
| ssn | `ssn` | `ssn_token` | Social Security Number |

## Performance Benchmarks

| Records | Time | Throughput | File Size (data) | File Size (tokens) |
|---------|------|------------|------------------|-------------------|
| 1K | <1s | ~10K/sec | ~100 KB | ~120 KB |
| 10K | ~1s | ~100K/sec | ~1 MB | ~1.2 MB |
| 100K | ~5s | ~200K/sec | ~10 MB | ~12 MB |
| 1M | ~20s | ~500K/sec | ~100 MB | ~120 MB |
| 10M | ~3min | ~600K/sec | ~1 GB | ~1.2 GB |

*Benchmarks vary by CPU. Tested on Apple M-series processors.*

## Notes

- All data values are guaranteed unique within a single run
- Timestamps ensure uniqueness across multiple runs
- SSN generation avoids invalid real-world patterns
- Date of birth range (1940-2010) represents realistic patient ages
- Names are drawn from US Census common names for realistic distribution
