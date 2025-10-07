package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
	"golang.org/x/term"
)

// FileConfig represents the structure of config.json
type FileConfig struct {
	Skyflow     SkyflowConfig     `json:"skyflow"`
	Snowflake   SnowflakeFileConfig `json:"snowflake"`
	CSV         CSVConfig         `json:"csv"`
	Performance PerformanceConfig `json:"performance"`
}

type SkyflowConfig struct {
	VaultURL    string        `json:"vault_url"`
	BearerToken string        `json:"bearer_token"`
	Vaults      []VaultConfig `json:"vaults"`
}

type SnowflakeFileConfig struct {
	User      string `json:"user"`
	Password  string `json:"password"`
	Account   string `json:"account"`
	Warehouse string `json:"warehouse"`
	Database  string `json:"database"`
	Schema    string `json:"schema"`
	Role      string `json:"role"`
	FetchSize int    `json:"fetch_size"`
	QueryMode string `json:"query_mode"`
}

type CSVConfig struct {
	DataFile  string `json:"data_file"`
	TokenFile string `json:"token_file"`
}

type PerformanceConfig struct {
	BatchSize      int  `json:"batch_size"`
	MaxConcurrency int  `json:"max_concurrency"`
	MaxRecords     int  `json:"max_records"`
	AppendSuffix   bool `json:"append_suffix"`
	BaseDelayMs    int  `json:"base_delay_ms"`
}

// Configuration (runtime config used by the application)
type Config struct {
	VaultURL           string
	BearerToken        string
	BatchSize          int
	MaxConcurrency     int
	MaxRecords         int
	AppendSuffix       bool
	DataSource         string // "csv" or "snowflake"
	DataFile           string
	TokenFile          string
	ProgressInterval   int
	BaseRequestDelay   time.Duration
	SnowflakeConfig    SnowflakeConfig
}

// Snowflake configuration
type SnowflakeConfig struct {
	User      string
	Password  string
	Account   string
	Warehouse string
	Database  string
	Schema    string
	Role      string
	FetchSize int
	QueryMode string // "simple" or "union"
}

// Vault configuration
type VaultConfig struct {
	Name   string `json:"name"`
	ID     string `json:"id"`
	Column string `json:"column"`
}

// Record for BYOT
type Record struct {
	Value string
	Token string
}

// DataSource interface for reading data from different sources
type DataSource interface {
	Connect() error
	Close() error
	ReadRecords(vaultConfig VaultConfig, maxRecords int) ([]Record, error)
}

// Performance metrics with atomic operations for thread safety
type Metrics struct {
	VaultName            string
	TotalRecords         int64
	SuccessfulBatches    int64
	FailedBatches        int64
	SnowflakeFetchTime   int64 // nanoseconds
	RecordCreationTime   int64
	SuffixGenTime        int64
	PayloadCreationTime  int64
	JSONSerializationTime int64
	BaseDelayTime        int64
	APICallTime          int64
	RetryDelayTime       int64
	StartTime            time.Time
	EndTime              time.Time
}

func (m *Metrics) AddRecord() {
	atomic.AddInt64(&m.TotalRecords, 1)
}

func (m *Metrics) AddSuccessfulBatch() {
	atomic.AddInt64(&m.SuccessfulBatches, 1)
}

func (m *Metrics) AddFailedBatch() {
	atomic.AddInt64(&m.FailedBatches, 1)
}

func (m *Metrics) AddTime(component string, duration time.Duration) {
	nanos := duration.Nanoseconds()
	switch component {
	case "csv_read":
		atomic.AddInt64(&m.SnowflakeFetchTime, nanos)
	case "record_creation":
		atomic.AddInt64(&m.RecordCreationTime, nanos)
	case "suffix_gen":
		atomic.AddInt64(&m.SuffixGenTime, nanos)
	case "payload_creation":
		atomic.AddInt64(&m.PayloadCreationTime, nanos)
	case "json_serialization":
		atomic.AddInt64(&m.JSONSerializationTime, nanos)
	case "base_delay":
		atomic.AddInt64(&m.BaseDelayTime, nanos)
	case "api_call":
		atomic.AddInt64(&m.APICallTime, nanos)
	case "retry_delay":
		atomic.AddInt64(&m.RetryDelayTime, nanos)
	}
}

func (m *Metrics) GetDuration(component string) time.Duration {
	var nanos int64
	switch component {
	case "csv_read":
		nanos = atomic.LoadInt64(&m.SnowflakeFetchTime)
	case "record_creation":
		nanos = atomic.LoadInt64(&m.RecordCreationTime)
	case "suffix_gen":
		nanos = atomic.LoadInt64(&m.SuffixGenTime)
	case "payload_creation":
		nanos = atomic.LoadInt64(&m.PayloadCreationTime)
	case "json_serialization":
		nanos = atomic.LoadInt64(&m.JSONSerializationTime)
	case "base_delay":
		nanos = atomic.LoadInt64(&m.BaseDelayTime)
	case "api_call":
		nanos = atomic.LoadInt64(&m.APICallTime)
	case "retry_delay":
		nanos = atomic.LoadInt64(&m.RetryDelayTime)
	}
	return time.Duration(nanos)
}

func (m *Metrics) Duration() time.Duration {
	return m.EndTime.Sub(m.StartTime)
}

func (m *Metrics) Throughput() float64 {
	duration := m.Duration().Seconds()
	if duration > 0 {
		return float64(atomic.LoadInt64(&m.TotalRecords)) / duration
	}
	return 0
}

// Buffer pool for JSON encoding - reduces GC pressure
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Optimized HTTP client with connection pooling
func createHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		DisableKeepAlives:   false,
		ForceAttemptHTTP2:   true, // Enable HTTP/2
	}

	return &http.Client{
		Transport: transport,
		Timeout:   120 * time.Second,
	}
}

// Generate unique suffix (optimized with pre-allocated buffer)
var suffixChars = []byte("abcdefghijklmnopqrstuvwxyz0123456789")

func generateUniqueSuffix() string {
	suffix := make([]byte, 16)
	for i := range suffix {
		suffix[i] = suffixChars[rand.Intn(len(suffixChars))]
	}
	return fmt.Sprintf("%d_%s", time.Now().Unix(), suffix)
}

// CSVDataSource implements DataSource interface for local CSV files
type CSVDataSource struct {
	DataFile  string
	TokenFile string
}

// Connect validates CSV files exist
func (c *CSVDataSource) Connect() error {
	if _, err := os.Stat(c.DataFile); os.IsNotExist(err) {
		return fmt.Errorf("data file not found: %s", c.DataFile)
	}
	if _, err := os.Stat(c.TokenFile); os.IsNotExist(err) {
		return fmt.Errorf("token file not found: %s", c.TokenFile)
	}
	return nil
}

// Close is a no-op for CSV files
func (c *CSVDataSource) Close() error {
	return nil
}

// ReadRecords reads records from CSV files
func (c *CSVDataSource) ReadRecords(vaultConfig VaultConfig, maxRecords int) ([]Record, error) {
	// Open data file
	dataFile, err := os.Open(c.DataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}
	defer dataFile.Close()

	// Open token file
	tokenFile, err := os.Open(c.TokenFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open token file: %w", err)
	}
	defer tokenFile.Close()

	dataReader := csv.NewReader(dataFile)
	tokenReader := csv.NewReader(tokenFile)

	// Optimize CSV reading
	dataReader.ReuseRecord = true
	tokenReader.ReuseRecord = true

	// Read headers
	dataHeaders, err := dataReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read data headers: %w", err)
	}
	dataHeaders = append([]string(nil), dataHeaders...) // Copy headers

	tokenHeaders, err := tokenReader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read token headers: %w", err)
	}
	tokenHeaders = append([]string(nil), tokenHeaders...) // Copy headers

	// Find column indices
	dataColIdx := -1
	tokenColIdx := -1

	dataColName := getDataColumnName(vaultConfig.Column)
	tokenColName := getTokenColumnName(vaultConfig.Column)

	for i, h := range dataHeaders {
		if h == dataColName {
			dataColIdx = i
			break
		}
	}

	for i, h := range tokenHeaders {
		if h == tokenColName {
			tokenColIdx = i
			break
		}
	}

	if dataColIdx == -1 || tokenColIdx == -1 {
		return nil, fmt.Errorf("column not found: data=%s token=%s", dataColName, tokenColName)
	}

	// Pre-allocate slice with capacity
	capacity := maxRecords
	if capacity <= 0 {
		capacity = 10000
	}
	records := make([]Record, 0, capacity)
	recordCount := 0

	for {
		if maxRecords > 0 && recordCount >= maxRecords {
			break
		}

		dataRow, err := dataReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading data row: %w", err)
		}

		tokenRow, err := tokenReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading token row: %w", err)
		}

		if dataColIdx < len(dataRow) && tokenColIdx < len(tokenRow) {
			value := dataRow[dataColIdx]
			token := tokenRow[tokenColIdx]

			if value != "" && token != "" {
				// Copy strings to avoid retaining CSV reader's internal buffer
				records = append(records, Record{
					Value: strings.Clone(value),
					Token: strings.Clone(token),
				})
				recordCount++
			}
		}
	}

	return records, nil
}

// SnowflakeDataSource implements DataSource interface for Snowflake
type SnowflakeDataSource struct {
	Config SnowflakeConfig
	DB     *sql.DB
}

// Connect establishes connection to Snowflake
func (s *SnowflakeDataSource) Connect() error {
	// Build DSN (Data Source Name)
	// Format: user:password@account/database/schema?warehouse=wh&role=role
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
		s.Config.User,
		s.Config.Password,
		s.Config.Account,
		s.Config.Database,
		s.Config.Schema,
		s.Config.Warehouse,
		s.Config.Role,
	)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open Snowflake connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping Snowflake: %w", err)
	}

	s.DB = db
	fmt.Println("✅ Successfully connected to Snowflake")
	return nil
}

// Close closes the Snowflake connection
func (s *SnowflakeDataSource) Close() error {
	if s.DB != nil {
		return s.DB.Close()
	}
	return nil
}

// buildSimpleQuery creates query for ELEVANCE.PUBLIC.PATIENTS table
func (s *SnowflakeDataSource) buildSimpleQuery(vaultConfig VaultConfig) string {
	var query string
	switch vaultConfig.Column {
	case "name":
		query = `SELECT DISTINCT UPPER(full_name) AS full_name, full_name_token
				FROM ELEVANCE.PUBLIC.PATIENTS
				WHERE full_name IS NOT NULL AND full_name_token IS NOT NULL`
	case "id":
		query = `SELECT DISTINCT TO_VARCHAR(id) AS id, id_token
				FROM ELEVANCE.PUBLIC.PATIENTS
				WHERE id IS NOT NULL AND id_token IS NOT NULL`
	case "dob":
		query = `SELECT DISTINCT TO_VARCHAR(dob) AS dob, dob_token
				FROM ELEVANCE.PUBLIC.PATIENTS
				WHERE dob IS NOT NULL AND dob_token IS NOT NULL`
	case "ssn":
		query = `SELECT DISTINCT TO_VARCHAR(ssn) AS ssn, ssn_token
				FROM ELEVANCE.PUBLIC.PATIENTS
				WHERE ssn IS NOT NULL AND ssn_token IS NOT NULL`
	}
	return query
}

// buildUnionQuery creates query for D01_SKYFLOW_POC with UNIONs and UDF detokenization
func (s *SnowflakeDataSource) buildUnionQuery(vaultConfig VaultConfig) string {
	var query string
	switch vaultConfig.Column {
	case "name":
		// First name, Last name, Middle initial from both CLM and MBR tables
		query = `SELECT NAME, SKFL_MBR_NAME_DETOK(NAME) AS name_token
FROM (
	SELECT SRC_MBR_FRST_NM AS NAME FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT FRST_NM AS NAME FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
	UNION
	SELECT SRC_MBR_LAST_NM AS NAME FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT LAST_NM AS NAME FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
	UNION
	SELECT SRC_MBR_MID_INIT_NM AS NAME FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT MID_INIT_NM AS NAME FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
) DT
GROUP BY 1, 2`
	case "id":
		// Multiple ID types from both CLM and MBR tables
		query = `SELECT ID, SKFL_MBR_IDENTIFIERS_DETOK(ID) AS id_token
FROM (
	SELECT SRC_ENRLMNT_ID AS ID FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT SRC_ENRLMNT_ID AS ID FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
	UNION
	SELECT SRC_HC_ID AS ID FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT HC_ID AS ID FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
	UNION
	SELECT SRC_SBSCRBR_ID AS ID FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT SBSCRBR_ID AS ID FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
) DT
GROUP BY 1, 2`
	case "dob":
		// Birth date from both CLM and MBR tables
		query = `SELECT BRTH_DT, SKFL_BIRTHDATE_DETOK(BRTH_DT) AS dob_token
FROM (
	SELECT SRC_MBR_BRTH_DT AS BRTH_DT FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM
	UNION
	SELECT BRTH_DT FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR
) DT
GROUP BY 1, 2`
	case "ssn":
		// SSN from both CLM and MBR tables
		query = `SELECT SSN, SKFL_SSN_DETOK(SSN) AS ssn_token
FROM (
	SELECT SRC_MBR_SSN AS SSN FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT SSN FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
) DT
GROUP BY 1, 2`
	}
	return query
}

// ReadRecords reads records from Snowflake
func (s *SnowflakeDataSource) ReadRecords(vaultConfig VaultConfig, maxRecords int) ([]Record, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("no active Snowflake connection")
	}

	var query string

	// Choose query based on mode
	if s.Config.QueryMode == "union" {
		// D01_SKYFLOW_POC mode: Complex UNION queries with detokenization UDFs
		query = s.buildUnionQuery(vaultConfig)
	} else {
		// Simple mode (default): ELEVANCE.PUBLIC.PATIENTS table
		query = s.buildSimpleQuery(vaultConfig)
	}

	// Add LIMIT if specified
	if maxRecords > 0 {
		query += fmt.Sprintf(" LIMIT %d", maxRecords)
	}

	fmt.Printf("📊 Querying Snowflake for %s data...\n", vaultConfig.Name)

	// Execute query
	rows, err := s.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Pre-allocate slice
	capacity := maxRecords
	if capacity <= 0 {
		capacity = 10000
	}
	records := make([]Record, 0, capacity)

	// Stream results
	recordCount := 0
	for rows.Next() {
		var value, token string
		if err := rows.Scan(&value, &token); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if value != "" && token != "" {
			records = append(records, Record{
				Value: value,
				Token: token,
			})
			recordCount++
		}

		if maxRecords > 0 && recordCount >= maxRecords {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	fmt.Printf("✅ Retrieved %d records from Snowflake\n", len(records))
	return records, nil
}

func getDataColumnName(column string) string {
	switch column {
	case "name":
		return "full_name"
	case "id":
		return "id"
	case "dob":
		return "dob"
	case "ssn":
		return "ssn"
	default:
		return column
	}
}

func getTokenColumnName(column string) string {
	return getDataColumnName(column) + "_token"
}

// Create BYOT payload (optimized with buffer pool)
func createBYOTPayload(records []Record, vaultConfig VaultConfig, config *Config, metrics *Metrics) ([]byte, error) {
	payloadStart := time.Now()

	recordsJSON := make([]map[string]interface{}, 0, len(records))

	for _, record := range records {
		value := record.Value
		token := record.Token

		if config.AppendSuffix {
			suffixStart := time.Now()
			dataSuffix := generateUniqueSuffix()
			tokenSuffix := generateUniqueSuffix()
			value = value + "_" + dataSuffix
			token = token + "_" + tokenSuffix
			metrics.AddTime("suffix_gen", time.Since(suffixStart))
		}

		recordsJSON = append(recordsJSON, map[string]interface{}{
			"fields": map[string]string{
				vaultConfig.Column: value,
			},
			"tokens": map[string]string{
				vaultConfig.Column: token,
			},
			"upsert": map[string]string{
				"column": vaultConfig.Column,
			},
		})
	}

	payload := map[string]interface{}{
		"records":         recordsJSON,
		"continueOnError": true,
		"tokenization":    true,
		"byot":            "ENABLE",
	}

	metrics.AddTime("payload_creation", time.Since(payloadStart))

	// Use buffer pool for JSON marshaling
	jsonStart := time.Now()
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(payload); err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Copy buffer content
	jsonData := make([]byte, buf.Len())
	copy(jsonData, buf.Bytes())

	metrics.AddTime("json_serialization", time.Since(jsonStart))

	return jsonData, nil
}

// Send batch to Skyflow (optimized with shared HTTP client)
func sendBatch(client *http.Client, config *Config, vaultConfig VaultConfig, batch []Record, batchNum int, metrics *Metrics) error {
	// Base delay
	if config.BaseRequestDelay > 0 {
		delayStart := time.Now()
		time.Sleep(config.BaseRequestDelay)
		metrics.AddTime("base_delay", time.Since(delayStart))
	}

	// Create payload
	payload, err := createBYOTPayload(batch, vaultConfig, config, metrics)
	if err != nil {
		return fmt.Errorf("failed to create payload: %w", err)
	}

	url := fmt.Sprintf("%s/v1/vaults/%s/%s", config.VaultURL, vaultConfig.ID, vaultConfig.Column)

	// Retry logic with exponential backoff
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+config.BearerToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "gzip")

		apiStart := time.Now()
		resp, err := client.Do(req)
		apiDuration := time.Since(apiStart)
		metrics.AddTime("api_call", apiDuration)

		if err != nil {
			if attempt < maxRetries-1 {
				retryStart := time.Now()
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				time.Sleep(backoff)
				metrics.AddTime("retry_delay", time.Since(retryStart))
				continue
			}
			metrics.AddFailedBatch()
			return fmt.Errorf("API request failed after retries: %w", err)
		}

		// Read and close body immediately to reuse connection
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 || resp.StatusCode == 201 {
			metrics.AddSuccessfulBatch()
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			if attempt < maxRetries-1 {
				retryStart := time.Now()
				backoff := time.Duration(2<<uint(attempt)) * time.Second
				time.Sleep(backoff)
				metrics.AddTime("retry_delay", time.Since(retryStart))
				continue
			}
			// Max retries reached for retryable error
			metrics.AddFailedBatch()
			return fmt.Errorf("API request failed with status %d after %d retries", resp.StatusCode, maxRetries)
		}

		// Non-retryable error (4xx other than 429)
		metrics.AddFailedBatch()
		return fmt.Errorf("API request failed with non-retryable status %d", resp.StatusCode)
	}

	metrics.AddFailedBatch()
	return fmt.Errorf("max retries exceeded due to network errors")
}

// Process a single vault
func processVault(config *Config, vaultConfig VaultConfig, dataSource DataSource) *Metrics {
	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
	fmt.Printf("PROCESSING %s DATA\n", vaultConfig.Name)
	fmt.Printf("%s\n", strings.Repeat("=", 80))

	metrics := &Metrics{
		VaultName: vaultConfig.Name,
		StartTime: time.Now(),
	}

	// Read data from source
	readStart := time.Now()
	records, err := dataSource.ReadRecords(vaultConfig, config.MaxRecords)
	if err != nil {
		fmt.Printf("❌ Failed to read data: %v\n", err)
		metrics.EndTime = time.Now()
		return metrics
	}
	metrics.AddTime("csv_read", time.Since(readStart))

	// Display appropriate message based on source
	sourceType := "data source"
	if config.DataSource == "snowflake" {
		sourceType = "Snowflake"
	} else if config.DataSource == "csv" {
		sourceType = "CSV files"
	}
	fmt.Printf("📊 Loaded %d records from %s\n", len(records), sourceType)

	// Split into batches
	var batches [][]Record
	for i := 0; i < len(records); i += config.BatchSize {
		end := i + config.BatchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}

	fmt.Printf("🔥 Processing %d batches with %d concurrent workers\n", len(batches), config.MaxConcurrency)

	// Create shared HTTP client (connection pooling)
	client := createHTTPClient()

	// Process batches concurrently with worker pool
	var wg sync.WaitGroup
	batchChan := make(chan struct {
		num   int
		batch []Record
	}, config.MaxConcurrency*2) // Buffered channel for pipelining

	// Start workers
	for i := 0; i < config.MaxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range batchChan {
				err := sendBatch(client, config, vaultConfig, job.batch, job.num, metrics)
				if err != nil {
					// Log error with batch details
					recordStart := job.num * len(job.batch)
					recordEnd := recordStart + len(job.batch)
					fmt.Printf("  ❌ Batch %d FAILED (records %d-%d): %v\n",
						job.num, recordStart, recordEnd, err)
				}

				for range job.batch {
					metrics.AddRecord()
				}

				// Progress reporting
				totalRecords := atomic.LoadInt64(&metrics.TotalRecords)
				if totalRecords%int64(config.ProgressInterval) == 0 {
					elapsed := time.Since(metrics.StartTime).Seconds()
					rate := float64(totalRecords) / elapsed
					fmt.Printf("  Progress: %d/%d records (%.1f%%) - %.0f records/sec\n",
						totalRecords, len(records),
						float64(totalRecords)/float64(len(records))*100, rate)
				}
			}
		}()
	}

	// Feed batches to workers
	for batchNum, batch := range batches {
		batchChan <- struct {
			num   int
			batch []Record
		}{batchNum, batch}
	}
	close(batchChan)

	wg.Wait()
	metrics.EndTime = time.Now()

	fmt.Printf("✅ %s processing complete: %d records, %d successful batches, %d failed batches\n",
		vaultConfig.Name, atomic.LoadInt64(&metrics.TotalRecords),
		atomic.LoadInt64(&metrics.SuccessfulBatches),
		atomic.LoadInt64(&metrics.FailedBatches))

	return metrics
}

// Display performance summary
func displaySummary(allMetrics []*Metrics, totalStart time.Time) {
	totalElapsed := time.Since(totalStart)

	fmt.Printf("\n%s\n", strings.Repeat("=", 100))
	fmt.Printf("COMPREHENSIVE PERFORMANCE SUMMARY\n")
	fmt.Printf("%s\n", strings.Repeat("=", 100))

	totalRecords := int64(0)
	totalSuccessful := int64(0)
	totalFailed := int64(0)

	for _, m := range allMetrics {
		records := atomic.LoadInt64(&m.TotalRecords)
		successful := atomic.LoadInt64(&m.SuccessfulBatches)
		failed := atomic.LoadInt64(&m.FailedBatches)

		if records > 0 {
			fmt.Printf("\n%s VAULT PERFORMANCE:\n", m.VaultName)
			fmt.Printf("  Records Processed:     %d\n", records)
			fmt.Printf("  Processing Time:       %.2f seconds\n", m.Duration().Seconds())
			fmt.Printf("  Throughput:            %.0f records/sec\n", m.Throughput())
			totalBatches := successful + failed
			if totalBatches > 0 {
				fmt.Printf("  Success Rate:          %d/%d batches (%.1f%%)\n",
					successful, totalBatches,
					float64(successful)/float64(totalBatches)*100)
			}

			// Detailed timing
			csvRead := m.GetDuration("csv_read")
			recordCreation := m.GetDuration("record_creation")
			suffixGen := m.GetDuration("suffix_gen")
			payloadCreation := m.GetDuration("payload_creation")
			jsonSer := m.GetDuration("json_serialization")
			baseDelay := m.GetDuration("base_delay")
			apiCall := m.GetDuration("api_call")
			retryDelay := m.GetDuration("retry_delay")

			cumulative := csvRead + recordCreation + suffixGen + payloadCreation +
				jsonSer + baseDelay + apiCall + retryDelay

			avgConcurrency := cumulative.Seconds() / m.Duration().Seconds()

			fmt.Printf("\n  DETAILED TIMING BREAKDOWN (Cumulative across all parallel workers):\n")
			fmt.Printf("    %-25s %12s %10s %16s\n", "Component", "Cumulative", "% of Total", "Est. Wall Clock")
			fmt.Printf("    %s %s %s %s\n", strings.Repeat("-", 25), strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 16))

			printTiming := func(label string, dur time.Duration, warn bool) {
				pct := 0.0
				if cumulative.Seconds() > 0 {
					pct = dur.Seconds() / cumulative.Seconds() * 100
				}
				estWall := 0.0
				if avgConcurrency > 0 {
					estWall = dur.Seconds() / avgConcurrency
				}
				fmt.Printf("    %-25s %10.2fs %9.1f%% %14.2fs\n", label, dur.Seconds(), pct, estWall)
			}

			printTiming("Data Source Read", csvRead, false)
			printTiming("Record Creation", recordCreation, false)
			printTiming("Suffix Generation", suffixGen, false)
			printTiming("Payload Creation", payloadCreation, false)
			printTiming("JSON Serialization", jsonSer, false)
			printTiming("BASE_REQUEST_DELAY", baseDelay, false)
			printTiming("Skyflow API Calls", apiCall, false)
			printTiming("Retry Delays", retryDelay, false)

			fmt.Printf("    %s %s %s %s\n", strings.Repeat("-", 25), strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 16))
			fmt.Printf("    %-25s %10.2fs %9s %14.2fs (actual)\n", "TOTAL (Cumulative)", cumulative.Seconds(), "100.0%", m.Duration().Seconds())
			fmt.Printf("\n    Average Concurrency: %.1fx (concurrent workers executing simultaneously)\n", avgConcurrency)

			totalRecords += records
			totalSuccessful += successful
			totalFailed += failed
		}
	}

	// Overall summary
	fmt.Printf("\n%s\n", strings.Repeat("=", 50))
	fmt.Printf("OVERALL PERFORMANCE SUMMARY\n")
	fmt.Printf("%s\n", strings.Repeat("=", 50))
	fmt.Printf("Total Records Processed:   %d\n", totalRecords)
	fmt.Printf("Total Elapsed Time (Wall Clock): %.2f seconds\n", totalElapsed.Seconds())

	// Error summary
	totalBatches := totalSuccessful + totalFailed
	if totalFailed == 0 {
		fmt.Printf("\nERROR SUMMARY:\n")
		fmt.Printf("  ✅ No errors encountered\n")
		fmt.Printf("  Total Batches:           %d (all successful)\n", totalBatches)
	} else {
		successRate := float64(totalSuccessful) / float64(totalBatches) * 100
		fmt.Printf("\nERROR SUMMARY:\n")
		fmt.Printf("  Errors detected:\n")
		fmt.Printf("  Total Batches:           %d\n", totalBatches)
		fmt.Printf("  Successful Batches:      %d (%.1f%%)\n", totalSuccessful, successRate)
		fmt.Printf("  Failed Batches:          %d (%.1f%%)\n", totalFailed, 100-successRate)
	}

	fmt.Printf("\nTHROUGHPUT RATES:\n")
	fmt.Printf("  End-to-End Throughput:      %.0f records/sec (actual)\n",
		float64(totalRecords)/totalElapsed.Seconds())

	fmt.Printf("\n🎉 All vaults processed!\n")
}

// Clear vault table - delete all records
func clearVaultTable(client *http.Client, config *Config, vaultConfig VaultConfig) error {
	fmt.Printf("\n🗑️  Clearing %s vault...\n", vaultConfig.Name)

	baseURL := fmt.Sprintf("%s/v1/vaults/%s/%s", config.VaultURL, vaultConfig.ID, vaultConfig.Column)
	totalDeleted := 0
	iteration := 0
	fetchLimit := 100
	batchSize := 100

	for {
		iteration++

		// Fetch records to delete
		fetchURL := fmt.Sprintf("%s?offset=0&limit=%d", baseURL, fetchLimit)
		req, err := http.NewRequest("GET", fetchURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create fetch request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+config.BearerToken)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to fetch records: %w", err)
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 404 {
			// Table is empty
			if iteration == 1 {
				fmt.Printf("  ℹ️  %s vault is already empty\n", vaultConfig.Name)
			}
			break
		}

		if resp.StatusCode != 200 {
			return fmt.Errorf("failed to fetch records: status %d", resp.StatusCode)
		}

		// Parse response to get skyflow_ids
		var fetchResp struct {
			Records []struct {
				Fields struct {
					SkyflowID string `json:"skyflow_id"`
				} `json:"fields"`
			} `json:"records"`
		}

		if err := json.Unmarshal(bodyBytes, &fetchResp); err != nil {
			return fmt.Errorf("failed to parse fetch response: %w", err)
		}

		if len(fetchResp.Records) == 0 {
			// No more records
			break
		}

		// Collect skyflow_ids
		skyflowIDs := make([]string, 0, len(fetchResp.Records))
		for _, record := range fetchResp.Records {
			if record.Fields.SkyflowID != "" {
				skyflowIDs = append(skyflowIDs, record.Fields.SkyflowID)
			}
		}

		if len(skyflowIDs) == 0 {
			break
		}

		// Delete in batches
		for i := 0; i < len(skyflowIDs); i += batchSize {
			end := i + batchSize
			if end > len(skyflowIDs) {
				end = len(skyflowIDs)
			}
			batch := skyflowIDs[i:end]

			deletePayload := map[string]interface{}{
				"skyflow_ids": batch,
			}

			payloadBytes, _ := json.Marshal(deletePayload)
			req, err := http.NewRequest("DELETE", baseURL, bytes.NewReader(payloadBytes))
			if err != nil {
				return fmt.Errorf("failed to create delete request: %w", err)
			}

			req.Header.Set("Authorization", "Bearer "+config.BearerToken)
			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				return fmt.Errorf("failed to delete batch: %w", err)
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			if resp.StatusCode != 200 {
				return fmt.Errorf("failed to delete batch: status %d", resp.StatusCode)
			}

			totalDeleted += len(batch)
		}

		fmt.Printf("  Iteration %d: Deleted %d records (total: %d)\n", iteration, len(skyflowIDs), totalDeleted)
	}

	if totalDeleted > 0 {
		fmt.Printf("  ✅ %s vault cleared: %d records deleted\n", vaultConfig.Name, totalDeleted)
	} else {
		fmt.Printf("  ✅ %s vault confirmed empty\n", vaultConfig.Name)
	}

	return nil
}

// Load configuration from JSON file
func loadConfigFile(filepath string) (*FileConfig, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var config FileConfig
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// promptForPassword prompts the user for a password securely (without echo)
func promptForPassword(prompt string) (string, error) {
	fmt.Print(prompt)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // Print newline after password input
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytePassword)), nil
}

// promptForInput prompts the user for text input
func promptForInput(prompt string) (string, error) {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(input), nil
}

func clearAllVaults(config *Config, vaults []VaultConfig) error {
	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
	fmt.Printf("CLEARING VAULT DATA (TEST USE ONLY)\n")
	fmt.Printf("%s\n", strings.Repeat("=", 80))

	client := createHTTPClient()

	for _, v := range vaults {
		if err := clearVaultTable(client, config, v); err != nil {
			fmt.Printf("  ❌ Failed to clear %s vault: %v\n", v.Name, err)
			return err
		}
	}

	fmt.Printf("\n✅ All vaults cleared successfully!\n")
	return nil
}

// Generate mock data CSV files
func generateMockData(numRecords int, dataFile, tokenFile string) error {
	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
	fmt.Printf("GENERATING MOCK DATA\n")
	fmt.Printf("%s\n", strings.Repeat("=", 80))
	fmt.Printf("Records to generate: %d\n", numRecords)
	fmt.Printf("Data file:  %s\n", dataFile)
	fmt.Printf("Token file: %s\n", tokenFile)
	fmt.Printf("%s\n\n", strings.Repeat("=", 80))

	// Sample names
	firstNames := []string{
		"JAMES", "MARY", "JOHN", "PATRICIA", "ROBERT", "JENNIFER", "MICHAEL", "LINDA",
		"WILLIAM", "BARBARA", "DAVID", "ELIZABETH", "RICHARD", "SUSAN", "JOSEPH", "JESSICA",
		"THOMAS", "SARAH", "CHARLES", "KAREN", "CHRISTOPHER", "NANCY", "DANIEL", "LISA",
		"MATTHEW", "BETTY", "ANTHONY", "MARGARET", "MARK", "SANDRA", "DONALD", "ASHLEY",
	}

	lastNames := []string{
		"SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS",
		"RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON", "THOMAS",
		"TAYLOR", "MOORE", "JACKSON", "MARTIN", "LEE", "PEREZ", "THOMPSON", "WHITE",
		"HARRIS", "SANCHEZ", "CLARK", "RAMIREZ", "LEWIS", "ROBINSON", "WALKER", "YOUNG",
	}

	// Generate timestamp for uniqueness
	timestamp := fmt.Sprintf("%d", time.Now().Unix())

	// Helper to generate random suffix
	randomSuffix := func() string {
		const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
		b := make([]byte, 16)
		for i := range b {
			b[i] = chars[rand.Intn(len(chars))]
		}
		return string(b)
	}

	// Create data file
	dataF, err := os.Create(dataFile)
	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}
	defer dataF.Close()

	// Create token file
	tokenF, err := os.Create(tokenFile)
	if err != nil {
		return fmt.Errorf("failed to create token file: %w", err)
	}
	defer tokenF.Close()

	dataWriter := csv.NewWriter(dataF)
	tokenWriter := csv.NewWriter(tokenF)

	// Write headers
	dataWriter.Write([]string{"full_name", "id", "dob", "ssn"})
	tokenWriter.Write([]string{"full_name_token", "id_token", "dob_token", "ssn_token"})

	// Generate records
	for i := 0; i < numRecords; i++ {
		uniqueSuffix := fmt.Sprintf("%s_%s", timestamp, randomSuffix())

		// Generate unique name
		firstName := firstNames[rand.Intn(len(firstNames))]
		lastName := lastNames[rand.Intn(len(lastNames))]
		fullName := fmt.Sprintf("%s %s %s", firstName, lastName, uniqueSuffix)

		// Generate unique ID
		baseID := rand.Intn(90000) + 10000
		patientID := fmt.Sprintf("%d-%s", baseID, uniqueSuffix)

		// Generate unique DOB
		// Random date between 1940 and 2010
		startYear := 1940
		endYear := 2010
		year := startYear + rand.Intn(endYear-startYear)
		month := rand.Intn(12) + 1
		day := rand.Intn(28) + 1 // Keep it simple, avoid month/day edge cases
		dob := fmt.Sprintf("%04d-%02d-%02d-%s", year, month, day, uniqueSuffix)

		// Generate unique SSN
		area := rand.Intn(899) + 1
		if area == 666 {
			area = 667
		}
		group := rand.Intn(99) + 1
		serial := rand.Intn(9999) + 1
		ssn := fmt.Sprintf("%03d-%02d-%04d-%s", area, group, serial, uniqueSuffix)

		// Write data record
		dataWriter.Write([]string{fullName, patientID, dob, ssn})

		// Write token record
		nameToken := fmt.Sprintf("tok_name_%s_%s", timestamp, randomSuffix())
		idToken := fmt.Sprintf("tok_id_%s_%s", timestamp, randomSuffix())
		dobToken := fmt.Sprintf("tok_dob_%s_%s", timestamp, randomSuffix())
		ssnToken := fmt.Sprintf("tok_ssn_%s_%s", timestamp, randomSuffix())
		tokenWriter.Write([]string{nameToken, idToken, dobToken, ssnToken})

		// Progress reporting
		if (i+1)%10000 == 0 {
			fmt.Printf("  Generated %d/%d records...\n", i+1, numRecords)
		}
	}

	dataWriter.Flush()
	tokenWriter.Flush()

	if err := dataWriter.Error(); err != nil {
		return fmt.Errorf("error writing data file: %w", err)
	}
	if err := tokenWriter.Error(); err != nil {
		return fmt.Errorf("error writing token file: %w", err)
	}

	fmt.Printf("\n✅ Successfully generated %d records!\n", numRecords)
	fmt.Printf("   Data file:  %s\n", dataFile)
	fmt.Printf("   Token file: %s\n", tokenFile)
	fmt.Printf("\nYou can now run:\n")
	fmt.Printf("   ./skyflow-loader -token \"YOUR_TOKEN\"\n\n")

	return nil
}

func main() {
	// Command-line flags
	configFile := flag.String("config", "config.json", "Path to configuration file")
	bearerToken := flag.String("token", "", "Bearer token for authentication (overrides config, optional if set in config.json)")

	// Override flags (optional - override config file values)
	vaultURL := flag.String("vault-url", "", "Skyflow vault URL (overrides config)")
	dataSource := flag.String("source", "", "Data source: csv or snowflake (overrides config)")

	// CSV override flags
	dataFile := flag.String("data-file", "", "Path to data CSV file (overrides config)")
	tokenFile := flag.String("token-file", "", "Path to token CSV file (overrides config)")

	// Snowflake override flags
	sfUser := flag.String("sf-user", "", "Snowflake user (overrides config)")
	sfPassword := flag.String("sf-password", "", "Snowflake password (overrides config)")
	sfAccount := flag.String("sf-account", "", "Snowflake account (overrides config)")
	sfWarehouse := flag.String("sf-warehouse", "", "Snowflake warehouse (overrides config)")
	sfDatabase := flag.String("sf-database", "", "Snowflake database (overrides config)")
	sfSchema := flag.String("sf-schema", "", "Snowflake schema (overrides config)")
	sfRole := flag.String("sf-role", "", "Snowflake role (overrides config)")
	sfFetchSize := flag.Int("sf-fetch-size", 0, "Snowflake fetch size (overrides config)")
	sfQueryMode := flag.String("sf-query-mode", "", "Query mode: simple or union (overrides config)")

	// Performance override flags
	batchSize := flag.Int("batch-size", 0, "Batch size for API calls (overrides config)")
	maxConcurrency := flag.Int("concurrency", 0, "Maximum concurrent requests per vault (overrides config)")
	maxRecords := flag.Int("max-records", -1, "Maximum records to process (overrides config, -1 uses config)")
	appendSuffix := flag.Bool("append-suffix", true, "Append unique suffix to data/tokens")
	baseDelay := flag.Int("base-delay-ms", -1, "Base delay between requests in milliseconds (overrides config, -1 uses config)")

	// Other flags
	vault := flag.String("vault", "", "Process only specific vault (name, id, dob, ssn)")
	clearVaults := flag.Bool("clear", false, "Clear all data from vaults before loading (TEST USE ONLY)")
	generateData := flag.Int("generate", 0, "Generate mock CSV data with N records and exit (e.g., -generate 1000)")

	flag.Parse()

	// Handle generate mode (doesn't require bearer token)
	if *generateData > 0 {
		rand.Seed(time.Now().UnixNano())
		if err := generateMockData(*generateData, *dataFile, *tokenFile); err != nil {
			fmt.Printf("❌ Error generating mock data: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Load configuration file
	fmt.Printf("📋 Loading configuration from: %s\n", *configFile)
	fileConfig, err := loadConfigFile(*configFile)
	if err != nil {
		fmt.Printf("❌ Failed to load config file: %v\n", err)
		fmt.Printf("   Make sure config.json exists in the current directory\n")
		os.Exit(1)
	}

	// Use bearer token from command line, config file, or prompt
	finalBearerToken := *bearerToken
	if finalBearerToken == "" {
		finalBearerToken = fileConfig.Skyflow.BearerToken
	}
	if finalBearerToken == "" {
		// Prompt for bearer token
		token, err := promptForPassword("🔑 Enter Skyflow bearer token: ")
		if err != nil {
			fmt.Printf("❌ Error reading bearer token: %v\n", err)
			os.Exit(1)
		}
		if token == "" {
			fmt.Println("❌ Error: Bearer token is required")
			os.Exit(1)
		}
		finalBearerToken = token
	}

	// Build runtime config with CLI overrides
	// Helper function to override string values
	overrideString := func(cliValue, configValue string) string {
		if cliValue != "" {
			return cliValue
		}
		return configValue
	}

	// Helper function to override int values
	overrideInt := func(cliValue, configValue int, defaultCheck int) int {
		if cliValue != defaultCheck {
			return cliValue
		}
		return configValue
	}

	// Determine data source
	dataSourceValue := "csv"
	if *dataSource != "" {
		dataSourceValue = *dataSource
	}

	// Handle Snowflake credentials prompt
	finalSnowflakeUser := overrideString(*sfUser, fileConfig.Snowflake.User)
	finalSnowflakePassword := overrideString(*sfPassword, fileConfig.Snowflake.Password)

	if dataSourceValue == "snowflake" {
		// Prompt for Snowflake user if missing
		if finalSnowflakeUser == "" {
			user, err := promptForInput("❄️  Enter Snowflake username: ")
			if err != nil {
				fmt.Printf("❌ Error reading Snowflake username: %v\n", err)
				os.Exit(1)
			}
			if user == "" {
				fmt.Println("❌ Error: Snowflake username is required when using Snowflake data source")
				os.Exit(1)
			}
			finalSnowflakeUser = user
		}

		// Prompt for Snowflake password if missing
		if finalSnowflakePassword == "" {
			password, err := promptForPassword("❄️  Enter Snowflake password: ")
			if err != nil {
				fmt.Printf("❌ Error reading Snowflake password: %v\n", err)
				os.Exit(1)
			}
			if password == "" {
				fmt.Println("❌ Error: Snowflake password is required when using Snowflake data source")
				os.Exit(1)
			}
			finalSnowflakePassword = password
		}
	}

	config := &Config{
		VaultURL:         overrideString(*vaultURL, fileConfig.Skyflow.VaultURL),
		BearerToken:      finalBearerToken,
		BatchSize:        overrideInt(*batchSize, fileConfig.Performance.BatchSize, 0),
		MaxConcurrency:   overrideInt(*maxConcurrency, fileConfig.Performance.MaxConcurrency, 0),
		MaxRecords:       overrideInt(*maxRecords, fileConfig.Performance.MaxRecords, -1),
		AppendSuffix:     *appendSuffix,
		DataSource:       dataSourceValue,
		DataFile:         overrideString(*dataFile, fileConfig.CSV.DataFile),
		TokenFile:        overrideString(*tokenFile, fileConfig.CSV.TokenFile),
		ProgressInterval: 1000,
		BaseRequestDelay: time.Duration(overrideInt(*baseDelay, fileConfig.Performance.BaseDelayMs, -1)) * time.Millisecond,
		SnowflakeConfig: SnowflakeConfig{
			User:      finalSnowflakeUser,
			Password:  finalSnowflakePassword,
			Account:   overrideString(*sfAccount, fileConfig.Snowflake.Account),
			Warehouse: overrideString(*sfWarehouse, fileConfig.Snowflake.Warehouse),
			Database:  overrideString(*sfDatabase, fileConfig.Snowflake.Database),
			Schema:    overrideString(*sfSchema, fileConfig.Snowflake.Schema),
			Role:      overrideString(*sfRole, fileConfig.Snowflake.Role),
			FetchSize: overrideInt(*sfFetchSize, fileConfig.Snowflake.FetchSize, 0),
			QueryMode: overrideString(*sfQueryMode, fileConfig.Snowflake.QueryMode),
		},
	}

	// Load vaults from config file
	vaults := fileConfig.Skyflow.Vaults
	if len(vaults) == 0 {
		fmt.Printf("❌ Error: No vaults defined in config file\n")
		os.Exit(1)
	}

	// Filter to specific vault if requested
	if *vault != "" {
		var filtered []VaultConfig
		for _, v := range vaults {
			if strings.EqualFold(v.Name, *vault) {
				filtered = []VaultConfig{v}
				break
			}
		}
		if len(filtered) == 0 {
			fmt.Printf("❌ Error: Unknown vault '%s'\n", *vault)
			os.Exit(1)
		}
		vaults = filtered
		fmt.Printf("🎯 Single-vault mode: Processing %s vault only\n", strings.ToUpper(*vault))
	} else {
		fmt.Printf("🔥 Processing %d vaults sequentially\n", len(vaults))
	}

	rand.Seed(time.Now().UnixNano())

	// Initialize data source
	var ds DataSource

	if config.DataSource == "snowflake" {
		fmt.Printf("❄️  Using Snowflake data source\n")
		fmt.Printf("   User: %s\n", config.SnowflakeConfig.User)
		fmt.Printf("   Account: %s\n", config.SnowflakeConfig.Account)
		fmt.Printf("   Database: %s\n", config.SnowflakeConfig.Database)
		fmt.Printf("   Schema: %s\n", config.SnowflakeConfig.Schema)

		sfSource := &SnowflakeDataSource{
			Config: config.SnowflakeConfig,
		}
		if err := sfSource.Connect(); err != nil {
			fmt.Printf("❌ Failed to connect to Snowflake: %v\n", err)
			os.Exit(1)
		}
		ds = sfSource
		defer ds.Close()
	} else {
		fmt.Printf("📁 Using CSV data source\n")
		fmt.Printf("   Data file: %s\n", config.DataFile)
		fmt.Printf("   Token file: %s\n", config.TokenFile)

		csvSource := &CSVDataSource{
			DataFile:  config.DataFile,
			TokenFile: config.TokenFile,
		}
		if err := csvSource.Connect(); err != nil {
			fmt.Printf("❌ Failed to validate CSV files: %v\n", err)
			os.Exit(1)
		}
		ds = csvSource
		defer ds.Close()
	}

	// Clear vaults if requested
	if *clearVaults {
		if err := clearAllVaults(config, vaults); err != nil {
			fmt.Printf("\n❌ Failed to clear vaults: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("\nProceeding with data load...\n")
	}

	totalStart := time.Now()

	// Process vaults sequentially
	var allMetrics []*Metrics

	for _, v := range vaults {
		metrics := processVault(config, v, ds)
		allMetrics = append(allMetrics, metrics)
	}

	// Display summary
	displaySummary(allMetrics, totalStart)
}
