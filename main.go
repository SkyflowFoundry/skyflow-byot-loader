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
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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
	User          string `json:"user"`
	Password      string `json:"password"`
	Authenticator string `json:"authenticator"`
	Account       string `json:"account"`
	Warehouse     string `json:"warehouse"`
	Database      string `json:"database"`
	Schema        string `json:"schema"`
	Role          string `json:"role"`
	FetchSize     int    `json:"fetch_size"`
	QueryMode     string `json:"query_mode"`
}

type CSVConfig struct {
	DataDirectory string `json:"data_directory"`
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
	DataDirectory      string
	ProgressInterval   int
	BaseRequestDelay   time.Duration
	SnowflakeConfig    SnowflakeConfig
}

// Snowflake configuration
type SnowflakeConfig struct {
	User          string
	Password      string
	Authenticator string // "snowflake" (default), "programmatic_access_token", "SNOWFLAKE_JWT", etc.
	Account       string
	Warehouse     string
	Database      string
	Schema        string
	Role          string
	FetchSize     int
	QueryMode     string // "simple" or "union"
	StartRecord   int    // Starting record offset (0-based)
	EndRecord     int    // Ending record (exclusive, 0 = no limit)
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

// BatchError captures details about a failed batch for error logging
type BatchError struct {
	BatchNumber int       `json:"batch_number"`
	Records     []Record  `json:"records"`
	Error       string    `json:"error"`
	StatusCode  int       `json:"status_code,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

// Performance metrics with atomic operations for thread safety
type Metrics struct {
	VaultName             string
	TotalRecords          int64
	SuccessfulBatches     int64
	FailedBatches         int64
	RateLimited429        int64 // Total 429 responses received (including during retries)
	RetriedSuccesses      int64 // Batches that succeeded after retry
	ImmediateSuccesses    int64 // Batches that succeeded on first attempt
	ServerErrors5xx       int64 // Count of 5xx server errors
	ActiveWorkers         int64 // Currently executing workers
	ActiveRequests        int64 // HTTP requests in flight
	TotalRequests         int64 // Total HTTP requests made
	TotalAPILatency       int64 // Cumulative API response time in nanoseconds
	MinAPILatency         int64 // Minimum API response time in nanoseconds
	MaxAPILatency         int64 // Maximum API response time in nanoseconds
	SnowflakeFetchTime    int64 // nanoseconds
	RecordCreationTime    int64
	SuffixGenTime         int64
	PayloadCreationTime   int64
	JSONSerializationTime int64
	BaseDelayTime         int64
	APICallTime           int64
	RetryDelayTime        int64
	StartTime             time.Time
	EndTime               time.Time
	BatchErrors           []BatchError // Thread-safe: only append, protected by mutex
	BatchErrorsMutex      sync.Mutex
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
// maxConns: maximum connections to allow (0 = use default of 100)
func createHTTPClient(maxConns int) *http.Client {
	if maxConns <= 0 {
		maxConns = 100
	}

	transport := &http.Transport{
		MaxIdleConns:        maxConns,
		MaxIdleConnsPerHost: maxConns,
		MaxConnsPerHost:     maxConns,
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

// Pool of random number generators to avoid global lock contention
var randPool = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	},
}

func generateUniqueSuffix() string {
	// Get per-goroutine random source from pool (avoids global lock)
	rng := randPool.Get().(*rand.Rand)
	defer randPool.Put(rng)

	suffix := make([]byte, 16)
	for i := range suffix {
		suffix[i] = suffixChars[rng.Intn(len(suffixChars))]
	}
	// Avoid fmt.Sprintf - use string concatenation with strconv
	timestamp := time.Now().Unix()
	return strconv.FormatInt(timestamp, 10) + "_" + string(suffix)
}

// CSVDataSource implements DataSource interface for local CSV files
type CSVDataSource struct {
	DataDirectory string
}

// Connect validates CSV data directory exists
func (c *CSVDataSource) Connect() error {
	if _, err := os.Stat(c.DataDirectory); os.IsNotExist(err) {
		return fmt.Errorf("data directory not found: %s", c.DataDirectory)
	}
	return nil
}

// Close is a no-op for CSV files
func (c *CSVDataSource) Close() error {
	return nil
}

// ReadRecords reads records from CSV files
func (c *CSVDataSource) ReadRecords(vaultConfig VaultConfig, maxRecords int) ([]Record, error) {
	// Construct file paths based on vault type
	dataFilePath := fmt.Sprintf("%s/%s_data.csv", c.DataDirectory, vaultConfig.Column)
	tokenFilePath := fmt.Sprintf("%s/%s_tokens.csv", c.DataDirectory, vaultConfig.Column)

	// Open data file
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataFilePath, err)
	}
	defer dataFile.Close()

	// Open token file
	tokenFile, err := os.Open(tokenFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open token file %s: %w", tokenFilePath, err)
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
	// Build DSN (Data Source Name) with URL encoding for special characters
	// Format: user:password@account/database/schema?warehouse=wh&role=role&authenticator=type
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
		url.QueryEscape(s.Config.User),
		url.QueryEscape(s.Config.Password),
		s.Config.Account,
		s.Config.Database,
		s.Config.Schema,
		url.QueryEscape(s.Config.Warehouse),
		url.QueryEscape(s.Config.Role),
	)

	// Add authenticator if specified (e.g., "programmatic_access_token")
	if s.Config.Authenticator != "" {
		dsn += fmt.Sprintf("&authenticator=%s", url.QueryEscape(s.Config.Authenticator))
	}

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
		query = `SELECT NAME, T01_PROTEGRITY.SCRTY_ACS_CNTRL.SKFL_MBR_NAME_DETOK(NAME) AS name_token
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
		query = `SELECT ID, T01_PROTEGRITY.SCRTY_ACS_CNTRL.SKFL_MBR_IDENTIFIERS_DETOK(ID) AS id_token
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
		query = `SELECT BRTH_DT, T01_PROTEGRITY.SCRTY_ACS_CNTRL.SKFL_BIRTHDATE_DETOK(BRTH_DT) AS dob_token
FROM (
	SELECT SRC_MBR_BRTH_DT AS BRTH_DT FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM
	UNION
	SELECT BRTH_DT FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR
) DT
GROUP BY 1, 2`
	case "ssn":
		// SSN from both CLM and MBR tables
		query = `SELECT SSN, T01_PROTEGRITY.SCRTY_ACS_CNTRL.SKFL_SSN_DETOK(SSN) AS ssn_token
FROM (
	SELECT SRC_MBR_SSN AS SSN FROM D01_SKYFLOW_POC.SKYFLOW_POC.CLM GROUP BY 1
	UNION
	SELECT SSN FROM D01_SKYFLOW_POC.SKYFLOW_POC.MBR GROUP BY 1
) DT
GROUP BY 1, 2`
	}
	return query
}

// ReadRecords reads records from Snowflake using cursor-based fetching
// The query executes ONCE on Snowflake, then rows are fetched incrementally via cursor
// The Snowflake driver automatically batches network fetches based on fetch_size
func (s *SnowflakeDataSource) ReadRecords(vaultConfig VaultConfig, maxRecords int) ([]Record, error) {
	if s.DB == nil {
		return nil, fmt.Errorf("no active Snowflake connection")
	}

	// Choose query based on mode
	var query string
	if s.Config.QueryMode == "union" {
		// D01_SKYFLOW_POC mode: Complex UNION queries with detokenization UDFs
		query = s.buildUnionQuery(vaultConfig)
	} else {
		// Simple mode (default): ELEVANCE.PUBLIC.PATIENTS table
		query = s.buildSimpleQuery(vaultConfig)
	}

	// Add LIMIT and OFFSET for manual chunking
	// Priority: start-record/end-record flags > max-records flag
	// NOTE: Snowflake syntax requires LIMIT before OFFSET
	if s.Config.StartRecord > 0 || s.Config.EndRecord > 0 {
		// Manual chunking mode
		if s.Config.EndRecord > 0 {
			limit := s.Config.EndRecord - s.Config.StartRecord
			query += fmt.Sprintf(" LIMIT %d", limit)
		} else if maxRecords > 0 {
			query += fmt.Sprintf(" LIMIT %d", maxRecords)
		}
		if s.Config.StartRecord > 0 {
			query += fmt.Sprintf(" OFFSET %d", s.Config.StartRecord)
		}
	} else if maxRecords > 0 {
		// Simple max records mode
		query += fmt.Sprintf(" LIMIT %d", maxRecords)
	}

	// Configure fetch size for this session if specified
	// This controls how many rows are fetched from Snowflake to the client in each network round trip
	if s.Config.FetchSize > 0 {
		if s.Config.StartRecord > 0 || s.Config.EndRecord > 0 {
			fmt.Printf("📊 Querying Snowflake for %s data (records %d-%d, network fetch size %d)...\n",
				vaultConfig.Name, s.Config.StartRecord, s.Config.EndRecord, s.Config.FetchSize)
		} else {
			fmt.Printf("📊 Querying Snowflake for %s data (cursor mode: network fetch size %d)...\n",
				vaultConfig.Name, s.Config.FetchSize)
		}
		// Set session parameter for fetch size
		_, err := s.DB.Exec(fmt.Sprintf("ALTER SESSION SET ROWS_PER_RESULTSET = %d", s.Config.FetchSize))
		if err != nil {
			fmt.Printf("⚠️  Warning: Could not set fetch size: %v (continuing anyway)\n", err)
		}
	} else {
		if s.Config.StartRecord > 0 || s.Config.EndRecord > 0 {
			fmt.Printf("📊 Querying Snowflake for %s data (records %d-%d)...\n",
				vaultConfig.Name, s.Config.StartRecord, s.Config.EndRecord)
		} else {
			fmt.Printf("📊 Querying Snowflake for %s data...\n", vaultConfig.Name)
		}
	}

	// Execute query ONCE - this creates a cursor on Snowflake
	fmt.Printf("  🔍 Executing SQL query on Snowflake...\n")
	queryStart := time.Now()
	rows, err := s.DB.Query(query)
	queryDuration := time.Since(queryStart)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()
	fmt.Printf("  ✅ Query executed successfully (%.2f seconds)\n", queryDuration.Seconds())

	// Pre-allocate slice
	capacity := maxRecords
	if capacity <= 0 {
		capacity = 10000
	}
	records := make([]Record, 0, capacity)

	// Stream results via cursor - the driver fetches in batches behind the scenes
	fmt.Printf("  📥 Starting to fetch rows from result set...\n")
	recordCount := 0
	lastLog := time.Now()
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

			// Progress update - more frequent for smaller datasets
			progressInterval := 100000
			if maxRecords > 0 && maxRecords < 100000 {
				progressInterval = maxRecords / 10 // Log every 10%
				if progressInterval < 100 {
					progressInterval = 100 // At least every 100 records
				}
			}

			// Also log every 5 seconds regardless of record count
			if recordCount%progressInterval == 0 || time.Since(lastLog) > 5*time.Second {
				fmt.Printf("  📥 Fetched %d records from Snowflake so far...\n", recordCount)
				lastLog = time.Now()
			}
		}

		if maxRecords > 0 && recordCount >= maxRecords {
			break
		}
	}
	fmt.Printf("  ✅ Finished fetching rows from Snowflake\n")

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

	// Pre-allocate string builders for suffix concatenation (if enabled)
	var valueBuilder, tokenBuilder strings.Builder

	for _, record := range records {
		value := record.Value
		token := record.Token

		if config.AppendSuffix {
			suffixStart := time.Now()
			dataSuffix := generateUniqueSuffix()
			tokenSuffix := generateUniqueSuffix()

			// Use strings.Builder to avoid multiple string allocations
			valueBuilder.Reset()
			valueBuilder.Grow(len(record.Value) + 1 + len(dataSuffix))
			valueBuilder.WriteString(record.Value)
			valueBuilder.WriteByte('_')
			valueBuilder.WriteString(dataSuffix)
			value = valueBuilder.String()

			tokenBuilder.Reset()
			tokenBuilder.Grow(len(record.Token) + 1 + len(tokenSuffix))
			tokenBuilder.WriteString(record.Token)
			tokenBuilder.WriteByte('_')
			tokenBuilder.WriteString(tokenSuffix)
			token = tokenBuilder.String()

			metrics.AddTime("suffix_gen", time.Since(suffixStart))
		}

		recordsJSON = append(recordsJSON, map[string]interface{}{
			"fields": map[string]string{
				vaultConfig.Column: value,
			},
			"tokens": map[string]string{
				vaultConfig.Column: token,
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
func sendBatch(client *http.Client, config *Config, vaultConfig VaultConfig, apiURL string, batch []Record, batchNum int, metrics *Metrics) error {
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

	// Retry logic with exponential backoff
	maxRetries := 5
	hadRetry := false
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequest("POST", apiURL, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+config.BearerToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "gzip")

		// Track active requests
		atomic.AddInt64(&metrics.ActiveRequests, 1)
		atomic.AddInt64(&metrics.TotalRequests, 1)

		apiStart := time.Now()
		resp, err := client.Do(req)
		apiDuration := time.Since(apiStart)

		atomic.AddInt64(&metrics.ActiveRequests, -1)
		metrics.AddTime("api_call", apiDuration)

		// Track API latency for live metrics
		latencyNanos := apiDuration.Nanoseconds()
		atomic.AddInt64(&metrics.TotalAPILatency, latencyNanos)

		// Update min latency (atomic compare-and-swap loop)
		for {
			oldMin := atomic.LoadInt64(&metrics.MinAPILatency)
			if oldMin != 0 && oldMin <= latencyNanos {
				break // Current min is smaller, no update needed
			}
			if atomic.CompareAndSwapInt64(&metrics.MinAPILatency, oldMin, latencyNanos) {
				break
			}
		}

		// Update max latency (atomic compare-and-swap loop)
		for {
			oldMax := atomic.LoadInt64(&metrics.MaxAPILatency)
			if oldMax >= latencyNanos {
				break // Current max is larger, no update needed
			}
			if atomic.CompareAndSwapInt64(&metrics.MaxAPILatency, oldMax, latencyNanos) {
				break
			}
		}

		if err != nil {
			if attempt < maxRetries-1 {
				hadRetry = true
				retryStart := time.Now()
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				time.Sleep(backoff)
				metrics.AddTime("retry_delay", time.Since(retryStart))
				continue
			}
			metrics.AddFailedBatch()
			return fmt.Errorf("API request failed after retries: %w", err)
		}

		// Read body for error diagnostics
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 || resp.StatusCode == 201 {
			// Track whether this was immediate success or after retry
			if hadRetry {
				atomic.AddInt64(&metrics.RetriedSuccesses, 1)
			} else {
				atomic.AddInt64(&metrics.ImmediateSuccesses, 1)
			}
			metrics.AddSuccessfulBatch()
			return nil
		}

		// Log non-success responses for diagnostics
		if resp.StatusCode == 429 {
			atomic.AddInt64(&metrics.RateLimited429, 1)
			hadRetry = true
			fmt.Printf("  ⚠️  Batch %d: Rate limited (429), retrying in %d seconds (attempt %d/%d)\n",
				batchNum, 2<<uint(attempt), attempt+1, maxRetries)
		} else if resp.StatusCode >= 500 {
			atomic.AddInt64(&metrics.ServerErrors5xx, 1)
			hadRetry = true
			fmt.Printf("  ⚠️  Batch %d: Server error (%d), retrying in %d seconds (attempt %d/%d)\n",
				batchNum, resp.StatusCode, 2<<uint(attempt), attempt+1, maxRetries)
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
			err := fmt.Errorf("API request failed with status %d after %d retries (body: %s)",
				resp.StatusCode, maxRetries, string(bodyBytes))
			// Log batch error for later review
			metrics.BatchErrorsMutex.Lock()
			metrics.BatchErrors = append(metrics.BatchErrors, BatchError{
				BatchNumber: batchNum,
				Records:     batch,
				Error:       err.Error(),
				StatusCode:  resp.StatusCode,
				Timestamp:   time.Now(),
			})
			metrics.BatchErrorsMutex.Unlock()
			return err
		}

		// Non-retryable error (4xx other than 429)
		metrics.AddFailedBatch()
		fmt.Printf("  ❌ Batch %d: Non-retryable error %d (body: %s)\n",
			batchNum, resp.StatusCode, string(bodyBytes))
		err = fmt.Errorf("API request failed with non-retryable status %d", resp.StatusCode)
		// Log batch error for later review
		metrics.BatchErrorsMutex.Lock()
		metrics.BatchErrors = append(metrics.BatchErrors, BatchError{
			BatchNumber: batchNum,
			Records:     batch,
			Error:       err.Error(),
			StatusCode:  resp.StatusCode,
			Timestamp:   time.Now(),
		})
		metrics.BatchErrorsMutex.Unlock()
		return err
	}

	metrics.AddFailedBatch()
	networkErr := fmt.Errorf("max retries exceeded due to network errors")
	// Log batch error for later review
	metrics.BatchErrorsMutex.Lock()
	metrics.BatchErrors = append(metrics.BatchErrors, BatchError{
		BatchNumber: batchNum,
		Records:     batch,
		Error:       networkErr.Error(),
		StatusCode:  0,
		Timestamp:   time.Now(),
	})
	metrics.BatchErrorsMutex.Unlock()
	return networkErr
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

	// Calculate dynamic progress interval (report every 1%, but keep reasonable bounds)
	// Minimum: 10,000 records, Maximum: 1,000,000 records
	progressInterval := len(records) / 100 // 1% of total
	if progressInterval < 10000 {
		progressInterval = 10000
	}
	if progressInterval > 1000000 {
		progressInterval = 1000000
	}
	config.ProgressInterval = progressInterval // Update config with calculated interval
	fmt.Printf("📈 Progress updates every %d records (~%.1f%%)\n", progressInterval, float64(progressInterval)/float64(len(records))*100)

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

	// Create shared HTTP client (connection pooling scaled to worker count)
	client := createHTTPClient(config.MaxConcurrency)

	// Pre-construct API URL (avoid repeated string formatting in hot path)
	apiURL := fmt.Sprintf("%s/v1/vaults/%s/%s", config.VaultURL, vaultConfig.ID, vaultConfig.Column)

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
				// Track active worker
				atomic.AddInt64(&metrics.ActiveWorkers, 1)

				err := sendBatch(client, config, vaultConfig, apiURL, job.batch, job.num, metrics)

				atomic.AddInt64(&metrics.ActiveWorkers, -1)

				if err != nil {
					// Log error with batch details
					recordStart := job.num * len(job.batch)
					recordEnd := recordStart + len(job.batch)
					fmt.Printf("  ❌ Batch %d FAILED (records %d-%d): %v\n",
						job.num, recordStart, recordEnd, err)
				} else {
					// Only count records on successful batch
					for range job.batch {
						metrics.AddRecord()
					}
				}

				// Progress reporting with HTTP status breakdown
				totalRecords := atomic.LoadInt64(&metrics.TotalRecords)
				successBatches := atomic.LoadInt64(&metrics.SuccessfulBatches)
				failedBatches := atomic.LoadInt64(&metrics.FailedBatches)
				rateLimited := atomic.LoadInt64(&metrics.RateLimited429)
				immediate := atomic.LoadInt64(&metrics.ImmediateSuccesses)
				retried := atomic.LoadInt64(&metrics.RetriedSuccesses)

				// Report every N successful records
				if totalRecords > 0 && totalRecords%int64(config.ProgressInterval) == 0 {
					elapsed := time.Since(metrics.StartTime).Seconds()
					rate := float64(totalRecords) / elapsed
					totalBatches := successBatches + failedBatches
					successRate := float64(successBatches) / float64(totalBatches) * 100

					fmt.Printf("  Progress: %d/%d records (%.1f%%) - %.0f records/sec | Batches: %d✅ (%d immediate, %d retried) %d❌ (%.0f%% success) | 429s: %d\n",
						totalRecords, len(records),
						float64(totalRecords)/float64(len(records))*100, rate,
						successBatches, immediate, retried, failedBatches, successRate, rateLimited)
				}
			}
		}()
	}

	// Start real-time metrics reporter
	stopMetrics := make(chan struct{})
	var metricsWg sync.WaitGroup
	metricsWg.Add(1)
	go func() {
		defer metricsWg.Done()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		lastRequests := int64(0)
		lastTime := time.Now()

		for {
			select {
			case <-stopMetrics:
				return
			case <-ticker.C:
				now := time.Now()
				currentRequests := atomic.LoadInt64(&metrics.TotalRequests)
				deltaRequests := currentRequests - lastRequests
				deltaTime := now.Sub(lastTime).Seconds()
				requestRate := float64(deltaRequests) / deltaTime

				activeWorkers := atomic.LoadInt64(&metrics.ActiveWorkers)
				activeRequests := atomic.LoadInt64(&metrics.ActiveRequests)
				rateLimited := atomic.LoadInt64(&metrics.RateLimited429)
				totalRecords := atomic.LoadInt64(&metrics.TotalRecords)

				elapsed := now.Sub(metrics.StartTime).Seconds()
				recordRate := float64(totalRecords) / elapsed

				// Calculate average API latency
				totalLatencyNanos := atomic.LoadInt64(&metrics.TotalAPILatency)
				avgLatencyMs := 0.0
				if currentRequests > 0 {
					avgLatencyMs = float64(totalLatencyNanos) / float64(currentRequests) / 1_000_000
				}

				// Get min/max latency
				minLatencyNanos := atomic.LoadInt64(&metrics.MinAPILatency)
				maxLatencyNanos := atomic.LoadInt64(&metrics.MaxAPILatency)
				minLatencyMs := float64(minLatencyNanos) / 1_000_000
				maxLatencyMs := float64(maxLatencyNanos) / 1_000_000

				fmt.Printf("  [LIVE] Workers: %d/%d | HTTP: %d in-flight | Req: %.0f/s | Rec: %.0f/s | Latency: avg=%.0fms min=%.0fms max=%.0fms | 429s: %d\n",
					activeWorkers, config.MaxConcurrency,
					activeRequests,
					requestRate,
					recordRate,
					avgLatencyMs,
					minLatencyMs,
					maxLatencyMs,
					rateLimited)

				lastRequests = currentRequests
				lastTime = now
			}
		}
	}()

	// Feed batches to workers
	for batchNum, batch := range batches {
		batchChan <- struct {
			num   int
			batch []Record
		}{batchNum, batch}
	}
	close(batchChan)

	wg.Wait()

	// Stop metrics reporter
	close(stopMetrics)
	metricsWg.Wait()

	metrics.EndTime = time.Now()

	totalRecords := atomic.LoadInt64(&metrics.TotalRecords)
	successBatches := atomic.LoadInt64(&metrics.SuccessfulBatches)
	failedBatches := atomic.LoadInt64(&metrics.FailedBatches)
	totalBatches := successBatches + failedBatches

	fmt.Printf("✅ %s processing complete: %d records uploaded | %d/%d batches successful (%.1f%%)\n",
		vaultConfig.Name, totalRecords,
		successBatches, totalBatches,
		float64(successBatches)/float64(totalBatches)*100)

	// Write error log if there were failures
	if len(metrics.BatchErrors) > 0 {
		if err := writeErrorLog(vaultConfig, metrics); err != nil {
			fmt.Printf("  ⚠️  Failed to write error log: %v\n", err)
		}
	}

	return metrics
}

// writeErrorLog creates a JSON file with details of failed batches
func writeErrorLog(vaultConfig VaultConfig, metrics *Metrics) error {
	if len(metrics.BatchErrors) == 0 {
		return nil
	}

	// Create filename with timestamp
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("error_log_%s_%s.json", vaultConfig.Name, timestamp)

	// Create error log structure
	errorLog := struct {
		VaultName     string       `json:"vault_name"`
		VaultID       string       `json:"vault_id"`
		Column        string       `json:"column"`
		Timestamp     time.Time    `json:"timestamp"`
		TotalErrors   int          `json:"total_errors"`
		FailedRecords int          `json:"failed_records"`
		Errors        []BatchError `json:"errors"`
	}{
		VaultName:     vaultConfig.Name,
		VaultID:       vaultConfig.ID,
		Column:        vaultConfig.Column,
		Timestamp:     time.Now(),
		TotalErrors:   len(metrics.BatchErrors),
		FailedRecords: 0,
		Errors:        metrics.BatchErrors,
	}

	// Count total failed records
	for _, batchErr := range metrics.BatchErrors {
		errorLog.FailedRecords += len(batchErr.Records)
	}

	// Write to file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create error log file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(errorLog); err != nil {
		return fmt.Errorf("failed to write error log: %w", err)
	}

	fmt.Printf("  📋 Error log written to: %s (%d batches, %d records)\n",
		filename, errorLog.TotalErrors, errorLog.FailedRecords)

	return nil
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

		if records > 0 || successful > 0 || failed > 0 {
			fmt.Printf("\n%s VAULT PERFORMANCE:\n", m.VaultName)
			fmt.Printf("  Records Uploaded:      %d (successfully processed)\n", records)
			fmt.Printf("  Processing Time:       %.2f seconds\n", m.Duration().Seconds())
			fmt.Printf("  Throughput:            %.0f records/sec (successful only)\n", m.Throughput())
			totalBatches := successful + failed
			if totalBatches > 0 {
				fmt.Printf("  Batch Success Rate:    %d/%d batches (%.1f%%)\n",
					successful, totalBatches,
					float64(successful)/float64(totalBatches)*100)
			}

			// API response summary
			rateLimited := atomic.LoadInt64(&m.RateLimited429)
			serverErrors := atomic.LoadInt64(&m.ServerErrors5xx)
			immediate := atomic.LoadInt64(&m.ImmediateSuccesses)
			retried := atomic.LoadInt64(&m.RetriedSuccesses)

			if rateLimited > 0 || serverErrors > 0 || retried > 0 {
				fmt.Printf("\n  API RESPONSE SUMMARY:\n")
				if successful > 0 {
					immediateRate := float64(immediate) / float64(successful) * 100
					retriedRate := float64(retried) / float64(successful) * 100
					fmt.Printf("    ✅ Immediate Successes: %d (%.1f%% of batches)\n", immediate, immediateRate)
					fmt.Printf("    🔄 Retried Successes:   %d (%.1f%% of batches)\n", retried, retriedRate)
				}
				if rateLimited > 0 {
					fmt.Printf("    ⚠️  Rate Limited (429):  %d responses during execution\n", rateLimited)
				}
				if serverErrors > 0 {
					fmt.Printf("    ⚠️  Server Errors (5xx): %d responses during execution\n", serverErrors)
				}
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

	// Error log summary
	hasErrors := false
	for _, m := range allMetrics {
		if len(m.BatchErrors) > 0 {
			hasErrors = true
			break
		}
	}

	if hasErrors {
		fmt.Printf("\n📋 ERROR LOGS CREATED:\n")
		for _, m := range allMetrics {
			if len(m.BatchErrors) > 0 {
				totalFailedRecords := 0
				for _, batchErr := range m.BatchErrors {
					totalFailedRecords += len(batchErr.Records)
				}
				fmt.Printf("  • %s: error_log_%s_*.json (%d batches, %d records)\n",
					m.VaultName, m.VaultName, len(m.BatchErrors), totalFailedRecords)
			}
		}
		fmt.Printf("\n  ⚠️  Review error logs and re-run failed records if needed\n")
	}

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

	client := createHTTPClient(0)

	for _, v := range vaults {
		if err := clearVaultTable(client, config, v); err != nil {
			fmt.Printf("  ❌ Failed to clear %s vault: %v\n", v.Name, err)
			return err
		}
	}

	fmt.Printf("\n✅ All vaults cleared successfully!\n")
	return nil
}

// setupOfflineMode creates a log file and redirects stdout/stderr to it
func setupOfflineMode(logFilename string) (*os.File, error) {
	// Create log file
	logFile, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Redirect stdout and stderr to log file
	os.Stdout = logFile
	os.Stderr = logFile

	// Also set up Go's log package to use the file
	log.SetOutput(logFile)

	return logFile, nil
}

// createPIDFile writes the current process ID to a file
func createPIDFile() error {
	pidFile := "skyflow-loader.pid"
	pid := os.Getpid()

	file, err := os.Create(pidFile)
	if err != nil {
		return fmt.Errorf("failed to create PID file: %w", err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%d\n", pid)
	if err != nil {
		return fmt.Errorf("failed to write PID: %w", err)
	}

	return nil
}

// removePIDFile removes the PID file
func removePIDFile() {
	pidFile := "skyflow-loader.pid"
	os.Remove(pidFile) // Ignore errors
}

// setupSignalHandler sets up handler to ignore SIGHUP (SSH disconnect)
func setupSignalHandler() {
	sigChan := make(chan os.Signal, 1)

	// Ignore SIGHUP (SSH disconnect) - process continues running
	signal.Notify(sigChan, syscall.SIGHUP)

	go func() {
		for sig := range sigChan {
			if sig == syscall.SIGHUP {
				fmt.Println("Received SIGHUP (SSH disconnect) - continuing in background...")
			}
		}
	}()
}

func main() {
	// Command-line flags
	configFile := flag.String("config", "config.json", "Path to configuration file")
	bearerToken := flag.String("token", "", "Bearer token for authentication (overrides config, optional if set in config.json)")

	// Override flags (optional - override config file values)
	vaultURL := flag.String("vault-url", "", "Skyflow vault URL (overrides config)")
	dataSource := flag.String("source", "", "Data source: csv or snowflake (overrides config)")

	// CSV override flags
	dataDirectory := flag.String("data-dir", "", "Path to data directory containing vault CSV files (overrides config)")

	// Snowflake override flags
	sfUser := flag.String("sf-user", "", "Snowflake user (overrides config)")
	sfPassword := flag.String("sf-password", "", "Snowflake password or PAT token (overrides config)")
	sfAuthenticator := flag.String("sf-authenticator", "", "Snowflake authenticator: snowflake, programmatic_access_token, SNOWFLAKE_JWT (overrides config)")
	sfAccount := flag.String("sf-account", "", "Snowflake account (overrides config)")
	sfWarehouse := flag.String("sf-warehouse", "", "Snowflake warehouse (overrides config)")
	sfDatabase := flag.String("sf-database", "", "Snowflake database (overrides config)")
	sfSchema := flag.String("sf-schema", "", "Snowflake schema (overrides config)")
	sfRole := flag.String("sf-role", "", "Snowflake role (overrides config)")
	sfFetchSize := flag.Int("sf-fetch-size", 0, "Snowflake fetch size (overrides config)")
	sfQueryMode := flag.String("sf-query-mode", "", "Query mode: simple or union (overrides config)")
	sfStartRecord := flag.Int("start-record", 0, "Starting record offset (0-based, for manual chunking)")
	sfEndRecord := flag.Int("end-record", 0, "Ending record (exclusive, 0 = no limit, for manual chunking)")

	// Performance override flags
	batchSize := flag.Int("batch-size", 0, "Batch size for API calls (overrides config)")
	maxConcurrency := flag.Int("concurrency", 0, "Maximum concurrent requests per vault (overrides config)")
	maxRecords := flag.Int("max-records", -1, "Maximum records to process (overrides config, -1 uses config)")
	appendSuffix := flag.Bool("append-suffix", true, "Append unique suffix to data/tokens")
	baseDelay := flag.Int("base-delay-ms", -1, "Base delay between requests in milliseconds (overrides config, -1 uses config)")

	// Other flags
	vault := flag.String("vault", "", "Process only specific vault (name, id, dob, ssn)")
	clearVaults := flag.Bool("clear", false, "Clear all data from vaults before loading (TEST USE ONLY)")
	offlineMode := flag.Bool("offline", false, "Run in offline mode: output to log file, survive SSH disconnect")

	flag.Parse()

	// Setup offline mode if requested (must be done before any other output)
	var logFile *os.File
	var logFilename string
	if *offlineMode {
		// Generate log filename before redirecting
		timestamp := time.Now().Format("20060102-150405")
		logFilename = fmt.Sprintf("skyflow-loader-%s.log", timestamp)

		// Print to console before redirecting output
		consoleMsg := fmt.Sprintf(`
╔════════════════════════════════════════════════════════════════╗
║               OFFLINE MODE - BACKGROUND EXECUTION               ║
╚════════════════════════════════════════════════════════════════╝

Starting skyflow-loader in offline mode...
• Process will survive SSH disconnection
• Output redirected to log file: %s
• PID file created: skyflow-loader.pid

MONITORING COMMANDS:
  tail -f %s              # Watch live output
  cat skyflow-loader.pid              # Get process ID
  ps -p $(cat skyflow-loader.pid)     # Check if running
  kill $(cat skyflow-loader.pid)      # Stop process

You can now safely disconnect from SSH. The process will continue running.

`, logFilename, logFilename)
		fmt.Print(consoleMsg)

		// Set up log file and redirect output
		var err error
		logFile, err = setupOfflineMode(logFilename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "❌ Failed to setup offline mode: %v\n", err)
			os.Exit(1)
		}
		defer logFile.Close()

		// Create PID file
		if err := createPIDFile(); err != nil {
			fmt.Printf("⚠️  Warning: Failed to create PID file: %v\n", err)
		} else {
			defer removePIDFile()
		}

		// Set up signal handler to ignore SIGHUP
		setupSignalHandler()

		// All subsequent output now goes to log file
		fmt.Printf("╔════════════════════════════════════════════════════════════════╗\n")
		fmt.Printf("║          SKYFLOW BYOT LOADER - OFFLINE MODE STARTED            ║\n")
		fmt.Printf("╚════════════════════════════════════════════════════════════════╝\n\n")
		fmt.Printf("Log File: %s\n", logFilename)
		fmt.Printf("Process ID: %d\n", os.Getpid())
		fmt.Printf("Started: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))
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
			password, err := promptForPassword("❄️  Enter Snowflake password (or PAT token): ")
			if err != nil {
				fmt.Printf("❌ Error reading Snowflake password: %v\n", err)
				os.Exit(1)
			}
			if password == "" {
				fmt.Println("❌ Error: Snowflake password/token is required when using Snowflake data source")
				os.Exit(1)
			}
			finalSnowflakePassword = password
		}
	}

	// Determine max records with Snowflake-specific default
	finalMaxRecords := overrideInt(*maxRecords, fileConfig.Performance.MaxRecords, -1)
	if dataSourceValue == "snowflake" && *maxRecords == -1 && fileConfig.Performance.MaxRecords == 0 {
		// Default to 100 for Snowflake if not explicitly set
		finalMaxRecords = 100
	}

	config := &Config{
		VaultURL:         overrideString(*vaultURL, fileConfig.Skyflow.VaultURL),
		BearerToken:      finalBearerToken,
		BatchSize:        overrideInt(*batchSize, fileConfig.Performance.BatchSize, 0),
		MaxConcurrency:   overrideInt(*maxConcurrency, fileConfig.Performance.MaxConcurrency, 0),
		MaxRecords:       finalMaxRecords,
		AppendSuffix:     *appendSuffix,
		DataSource:       dataSourceValue,
		DataDirectory:    overrideString(*dataDirectory, fileConfig.CSV.DataDirectory),
		ProgressInterval: 1000,
		BaseRequestDelay: time.Duration(overrideInt(*baseDelay, fileConfig.Performance.BaseDelayMs, -1)) * time.Millisecond,
		SnowflakeConfig: SnowflakeConfig{
			User:          finalSnowflakeUser,
			Password:      finalSnowflakePassword,
			Authenticator: overrideString(*sfAuthenticator, fileConfig.Snowflake.Authenticator),
			Account:       overrideString(*sfAccount, fileConfig.Snowflake.Account),
			Warehouse:     overrideString(*sfWarehouse, fileConfig.Snowflake.Warehouse),
			Database:      overrideString(*sfDatabase, fileConfig.Snowflake.Database),
			Schema:        overrideString(*sfSchema, fileConfig.Snowflake.Schema),
			Role:          overrideString(*sfRole, fileConfig.Snowflake.Role),
			FetchSize:     overrideInt(*sfFetchSize, fileConfig.Snowflake.FetchSize, 0),
			QueryMode:     overrideString(*sfQueryMode, fileConfig.Snowflake.QueryMode),
			StartRecord:   *sfStartRecord,
			EndRecord:     *sfEndRecord,
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
		fmt.Printf("   Data directory: %s\n", config.DataDirectory)

		csvSource := &CSVDataSource{
			DataDirectory: config.DataDirectory,
		}
		if err := csvSource.Connect(); err != nil {
			fmt.Printf("❌ Failed to validate data directory: %v\n", err)
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
