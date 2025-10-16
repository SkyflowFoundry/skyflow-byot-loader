package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/snowflakedb/gosnowflake"
)

// Sample names for generating realistic test data
var firstNames = []string{
	"JAMES", "MARY", "JOHN", "PATRICIA", "ROBERT", "JENNIFER", "MICHAEL", "LINDA",
	"WILLIAM", "BARBARA", "DAVID", "ELIZABETH", "RICHARD", "SUSAN", "JOSEPH", "JESSICA",
	"THOMAS", "SARAH", "CHARLES", "KAREN", "CHRISTOPHER", "NANCY", "DANIEL", "LISA",
	"MATTHEW", "BETTY", "ANTHONY", "MARGARET", "MARK", "SANDRA", "DONALD", "ASHLEY",
	"STEVEN", "KIMBERLY", "PAUL", "EMILY", "ANDREW", "DONNA", "JOSHUA", "MICHELLE",
	"KENNETH", "DOROTHY", "KEVIN", "CAROL", "BRIAN", "AMANDA", "GEORGE", "MELISSA",
	"EDWARD", "DEBORAH", "RONALD", "STEPHANIE", "TIMOTHY", "REBECCA", "JASON", "SHARON",
	"JEFFREY", "LAURA", "RYAN", "CYNTHIA", "JACOB", "KATHLEEN", "GARY", "AMY",
}

var lastNames = []string{
	"SMITH", "JOHNSON", "WILLIAMS", "BROWN", "JONES", "GARCIA", "MILLER", "DAVIS",
	"RODRIGUEZ", "MARTINEZ", "HERNANDEZ", "LOPEZ", "GONZALEZ", "WILSON", "ANDERSON", "THOMAS",
	"TAYLOR", "MOORE", "JACKSON", "MARTIN", "LEE", "PEREZ", "THOMPSON", "WHITE",
	"HARRIS", "SANCHEZ", "CLARK", "RAMIREZ", "LEWIS", "ROBINSON", "WALKER", "YOUNG",
	"ALLEN", "KING", "WRIGHT", "SCOTT", "TORRES", "NGUYEN", "HILL", "FLORES",
	"GREEN", "ADAMS", "NELSON", "BAKER", "HALL", "RIVERA", "CAMPBELL", "MITCHELL",
	"CARTER", "ROBERTS", "GOMEZ", "PHILLIPS", "EVANS", "TURNER", "DIAZ", "PARKER",
	"CRUZ", "EDWARDS", "COLLINS", "REYES", "STEWART", "MORRIS", "MORALES", "MURPHY",
}

// Configuration structs (reuse from main loader)
type FileConfig struct {
	Snowflake SnowflakeFileConfig `json:"snowflake"`
}

type SnowflakeFileConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Account  string `json:"account"`
	Warehouse string `json:"warehouse"`
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Role     string `json:"role"`
}

type SnowflakeConfig struct {
	User      string
	Password  string
	Account   string
	Warehouse string
	Database  string
	Schema    string
	Role      string
}

// VaultRecords represents records for a specific vault
type VaultRecords struct {
	VaultName string
	DataFile  string
	TokenFile string
	Records   []VaultRecord
}

// VaultRecord represents a complete patient record with all vault data
type VaultRecord struct {
	// For CSV mode
	Data  string
	Token string

	// For Snowflake simple mode (unified table)
	ID            int64
	IDToken       string
	FullName      string
	FullNameToken string
	DOB           string
	DOBToken      string
	SSN           string
	SSNToken      string

	// For Snowflake union mode - separate name fields
	FirstName       string
	FirstNameToken  string
	LastName        string
	LastNameToken   string
	MiddleInitial   string
	MiddleInitToken string

	// For Snowflake union mode - multiple ID types
	EnrollmentID      int64
	EnrollmentIDToken string
	HCID              int64
	HCIDToken         string
	SubscriberID      int64
	SubscriberIDToken string

	// For union mode - determine which table (CLM or MBR)
	TableType string // "CLM" or "MBR"
}

// OutputWriter interface for different output destinations
type OutputWriter interface {
	Connect() error
	Initialize(vaultTypes []string) error
	TruncateTables() error
	WriteRecord(record VaultRecord) error
	Flush() error
	Close() error
	GetStats() (recordsWritten int64, tableName string)
}

// generateRandomSuffix generates a random alphanumeric suffix
func generateRandomSuffix(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

// generateToken generates a random token with prefix and timestamp
func generateToken(prefix, timestamp string, length int) string {
	randomString := generateRandomSuffix(length)
	return fmt.Sprintf("%s_%s_%s", prefix, timestamp, randomString)
}

// generateRandomSuffixWithRand generates a random suffix with specific random source
func generateRandomSuffixWithRand(length int, rnd *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[rnd.Intn(len(chars))]
	}
	return string(result)
}

// generateTokenWithRand generates token with specific random source
func generateTokenWithRand(prefix, timestamp string, length int, rnd *rand.Rand) string {
	randomString := generateRandomSuffixWithRand(length, rnd)
	return fmt.Sprintf("%s_%s_%s", prefix, timestamp, randomString)
}

// generateUniqueToken generates a GUARANTEED unique token using SHA256 hashing
// recordID must be globally unique across all workers
func generateUniqueToken(prefix, timestamp string, recordID int64) string {
	// Create deterministic unique string
	input := fmt.Sprintf("%s_%s_%d", prefix, timestamp, recordID)
	hash := sha256.Sum256([]byte(input))
	// Use first 16 bytes of hash (32 hex chars)
	hashStr := hex.EncodeToString(hash[:16])
	return fmt.Sprintf("%s_%s_%s", prefix, timestamp, hashStr)
}

// generateUniqueSuffix generates a GUARANTEED unique suffix using SHA256 hashing
func generateUniqueSuffix(timestamp string, recordID int64) string {
	input := fmt.Sprintf("suffix_%s_%d", timestamp, recordID)
	hash := sha256.Sum256([]byte(input))
	// Use first 8 bytes of hash (16 hex chars) for shorter suffix
	return hex.EncodeToString(hash[:8])
}

// formatNumber formats an integer with comma separators
func formatNumber(n int) string {
	s := strconv.Itoa(n)
	if len(s) <= 3 {
		return s
	}

	// Add commas from right to left
	var result []byte
	for i, digit := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(digit))
	}
	return string(result)
}

// formatBytes formats bytes into human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// loadSnowflakeConfig loads Snowflake config from config.json
func loadSnowflakeConfig(configPath string) (*SnowflakeConfig, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var fileConfig FileConfig
	if err := json.NewDecoder(file).Decode(&fileConfig); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &SnowflakeConfig{
		User:      fileConfig.Snowflake.User,
		Password:  fileConfig.Snowflake.Password,
		Account:   fileConfig.Snowflake.Account,
		Warehouse: fileConfig.Snowflake.Warehouse,
		Database:  fileConfig.Snowflake.Database,
		Schema:    fileConfig.Snowflake.Schema,
		Role:      fileConfig.Snowflake.Role,
	}, nil
}

// ==================== CSV OUTPUT WRITER ====================

type CSVOutputWriter struct {
	dataDir       string
	vaultTypes    []string
	dataWriters   map[string]*csv.Writer
	tokenWriters  map[string]*csv.Writer
	dataFiles     map[string]*os.File
	tokenFiles    map[string]*os.File
	recordsWritten int64
	mu            sync.Mutex
}

func NewCSVOutputWriter(dataDir string) *CSVOutputWriter {
	return &CSVOutputWriter{
		dataDir:      dataDir,
		dataWriters:  make(map[string]*csv.Writer),
		tokenWriters: make(map[string]*csv.Writer),
		dataFiles:    make(map[string]*os.File),
		tokenFiles:   make(map[string]*os.File),
	}
}

func (c *CSVOutputWriter) Connect() error {
	// Ensure data directory exists
	if err := os.MkdirAll(c.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	return nil
}

func (c *CSVOutputWriter) Initialize(vaultTypes []string) error {
	c.vaultTypes = vaultTypes

	for _, vaultType := range vaultTypes {
		dataFile := fmt.Sprintf("%s/%s_data.csv", c.dataDir, vaultType)
		tokenFile := fmt.Sprintf("%s/%s_tokens.csv", c.dataDir, vaultType)

		// Create data file
		dataF, err := os.Create(dataFile)
		if err != nil {
			return fmt.Errorf("error creating %s data file: %w", vaultType, err)
		}
		c.dataFiles[vaultType] = dataF

		// Create token file
		tokenF, err := os.Create(tokenFile)
		if err != nil {
			return fmt.Errorf("error creating %s token file: %w", vaultType, err)
		}
		c.tokenFiles[vaultType] = tokenF

		// Create CSV writers
		dataWriter := csv.NewWriter(dataF)
		tokenWriter := csv.NewWriter(tokenF)
		c.dataWriters[vaultType] = dataWriter
		c.tokenWriters[vaultType] = tokenWriter

		// Write headers
		columnName := getColumnName(vaultType)
		if err := dataWriter.Write([]string{columnName}); err != nil {
			return fmt.Errorf("error writing data header: %w", err)
		}
		if err := tokenWriter.Write([]string{columnName + "_token"}); err != nil {
			return fmt.Errorf("error writing token header: %w", err)
		}
	}

	return nil
}

func (c *CSVOutputWriter) WriteRecord(record VaultRecord) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if record.Data != "" {
		// Single vault mode - use Data/Token fields directly
		// Need to infer vault type from context
		return fmt.Errorf("CSV writer requires vault type to be specified")
	} else {
		// Multi-vault mode - write to all applicable vaults
		if record.FullName != "" {
			dataWriter := c.dataWriters["name"]
			tokenWriter := c.tokenWriters["name"]
			dataWriter.Write([]string{record.FullName})
			tokenWriter.Write([]string{record.FullNameToken})
		}
		if record.ID != 0 {
			dataWriter := c.dataWriters["id"]
			tokenWriter := c.tokenWriters["id"]
			dataWriter.Write([]string{fmt.Sprintf("%d", record.ID)})
			tokenWriter.Write([]string{record.IDToken})
		}
		if record.DOB != "" {
			dataWriter := c.dataWriters["dob"]
			tokenWriter := c.tokenWriters["dob"]
			dataWriter.Write([]string{record.DOB})
			tokenWriter.Write([]string{record.DOBToken})
		}
		if record.SSN != "" {
			dataWriter := c.dataWriters["ssn"]
			tokenWriter := c.tokenWriters["ssn"]
			dataWriter.Write([]string{record.SSN})
			tokenWriter.Write([]string{record.SSNToken})
		}
	}

	atomic.AddInt64(&c.recordsWritten, 1)

	// Periodic flush
	if c.recordsWritten%10000 == 0 {
		c.flushAll()
	}

	return nil
}

func (c *CSVOutputWriter) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.flushAll()
}

func (c *CSVOutputWriter) flushAll() error {
	for _, writer := range c.dataWriters {
		writer.Flush()
	}
	for _, writer := range c.tokenWriters {
		writer.Flush()
	}
	return nil
}

func (c *CSVOutputWriter) Close() error {
	c.flushAll()

	for _, file := range c.dataFiles {
		file.Close()
	}
	for _, file := range c.tokenFiles {
		file.Close()
	}

	return nil
}

func (c *CSVOutputWriter) TruncateTables() error {
	// CSV mode doesn't need truncation - files are recreated on Initialize
	return nil
}

func (c *CSVOutputWriter) GetStats() (int64, string) {
	return atomic.LoadInt64(&c.recordsWritten), c.dataDir
}

// ==================== SNOWFLAKE OUTPUT WRITER ====================

type SnowflakeOutputWriter struct {
	db             *sql.DB
	config         *SnowflakeConfig
	tableName      string
	tableNameMBR   string // For union mode - MBR table
	batchSize      int
	queryMode      string // "simple" or "union"
	recordBuffer   []VaultRecord
	recordBufferMBR []VaultRecord // For union mode - MBR table buffer
	recordsWritten int64
	recordsWrittenMBR int64 // For union mode - MBR table records
	mu             sync.Mutex
}

func NewSnowflakeOutputWriter(config *SnowflakeConfig, tableName string, batchSize int, queryMode string) *SnowflakeOutputWriter {
	writer := &SnowflakeOutputWriter{
		config:    config,
		tableName: tableName,
		batchSize: batchSize,
		queryMode: queryMode,
	}

	// For union mode, set MBR table name
	if queryMode == "union" {
		// CLM table is tableName, MBR is separate
		writer.tableNameMBR = strings.Replace(tableName, "CLM", "MBR", 1)
		if writer.tableNameMBR == tableName {
			// If no CLM in name, append _MBR
			writer.tableNameMBR = tableName + "_MBR"
		}
	}

	return writer
}

func (s *SnowflakeOutputWriter) Connect() error {
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s&role=%s",
		url.QueryEscape(s.config.User),
		url.QueryEscape(s.config.Password),
		s.config.Account,
		s.config.Database,
		s.config.Schema,
		url.QueryEscape(s.config.Warehouse),
		url.QueryEscape(s.config.Role),
	)

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping Snowflake: %w", err)
	}

	s.db = db
	fmt.Println("✅ Connected to Snowflake")
	return nil
}

func (s *SnowflakeOutputWriter) Initialize(vaultTypes []string) error {
	if s.queryMode == "simple" {
		// Create unified PATIENTS table
		createSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s.%s (
				id BIGINT,
				id_token VARCHAR(255),
				full_name VARCHAR(500),
				full_name_token VARCHAR(255),
				dob VARCHAR(255),
				dob_token VARCHAR(255),
				ssn VARCHAR(255),
				ssn_token VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
			)`,
			s.config.Database,
			s.config.Schema,
			s.tableName,
		)

		if _, err := s.db.Exec(createSQL); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		fmt.Printf("✅ Created/verified table: %s.%s.%s\n",
			s.config.Database, s.config.Schema, s.tableName)

	} else if s.queryMode == "union" {
		// Create CLM table
		createCLMSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s.%s (
				SRC_MBR_FRST_NM VARCHAR(500),
				SRC_MBR_FRST_NM_TOKEN VARCHAR(255),
				SRC_MBR_LAST_NM VARCHAR(500),
				SRC_MBR_LAST_NM_TOKEN VARCHAR(255),
				SRC_MBR_MID_INIT_NM VARCHAR(500),
				SRC_MBR_MID_INIT_NM_TOKEN VARCHAR(255),
				SRC_ENRLMNT_ID BIGINT,
				SRC_ENRLMNT_ID_TOKEN VARCHAR(255),
				SRC_HC_ID BIGINT,
				SRC_HC_ID_TOKEN VARCHAR(255),
				SRC_SBSCRBR_ID BIGINT,
				SRC_SBSCRBR_ID_TOKEN VARCHAR(255),
				SRC_MBR_BRTH_DT VARCHAR(255),
				SRC_MBR_BRTH_DT_TOKEN VARCHAR(255),
				SRC_MBR_SSN VARCHAR(255),
				SRC_MBR_SSN_TOKEN VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
			)`,
			s.config.Database,
			s.config.Schema,
			s.tableName,
		)

		if _, err := s.db.Exec(createCLMSQL); err != nil {
			return fmt.Errorf("failed to create CLM table: %w", err)
		}

		fmt.Printf("✅ Created/verified table: %s.%s.%s\n",
			s.config.Database, s.config.Schema, s.tableName)

		// Create MBR table
		createMBRSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s.%s.%s (
				FRST_NM VARCHAR(500),
				FRST_NM_TOKEN VARCHAR(255),
				LAST_NM VARCHAR(500),
				LAST_NM_TOKEN VARCHAR(255),
				MID_INIT_NM VARCHAR(500),
				MID_INIT_NM_TOKEN VARCHAR(255),
				ENRLMNT_ID BIGINT,
				ENRLMNT_ID_TOKEN VARCHAR(255),
				HC_ID BIGINT,
				HC_ID_TOKEN VARCHAR(255),
				SBSCRBR_ID BIGINT,
				SBSCRBR_ID_TOKEN VARCHAR(255),
				BRTH_DT VARCHAR(255),
				BRTH_DT_TOKEN VARCHAR(255),
				SSN VARCHAR(255),
				SSN_TOKEN VARCHAR(255),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
			)`,
			s.config.Database,
			s.config.Schema,
			s.tableNameMBR,
		)

		if _, err := s.db.Exec(createMBRSQL); err != nil {
			return fmt.Errorf("failed to create MBR table: %w", err)
		}

		fmt.Printf("✅ Created/verified table: %s.%s.%s\n",
			s.config.Database, s.config.Schema, s.tableNameMBR)
	}

	return nil
}

func (s *SnowflakeOutputWriter) TruncateTables() error {
	if s.queryMode == "simple" {
		// Truncate PATIENTS table
		truncateSQL := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s.%s",
			s.config.Database,
			s.config.Schema,
			s.tableName)

		if _, err := s.db.Exec(truncateSQL); err != nil {
			return fmt.Errorf("failed to truncate table %s: %w", s.tableName, err)
		}

		fmt.Printf("🗑️  Truncated table: %s.%s.%s\n",
			s.config.Database, s.config.Schema, s.tableName)

	} else if s.queryMode == "union" {
		// Truncate CLM table
		truncateCLMSQL := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s.%s",
			s.config.Database,
			s.config.Schema,
			s.tableName)

		if _, err := s.db.Exec(truncateCLMSQL); err != nil {
			return fmt.Errorf("failed to truncate CLM table: %w", err)
		}

		fmt.Printf("🗑️  Truncated table: %s.%s.%s\n",
			s.config.Database, s.config.Schema, s.tableName)

		// Truncate MBR table
		truncateMBRSQL := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s.%s.%s",
			s.config.Database,
			s.config.Schema,
			s.tableNameMBR)

		if _, err := s.db.Exec(truncateMBRSQL); err != nil {
			return fmt.Errorf("failed to truncate MBR table: %w", err)
		}

		fmt.Printf("🗑️  Truncated table: %s.%s.%s\n",
			s.config.Database, s.config.Schema, s.tableNameMBR)
	}

	return nil
}

func (s *SnowflakeOutputWriter) WriteRecord(record VaultRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queryMode == "union" {
		// Route to appropriate buffer based on table type
		if record.TableType == "MBR" {
			s.recordBufferMBR = append(s.recordBufferMBR, record)
			// Flush MBR when batch size reached
			if len(s.recordBufferMBR) >= s.batchSize {
				return s.flushBatchMBR()
			}
		} else {
			// CLM table
			s.recordBuffer = append(s.recordBuffer, record)
			// Flush CLM when batch size reached
			if len(s.recordBuffer) >= s.batchSize {
				return s.flushBatch()
			}
		}
	} else {
		// Simple mode
		s.recordBuffer = append(s.recordBuffer, record)
		// Flush when batch size reached
		if len(s.recordBuffer) >= s.batchSize {
			return s.flushBatch()
		}
	}

	return nil
}

func (s *SnowflakeOutputWriter) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush both buffers for union mode
	if s.queryMode == "union" {
		if err := s.flushBatch(); err != nil {
			return err
		}
		return s.flushBatchMBR()
	}

	return s.flushBatch()
}

func (s *SnowflakeOutputWriter) flushBatch() error {
	if len(s.recordBuffer) == 0 {
		return nil
	}

	var insertSQL string
	values := []interface{}{}
	placeholders := []string{}

	if s.queryMode == "simple" {
		// Simple mode - PATIENTS table
		insertSQL = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			(id, id_token, full_name, full_name_token, dob, dob_token, ssn, ssn_token)
			VALUES `,
			s.config.Database,
			s.config.Schema,
			s.tableName,
		)

		for _, record := range s.recordBuffer {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?)")
			values = append(values,
				record.ID,
				record.IDToken,
				record.FullName,
				record.FullNameToken,
				record.DOB,
				record.DOBToken,
				record.SSN,
				record.SSNToken,
			)
		}
	} else {
		// Union mode - CLM table
		insertSQL = fmt.Sprintf(`
			INSERT INTO %s.%s.%s
			(SRC_MBR_FRST_NM, SRC_MBR_FRST_NM_TOKEN, SRC_MBR_LAST_NM, SRC_MBR_LAST_NM_TOKEN,
			 SRC_MBR_MID_INIT_NM, SRC_MBR_MID_INIT_NM_TOKEN,
			 SRC_ENRLMNT_ID, SRC_ENRLMNT_ID_TOKEN, SRC_HC_ID, SRC_HC_ID_TOKEN,
			 SRC_SBSCRBR_ID, SRC_SBSCRBR_ID_TOKEN,
			 SRC_MBR_BRTH_DT, SRC_MBR_BRTH_DT_TOKEN, SRC_MBR_SSN, SRC_MBR_SSN_TOKEN)
			VALUES `,
			s.config.Database,
			s.config.Schema,
			s.tableName,
		)

		for _, record := range s.recordBuffer {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
			values = append(values,
				record.FirstName,
				record.FirstNameToken,
				record.LastName,
				record.LastNameToken,
				record.MiddleInitial,
				record.MiddleInitToken,
				record.EnrollmentID,
				record.EnrollmentIDToken,
				record.HCID,
				record.HCIDToken,
				record.SubscriberID,
				record.SubscriberIDToken,
				record.DOB,
				record.DOBToken,
				record.SSN,
				record.SSNToken,
			)
		}
	}

	insertSQL += strings.Join(placeholders, ", ")

	// Execute batch insert
	_, err := s.db.Exec(insertSQL, values...)
	if err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	s.recordsWritten += int64(len(s.recordBuffer))

	// Progress reporting (every 50k records for better visibility)
	if s.recordsWritten%50000 == 0 {
		fmt.Fprintf(os.Stdout, "  📥 Written %s CLM records to Snowflake...\n", formatNumber(int(s.recordsWritten)))
		os.Stdout.Sync() // Force flush
	}

	// Clear buffer
	s.recordBuffer = s.recordBuffer[:0]

	return nil
}

// flushBatchMBR flushes the MBR table buffer (union mode only)
func (s *SnowflakeOutputWriter) flushBatchMBR() error {
	if len(s.recordBufferMBR) == 0 {
		return nil
	}

	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.%s
		(FRST_NM, FRST_NM_TOKEN, LAST_NM, LAST_NM_TOKEN,
		 MID_INIT_NM, MID_INIT_NM_TOKEN,
		 ENRLMNT_ID, ENRLMNT_ID_TOKEN, HC_ID, HC_ID_TOKEN,
		 SBSCRBR_ID, SBSCRBR_ID_TOKEN,
		 BRTH_DT, BRTH_DT_TOKEN, SSN, SSN_TOKEN)
		VALUES `,
		s.config.Database,
		s.config.Schema,
		s.tableNameMBR,
	)

	values := []interface{}{}
	placeholders := []string{}

	for _, record := range s.recordBufferMBR {
		placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		values = append(values,
			record.FirstName,
			record.FirstNameToken,
			record.LastName,
			record.LastNameToken,
			record.MiddleInitial,
			record.MiddleInitToken,
			record.EnrollmentID,
			record.EnrollmentIDToken,
			record.HCID,
			record.HCIDToken,
			record.SubscriberID,
			record.SubscriberIDToken,
			record.DOB,
			record.DOBToken,
			record.SSN,
			record.SSNToken,
		)
	}

	insertSQL += strings.Join(placeholders, ", ")

	// Execute batch insert
	_, err := s.db.Exec(insertSQL, values...)
	if err != nil {
		return fmt.Errorf("MBR batch insert failed: %w", err)
	}

	s.recordsWrittenMBR += int64(len(s.recordBufferMBR))

	// Progress reporting (every 50k records for better visibility)
	if s.recordsWrittenMBR%50000 == 0 {
		fmt.Fprintf(os.Stdout, "  📥 Written %s MBR records to Snowflake...\n", formatNumber(int(s.recordsWrittenMBR)))
		os.Stdout.Sync() // Force flush
	}

	// Clear buffer
	s.recordBufferMBR = s.recordBufferMBR[:0]

	return nil
}

func (s *SnowflakeOutputWriter) Close() error {
	// Flush remaining records
	if err := s.Flush(); err != nil {
		return err
	}

	if s.queryMode == "union" {
		fmt.Printf("✅ Total CLM records written: %s\n", formatNumber(int(s.recordsWritten)))
		fmt.Printf("✅ Total MBR records written: %s\n", formatNumber(int(s.recordsWrittenMBR)))
		fmt.Printf("✅ Total records written: %s\n", formatNumber(int(s.recordsWritten+s.recordsWrittenMBR)))
	} else {
		fmt.Printf("✅ Total records written to Snowflake: %s\n", formatNumber(int(s.recordsWritten)))
	}

	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *SnowflakeOutputWriter) GetStats() (int64, string) {
	fullTableName := fmt.Sprintf("%s.%s.%s", s.config.Database, s.config.Schema, s.tableName)
	if s.queryMode == "union" {
		fullTableName += fmt.Sprintf(" & %s.%s.%s", s.config.Database, s.config.Schema, s.tableNameMBR)
		return atomic.LoadInt64(&s.recordsWritten) + atomic.LoadInt64(&s.recordsWrittenMBR), fullTableName
	}
	return atomic.LoadInt64(&s.recordsWritten), fullTableName
}

// generateVaultData generates data for a specific vault type
func generateVaultData(vaultType string, numRecords int, timestamp string) error {
	if numRecords == 0 {
		fmt.Printf("⏭️  Skipping %s vault (0 records requested)\n", vaultType)
		return nil
	}

	fmt.Printf("\n🔄 Generating %s vault (%s records)...\n", vaultType, formatNumber(numRecords))

	// Define file names
	dataFile := fmt.Sprintf("data/%s_data.csv", vaultType)
	tokenFile := fmt.Sprintf("data/%s_tokens.csv", vaultType)

	// Create data file
	dataF, err := os.Create(dataFile)
	if err != nil {
		return fmt.Errorf("error creating %s data file: %w", vaultType, err)
	}
	defer dataF.Close()

	// Create token file
	tokenF, err := os.Create(tokenFile)
	if err != nil {
		return fmt.Errorf("error creating %s token file: %w", vaultType, err)
	}
	defer tokenF.Close()

	// Create CSV writers
	dataWriter := csv.NewWriter(dataF)
	tokenWriter := csv.NewWriter(tokenF)
	defer dataWriter.Flush()
	defer tokenWriter.Flush()

	// Write headers based on vault type
	columnName := getColumnName(vaultType)
	if err := dataWriter.Write([]string{columnName}); err != nil {
		return fmt.Errorf("error writing data header: %w", err)
	}
	if err := tokenWriter.Write([]string{columnName + "_token"}); err != nil {
		return fmt.Errorf("error writing token header: %w", err)
	}

	// Generate and write records
	return generateVaultRecordsStreaming(vaultType, numRecords, timestamp, dataWriter, tokenWriter)
}

// getColumnName returns the column name for a vault type
func getColumnName(vaultType string) string {
	switch vaultType {
	case "name":
		return "full_name"
	case "id":
		return "id"
	case "dob":
		return "dob"
	case "ssn":
		return "ssn"
	default:
		return vaultType
	}
}

// generateVaultRecordsStreaming generates records in parallel and writes them
func generateVaultRecordsStreaming(vaultType string, numRecords int, timestamp string, dataWriter, tokenWriter *csv.Writer) error {
	// Create channel for generated records
	recordChan := make(chan VaultRecord, 10000)

	// Track progress (atomic for thread safety)
	var recordsGenerated int64 = 0

	// Worker pool
	numWorkers := 8 // Use fixed number for better control
	var wg sync.WaitGroup

	// Distribute work
	recordsPerWorker := numRecords / numWorkers
	remainder := numRecords % numWorkers

	// Start workers with guaranteed unique record ID ranges
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerRecords := recordsPerWorker
		if w == 0 {
			workerRecords += remainder
		}

		// Calculate starting record ID for this worker (guaranteed unique across workers)
		startRecordID := int64(w) * int64(recordsPerWorker)

		go func(workerID int, numRecs int, baseRecordID int64) {
			defer wg.Done()
			// Keep random for data variety (names, dates), but not for uniqueness
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*1000000)))

			for i := 0; i < numRecs; i++ {
				// Calculate globally unique record ID
				recordID := baseRecordID + int64(i)
				record := generateVaultRecordUnique(vaultType, timestamp, recordID, localRand)
				recordChan <- record

				// Progress reporting (atomic)
				generated := atomic.AddInt64(&recordsGenerated, 1)
				if generated%100000 == 0 {
					fmt.Printf("  Generated %s/%s %s records...\n",
						formatNumber(int(generated)),
						formatNumber(numRecords),
						vaultType)
				}
			}
		}(w, workerRecords, startRecordID)
	}

	// Close channel when done
	go func() {
		wg.Wait()
		close(recordChan)
	}()

	// Write records
	recordsWritten := 0
	for record := range recordChan {
		if err := dataWriter.Write([]string{record.Data}); err != nil {
			return fmt.Errorf("error writing data record: %w", err)
		}
		if err := tokenWriter.Write([]string{record.Token}); err != nil {
			return fmt.Errorf("error writing token record: %w", err)
		}

		recordsWritten++
		if recordsWritten%10000 == 0 {
			dataWriter.Flush()
			tokenWriter.Flush()
		}
	}

	// Final flush
	dataWriter.Flush()
	tokenWriter.Flush()

	fmt.Printf("✅ Generated %s records for %s vault\n", formatNumber(numRecords), vaultType)
	return nil
}

// generateVaultRecord generates a single record for a specific vault type (OLD - kept for compatibility)
func generateVaultRecord(vaultType, timestamp string, rnd *rand.Rand) VaultRecord {
	uniqueSuffix := fmt.Sprintf("%s_%s", timestamp, generateRandomSuffixWithRand(16, rnd))

	var data string
	var token string

	switch vaultType {
	case "name":
		firstName := firstNames[rnd.Intn(len(firstNames))]
		lastName := lastNames[rnd.Intn(len(lastNames))]
		data = fmt.Sprintf("%s %s %s", firstName, lastName, uniqueSuffix)
		token = generateTokenWithRand("tok_name", timestamp, 16, rnd)

	case "id":
		baseID := rnd.Intn(90000) + 10000
		data = fmt.Sprintf("%d-%s", baseID, uniqueSuffix)
		token = generateTokenWithRand("tok_id", timestamp, 16, rnd)

	case "dob":
		startYear := 1940
		endYear := 2010
		year := startYear + rnd.Intn(endYear-startYear)
		month := rnd.Intn(12) + 1
		day := rnd.Intn(28) + 1
		data = fmt.Sprintf("%04d-%02d-%02d-%s", year, month, day, uniqueSuffix)
		token = generateTokenWithRand("tok_dob", timestamp, 16, rnd)

	case "ssn":
		area := rnd.Intn(899) + 1
		if area == 666 {
			area = 667
		}
		group := rnd.Intn(99) + 1
		serial := rnd.Intn(9999) + 1
		data = fmt.Sprintf("%03d-%02d-%04d-%s", area, group, serial, uniqueSuffix)
		token = generateTokenWithRand("tok_ssn", timestamp, 16, rnd)

	default:
		data = fmt.Sprintf("unknown_%s", uniqueSuffix)
		token = generateTokenWithRand("tok_unknown", timestamp, 16, rnd)
	}

	return VaultRecord{
		Data:  data,
		Token: token,
	}
}

// generateVaultRecordUnique generates a record with GUARANTEED unique token and data
// Uses SHA256 hashing with recordID to ensure no collisions
func generateVaultRecordUnique(vaultType, timestamp string, recordID int64, rnd *rand.Rand) VaultRecord {
	// Generate GUARANTEED unique suffix using SHA256 hash of recordID
	uniqueSuffix := generateUniqueSuffix(timestamp, recordID)

	var data string
	var token string

	switch vaultType {
	case "name":
		// Use random for variety, but uniqueness comes from suffix
		firstName := firstNames[rnd.Intn(len(firstNames))]
		lastName := lastNames[rnd.Intn(len(lastNames))]
		data = fmt.Sprintf("%s %s %s", firstName, lastName, uniqueSuffix)
		token = generateUniqueToken("tok_name", timestamp, recordID)

	case "id":
		// Use random for variety, but uniqueness comes from suffix
		baseID := rnd.Intn(90000) + 10000
		data = fmt.Sprintf("%d-%s", baseID, uniqueSuffix)
		token = generateUniqueToken("tok_id", timestamp, recordID)

	case "dob":
		// Use random for variety, but uniqueness comes from suffix
		startYear := 1940
		endYear := 2010
		year := startYear + rnd.Intn(endYear-startYear)
		month := rnd.Intn(12) + 1
		day := rnd.Intn(28) + 1
		data = fmt.Sprintf("%04d-%02d-%02d-%s", year, month, day, uniqueSuffix)
		token = generateUniqueToken("tok_dob", timestamp, recordID)

	case "ssn":
		// Use random for variety, but uniqueness comes from suffix
		area := rnd.Intn(899) + 1
		if area == 666 {
			area = 667
		}
		group := rnd.Intn(99) + 1
		serial := rnd.Intn(9999) + 1
		data = fmt.Sprintf("%03d-%02d-%04d-%s", area, group, serial, uniqueSuffix)
		token = generateUniqueToken("tok_ssn", timestamp, recordID)

	default:
		data = fmt.Sprintf("unknown_%s", uniqueSuffix)
		token = generateUniqueToken("tok_unknown", timestamp, recordID)
	}

	return VaultRecord{
		Data:  data,
		Token: token,
	}
}

// generateCompleteRecord generates a complete patient record with ALL vault fields populated
// For Snowflake unified table mode - with GUARANTEED unique data values
// Suffixes each value with its corresponding token to ensure uniqueness
func generateCompleteRecord(timestamp string, recordID int64, rnd *rand.Rand) VaultRecord {
	// Generate tokens first (these are unique and deterministic)
	idToken := generateUniqueToken("tok_id", timestamp, recordID)
	nameToken := generateUniqueToken("tok_name", timestamp, recordID)
	dobToken := generateUniqueToken("tok_dob", timestamp, recordID)
	ssnToken := generateUniqueToken("tok_ssn", timestamp, recordID)

	// Generate name components (random for variety, uniqueness from token suffix)
	firstName := firstNames[rnd.Intn(len(firstNames))]
	lastName := lastNames[rnd.Intn(len(lastNames))]
	fullName := fmt.Sprintf("%s %s %s", firstName, lastName, nameToken)

	// Generate DOB (random for variety, uniqueness from token suffix)
	startYear := 1940
	endYear := 2010
	year := startYear + rnd.Intn(endYear-startYear)
	month := rnd.Intn(12) + 1
	day := rnd.Intn(28) + 1
	dob := fmt.Sprintf("%04d-%02d-%02d-%s", year, month, day, dobToken)

	// Generate SSN (random for variety, uniqueness from token suffix)
	area := rnd.Intn(899) + 1
	if area == 666 {
		area = 667
	}
	group := rnd.Intn(99) + 1
	serial := rnd.Intn(9999) + 1
	ssn := fmt.Sprintf("%03d-%02d-%04d-%s", area, group, serial, ssnToken)

	return VaultRecord{
		ID:            recordID,
		IDToken:       idToken,
		FullName:      fullName,
		FullNameToken: nameToken,
		DOB:           dob,
		DOBToken:      dobToken,
		SSN:           ssn,
		SSNToken:      ssnToken,
	}
}

// generateUnionModeRecord generates a record for union mode (CLM or MBR table)
// Generates separate first/middle/last names and multiple ID types
func generateUnionModeRecord(timestamp string, recordID int64, tableType string, rnd *rand.Rand) VaultRecord {
	// Generate tokens first (unique and deterministic)
	firstNameToken := generateUniqueToken("tok_fname", timestamp, recordID*3)
	lastNameToken := generateUniqueToken("tok_lname", timestamp, recordID*3+1)
	midInitToken := generateUniqueToken("tok_minit", timestamp, recordID*3+2)

	enrollmentIDToken := generateUniqueToken("tok_enroll", timestamp, recordID*5)
	hcIDToken := generateUniqueToken("tok_hc", timestamp, recordID*5+1)
	subscriberIDToken := generateUniqueToken("tok_sub", timestamp, recordID*5+2)

	dobToken := generateUniqueToken("tok_dob", timestamp, recordID*5+3)
	ssnToken := generateUniqueToken("tok_ssn", timestamp, recordID*5+4)

	// Generate name components (random for variety)
	firstName := firstNames[rnd.Intn(len(firstNames))]
	lastName := lastNames[rnd.Intn(len(lastNames))]
	// Generate middle initial (A-Z)
	middleInitial := string(rune('A' + rnd.Intn(26)))

	// Generate DOB
	startYear := 1940
	endYear := 2010
	year := startYear + rnd.Intn(endYear-startYear)
	month := rnd.Intn(12) + 1
	day := rnd.Intn(28) + 1
	dob := fmt.Sprintf("%04d-%02d-%02d", year, month, day)

	// Generate SSN
	area := rnd.Intn(899) + 1
	if area == 666 {
		area = 667
	}
	group := rnd.Intn(99) + 1
	serial := rnd.Intn(9999) + 1
	ssn := fmt.Sprintf("%03d-%02d-%04d", area, group, serial)

	// Generate IDs (use recordID as base for uniqueness)
	enrollmentID := recordID*3 + 100000
	hcID := recordID*7 + 200000
	subscriberID := recordID*11 + 300000

	return VaultRecord{
		FirstName:         firstName,
		FirstNameToken:    firstNameToken,
		LastName:          lastName,
		LastNameToken:     lastNameToken,
		MiddleInitial:     middleInitial,
		MiddleInitToken:   midInitToken,
		EnrollmentID:      enrollmentID,
		EnrollmentIDToken: enrollmentIDToken,
		HCID:              hcID,
		HCIDToken:         hcIDToken,
		SubscriberID:      subscriberID,
		SubscriberIDToken: subscriberIDToken,
		DOB:               dob,
		DOBToken:          dobToken,
		SSN:               ssn,
		SSNToken:          ssnToken,
		TableType:         tableType,
	}
}

func main() {
	// Custom usage message
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "SKYFLOW BYOT MOCK DATA GENERATOR\n")
		fmt.Fprintf(os.Stderr, strings.Repeat("=", 80)+"\n\n")
		fmt.Fprintf(os.Stderr, "Generates mock patient data with tokens for testing Skyflow BYOT loader.\n")
		fmt.Fprintf(os.Stderr, "Supports both CSV file output and direct Snowflake table creation.\n\n")
		fmt.Fprintf(os.Stderr, "USAGE:\n")
		fmt.Fprintf(os.Stderr, "  %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "FLAGS:\n")
		fmt.Fprintf(os.Stderr, "  -count int\n")
		fmt.Fprintf(os.Stderr, "        Number of records to generate (default: 1000)\n")
		fmt.Fprintf(os.Stderr, "        For CSV: generates this many records per vault type\n")
		fmt.Fprintf(os.Stderr, "        For Snowflake: total records across tables\n\n")
		fmt.Fprintf(os.Stderr, "  -output string\n")
		fmt.Fprintf(os.Stderr, "        Output destination: \"csv\" or \"snowflake\" (default: \"csv\")\n")
		fmt.Fprintf(os.Stderr, "        csv: Creates local CSV files in data directory\n")
		fmt.Fprintf(os.Stderr, "        snowflake: Writes directly to Snowflake tables\n\n")
		fmt.Fprintf(os.Stderr, "  -data-dir string\n")
		fmt.Fprintf(os.Stderr, "        Data directory for CSV output (default: \"data\")\n")
		fmt.Fprintf(os.Stderr, "        Only used when -output=csv\n\n")
		fmt.Fprintf(os.Stderr, "  -config string\n")
		fmt.Fprintf(os.Stderr, "        Path to config.json file (default: \"config.json\")\n")
		fmt.Fprintf(os.Stderr, "        Used for Snowflake connection credentials\n\n")
		fmt.Fprintf(os.Stderr, "SNOWFLAKE-SPECIFIC FLAGS:\n")
		fmt.Fprintf(os.Stderr, "  -sf-query-mode string\n")
		fmt.Fprintf(os.Stderr, "        Query mode: \"simple\" or \"union\" (default: \"simple\")\n")
		fmt.Fprintf(os.Stderr, "        simple: Creates PATIENTS table (id, full_name, dob, ssn + tokens)\n")
		fmt.Fprintf(os.Stderr, "        union: Creates CLM/MBR tables with separate name fields\n\n")
		fmt.Fprintf(os.Stderr, "  -sf-table string\n")
		fmt.Fprintf(os.Stderr, "        Snowflake table name (optional)\n")
		fmt.Fprintf(os.Stderr, "        Default: \"PATIENTS\" for simple mode, \"CLM\" for union mode\n")
		fmt.Fprintf(os.Stderr, "        For union mode: automatically creates both CLM and MBR tables\n\n")
		fmt.Fprintf(os.Stderr, "  -sf-batch-size int\n")
		fmt.Fprintf(os.Stderr, "        Snowflake insert batch size (default: 10000)\n")
		fmt.Fprintf(os.Stderr, "        Number of records to insert in a single batch\n\n")
		fmt.Fprintf(os.Stderr, "  -truncate\n")
		fmt.Fprintf(os.Stderr, "        Truncate existing table data before inserting (default: false)\n")
		fmt.Fprintf(os.Stderr, "        WARNING: This will delete all existing data in the target tables\n\n")
		fmt.Fprintf(os.Stderr, "EXAMPLES:\n")
		fmt.Fprintf(os.Stderr, "  # Generate 1M records to CSV files (creates 4M total: 1M per vault)\n")
		fmt.Fprintf(os.Stderr, "  %s -count 1000000 -output csv\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate 5M records to Snowflake PATIENTS table (simple mode)\n")
		fmt.Fprintf(os.Stderr, "  %s -count 5000000 -output snowflake\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate 10M records to Snowflake in union mode (CLM/MBR tables)\n")
		fmt.Fprintf(os.Stderr, "  %s -count 10000000 -output snowflake -sf-query-mode union\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate to custom Snowflake table with truncation\n")
		fmt.Fprintf(os.Stderr, "  %s -count 1000000 -output snowflake -sf-table PATIENTS_TEST -truncate\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Generate CSV files in custom directory\n")
		fmt.Fprintf(os.Stderr, "  %s -count 500000 -output csv -data-dir ./test_data\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "OUTPUT:\n")
		fmt.Fprintf(os.Stderr, "  CSV Mode:\n")
		fmt.Fprintf(os.Stderr, "    Creates separate files for each vault type:\n")
		fmt.Fprintf(os.Stderr, "      - name_data.csv / name_tokens.csv\n")
		fmt.Fprintf(os.Stderr, "      - id_data.csv / id_tokens.csv\n")
		fmt.Fprintf(os.Stderr, "      - dob_data.csv / dob_tokens.csv\n")
		fmt.Fprintf(os.Stderr, "      - ssn_data.csv / ssn_tokens.csv\n\n")
		fmt.Fprintf(os.Stderr, "  Snowflake Simple Mode:\n")
		fmt.Fprintf(os.Stderr, "    Creates PATIENTS table with columns:\n")
		fmt.Fprintf(os.Stderr, "      - id, id_token, full_name, full_name_token\n")
		fmt.Fprintf(os.Stderr, "      - dob, dob_token, ssn, ssn_token\n\n")
		fmt.Fprintf(os.Stderr, "  Snowflake Union Mode:\n")
		fmt.Fprintf(os.Stderr, "    Creates CLM table with SRC_* prefixed columns:\n")
		fmt.Fprintf(os.Stderr, "      - SRC_MBR_FRST_NM, SRC_MBR_LAST_NM, SRC_MBR_MID_INIT_NM\n")
		fmt.Fprintf(os.Stderr, "      - SRC_ENRLMNT_ID, SRC_HC_ID, SRC_SBSCRBR_ID\n")
		fmt.Fprintf(os.Stderr, "      - SRC_MBR_BRTH_DT, SRC_MBR_SSN (+ tokens)\n")
		fmt.Fprintf(os.Stderr, "    Creates MBR table with non-prefixed columns:\n")
		fmt.Fprintf(os.Stderr, "      - FRST_NM, LAST_NM, MID_INIT_NM\n")
		fmt.Fprintf(os.Stderr, "      - ENRLMNT_ID, HC_ID, SBSCRBR_ID\n")
		fmt.Fprintf(os.Stderr, "      - BRTH_DT, SSN (+ tokens)\n\n")
		fmt.Fprintf(os.Stderr, "NOTES:\n")
		fmt.Fprintf(os.Stderr, "  - All tokens are guaranteed unique using SHA256 hashing\n")
		fmt.Fprintf(os.Stderr, "  - Records are generated in parallel using 8 workers\n")
		fmt.Fprintf(os.Stderr, "  - Progress updates shown every 50,000 records\n")
		fmt.Fprintf(os.Stderr, "  - Snowflake credentials read from config.json\n\n")
	}

	// Define flags
	count := flag.Int("count", 1000, "Number of records to generate")
	outputType := flag.String("output", "csv", "Output type: csv or snowflake")
	sfTable := flag.String("sf-table", "", "Snowflake table name (default: PATIENTS for simple, CLM/MBR for union)")
	sfBatchSize := flag.Int("sf-batch-size", 10000, "Snowflake insert batch size")
	sfQueryMode := flag.String("sf-query-mode", "simple", "Snowflake query mode: simple or union")
	truncate := flag.Bool("truncate", false, "Truncate existing table data before inserting (Snowflake only)")
	configPath := flag.String("config", "config.json", "Path to config.json")
	dataDir := flag.String("data-dir", "data", "Data directory for CSV output")

	flag.Parse()

	fmt.Println("=" + strings.Repeat("=", 79))
	fmt.Println("SKYFLOW BYOT MOCK DATA GENERATOR")
	fmt.Println("=" + strings.Repeat("=", 79))
	fmt.Printf("\n")

	// Generate timestamp for this run (nanosecond precision for uniqueness across runs)
	timestamp := fmt.Sprintf("%d", time.Now().UnixNano())

	// Setup output writer based on type
	var writer OutputWriter
	var vaultTypes []string

	if *outputType == "snowflake" {
		// Snowflake mode
		config, err := loadSnowflakeConfig(*configPath)
		if err != nil {
			fmt.Printf("❌ Failed to load Snowflake config: %v\n", err)
			os.Exit(1)
		}

		tableName := *sfTable
		if tableName == "" {
			if *sfQueryMode == "union" {
				tableName = "CLM"
			} else {
				tableName = "PATIENTS"
			}
		}

		writer = NewSnowflakeOutputWriter(config, tableName, *sfBatchSize, *sfQueryMode)
		vaultTypes = []string{} // Not needed for Snowflake unified table

		fmt.Printf("📊 Output: Snowflake\n")
		fmt.Printf("   Query mode: %s\n", *sfQueryMode)
		if *sfQueryMode == "union" {
			tableNameMBR := strings.Replace(tableName, "CLM", "MBR", 1)
			if tableNameMBR == tableName {
				tableNameMBR = tableName + "_MBR"
			}
			fmt.Printf("   Tables: %s.%s.%s & %s.%s.%s\n",
				config.Database, config.Schema, tableName,
				config.Database, config.Schema, tableNameMBR)
		} else {
			fmt.Printf("   Table: %s.%s.%s\n", config.Database, config.Schema, tableName)
		}
		fmt.Printf("   Records: %s\n", formatNumber(*count))
		fmt.Printf("   Batch size: %s\n\n", formatNumber(*sfBatchSize))

	} else {
		// CSV mode
		writer = NewCSVOutputWriter(*dataDir)
		vaultTypes = []string{"name", "id", "dob", "ssn"}

		fmt.Printf("📊 Output: CSV\n")
		fmt.Printf("   Directory: %s\n", *dataDir)
		fmt.Printf("   Records: %s per vault\n", formatNumber(*count))
		fmt.Printf("   Total: %s records\n\n", formatNumber(*count*4))
	}

	// Connect
	if err := writer.Connect(); err != nil {
		fmt.Printf("❌ Failed to connect: %v\n", err)
		os.Exit(1)
	}

	// Initialize
	if err := writer.Initialize(vaultTypes); err != nil {
		fmt.Printf("❌ Failed to initialize: %v\n", err)
		os.Exit(1)
	}

	// Truncate tables if requested
	if *truncate {
		if err := writer.TruncateTables(); err != nil {
			fmt.Printf("❌ Failed to truncate tables: %v\n", err)
			os.Exit(1)
		}
	}

	// Track overall time
	overallStart := time.Now()

	// Generate records
	fmt.Printf("🔄 Generating records...\n")

	numWorkers := 8
	var wg sync.WaitGroup
	var recordsGenerated int64 = 0

	recordsPerWorker := *count / numWorkers
	remainder := *count % numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		workerRecords := recordsPerWorker
		if w == 0 {
			workerRecords += remainder
		}

		startRecordID := int64(w) * int64(recordsPerWorker)

		go func(workerID int, numRecs int, baseRecordID int64, queryMode string) {
			defer wg.Done()
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*1000000)))

			for i := 0; i < numRecs; i++ {
				recordID := baseRecordID + int64(i)

				var record VaultRecord
				if *outputType == "snowflake" {
					if queryMode == "union" {
						// For union mode, alternate between CLM and MBR tables
						tableType := "CLM"
						if recordID%2 == 0 {
							tableType = "MBR"
						}
						record = generateUnionModeRecord(timestamp, recordID, tableType, localRand)
					} else {
						// Simple mode - unified PATIENTS table
						record = generateCompleteRecord(timestamp, recordID, localRand)
					}
				} else {
					// CSV mode
					record = generateCompleteRecord(timestamp, recordID, localRand)
				}

				if err := writer.WriteRecord(record); err != nil {
					fmt.Printf("❌ Error writing record: %v\n", err)
					return
				}

				generated := atomic.AddInt64(&recordsGenerated, 1)
				if generated%50000 == 0 {
					fmt.Fprintf(os.Stdout, "  Generated %s/%s records (%.1f%%)...\n",
						formatNumber(int(generated)),
						formatNumber(*count),
						float64(generated)/float64(*count)*100)
					os.Stdout.Sync() // Force flush to show progress immediately
				}
			}
		}(w, workerRecords, startRecordID, *sfQueryMode)
	}

	wg.Wait()

	// Flush and close
	if err := writer.Flush(); err != nil {
		fmt.Printf("❌ Failed to flush: %v\n", err)
		os.Exit(1)
	}

	if err := writer.Close(); err != nil {
		fmt.Printf("❌ Failed to close: %v\n", err)
		os.Exit(1)
	}

	// Calculate overall duration
	overallDuration := time.Since(overallStart)

	// Get stats
	recordsWritten, destination := writer.GetStats()

	// Performance metrics
	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
	fmt.Printf("📊 PERFORMANCE METRICS\n")
	fmt.Printf("%s\n", strings.Repeat("=", 80))
	fmt.Printf("Total Records:        %s\n", formatNumber(int(recordsWritten)))
	fmt.Printf("Total Time:           %.3f seconds\n", overallDuration.Seconds())
	fmt.Printf("Overall Throughput:   %s records/sec\n", formatNumber(int(float64(recordsWritten)/overallDuration.Seconds())))
	fmt.Printf("%s\n", strings.Repeat("=", 80))

	fmt.Printf("\n✅ Successfully generated %s records!\n", formatNumber(int(recordsWritten)))

	if *outputType == "csv" {
		fmt.Printf("\nYou can now run:\n")
		fmt.Printf("   ./skyflow-loader -source csv\n")
	} else {
		fmt.Printf("\nTable: %s\n", destination)
		fmt.Printf("\nYou can now run:\n")
		fmt.Printf("   ./skyflow-loader -source snowflake\n")
	}
	fmt.Println()
}
