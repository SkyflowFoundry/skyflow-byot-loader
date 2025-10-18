package types

import "context"

// DataType represents the type of data being processed
type DataType string

const (
	DataTypeName DataType = "NAME"
	DataTypeID   DataType = "ID"
	DataTypeDOB  DataType = "DOB"
	DataTypeSSN  DataType = "SSN"
)

// Config holds the application configuration
type Config struct {
	// Skyflow configuration
	VaultURL    string
	BearerToken string

	// Performance configuration
	BatchSize      int
	MaxConcurrency int
	MaxRetries     int
	RetryDelayMs   int

	// AWS configuration
	AWSRegion         string
	SecretName        string
	UseSecretsManager bool

	// Data type mappings
	DataTypeMappings map[DataType]VaultMapping
}

// VaultMapping defines vault, table, and column for a data type
type VaultMapping struct {
	VaultID string `json:"vault_id"`
	Table   string `json:"table"`
	Column  string `json:"column"`
}

// SnowflakeRequest represents the incoming request from Snowflake
type SnowflakeRequest struct {
	Data [][]interface{} `json:"data"`
}

// SnowflakeResponse represents the response to Snowflake
type SnowflakeResponse struct {
	Data [][]interface{} `json:"data"`
}

// TokenizeRequest represents a single tokenization request
type TokenizeRequest struct {
	RowIndex int
	Value    string
	VaultID  string
	Table    string
	Column   string
}

// DetokenizeRequest represents a single detokenization request
type DetokenizeRequest struct {
	RowIndex int
	Token    string
	VaultID  string
}

// Result represents the result of a tokenize/detokenize operation
type Result struct {
	RowIndex int
	Value    string
	Error    error
}

// SkyflowClient defines the interface for Skyflow operations
type SkyflowClient interface {
	TokenizeBatch(ctx context.Context, requests []TokenizeRequest) ([]Result, error)
	DetokenizeBatch(ctx context.Context, requests []DetokenizeRequest) ([]Result, error)
}

// SkyflowTokenizeRequest is the API request format for tokenization
type SkyflowTokenizeRequest struct {
	Records      []map[string]interface{} `json:"records"`
	Tokenization bool                     `json:"tokenization"`
	Upsert       string                   `json:"upsert"`
}

// SkyflowTokenizeResponse is the API response format for tokenization
type SkyflowTokenizeResponse struct {
	Records []SkyflowTokenizeRecord `json:"records"`
}

// SkyflowTokenizeRecord represents a single record in the tokenize response
type SkyflowTokenizeRecord struct {
	Tokens map[string]string      `json:"tokens"`
	Fields map[string]interface{} `json:"fields"`
	Error  *SkyflowError          `json:"error,omitempty"`
}

// SkyflowDetokenizeRequest is the API request format for detokenization
type SkyflowDetokenizeRequest struct {
	DetokenizationParameters []SkyflowDetokenizeParam `json:"detokenizationParameters"`
}

// SkyflowDetokenizeParam represents a single token to detokenize
type SkyflowDetokenizeParam struct {
	Token     string `json:"token"`
	Redaction string `json:"redaction"`
}

// SkyflowDetokenizeResponse is the API response format for detokenization
type SkyflowDetokenizeResponse struct {
	Records []SkyflowDetokenizeRecord `json:"records"`
}

// SkyflowDetokenizeRecord represents a single record in the detokenize response
type SkyflowDetokenizeRecord struct {
	Value    string        `json:"value,omitempty"`
	ValueStr string        `json:"valueStr,omitempty"`
	Error    *SkyflowError `json:"error,omitempty"`
}

// SkyflowError represents an error from the Skyflow API
type SkyflowError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}
