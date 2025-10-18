package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"skyflow-snowflake-tokenization-go-minimal/pkg/types"
)

// Load loads configuration from AWS Secrets Manager or environment variables
func Load(ctx context.Context) (*types.Config, error) {
	useSecretsManager := getEnv("USE_SECRETS_MANAGER", "false") == "true"

	if useSecretsManager {
		return loadFromSecretsManager(ctx)
	}

	return loadFromEnvironment()
}

// loadFromSecretsManager loads configuration from AWS Secrets Manager
func loadFromSecretsManager(ctx context.Context) (*types.Config, error) {
	secretName := os.Getenv("SECRET_NAME")
	if secretName == "" {
		return nil, fmt.Errorf("SECRET_NAME environment variable is required when USE_SECRETS_MANAGER=true")
	}

	// Load AWS config
	region := getEnv("AWS_REGION", "us-east-1")
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create Secrets Manager client
	client := secretsmanager.NewFromConfig(cfg)

	// Get secret value
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	}

	result, err := client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s: %w", secretName, err)
	}

	if result.SecretString == nil {
		return nil, fmt.Errorf("secret %s has no string value", secretName)
	}

	// Parse JSON secret
	var config types.Config
	if err := json.Unmarshal([]byte(*result.SecretString), &config); err != nil {
		return nil, fmt.Errorf("failed to parse secret JSON: %w", err)
	}

	// Validate required fields
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// loadFromEnvironment loads configuration from environment variables
func loadFromEnvironment() (*types.Config, error) {
	vaultURL := os.Getenv("VAULT_URL")
	if vaultURL == "" {
		return nil, fmt.Errorf("VAULT_URL environment variable is required")
	}

	bearerToken := os.Getenv("BEARER_TOKEN")
	if bearerToken == "" {
		return nil, fmt.Errorf("BEARER_TOKEN environment variable is required")
	}

	config := &types.Config{
		VaultURL:       vaultURL,
		BearerToken:    bearerToken,
		BatchSize:      getEnvInt("BATCH_SIZE", 100),
		MaxConcurrency: getEnvInt("MAX_CONCURRENCY", 20),
		MaxRetries:     getEnvInt("MAX_RETRIES", 3),
		RetryDelayMs:   getEnvInt("RETRY_DELAY_MS", 1000),
		DataTypeMappings: map[types.DataType]types.VaultMapping{
			types.DataTypeName: {
				VaultID: getEnv("VAULT_ID_NAME", ""),
				Table:   getEnv("TABLE_NAME", "persons"),
				Column:  getEnv("COLUMN_NAME", "name"),
			},
			types.DataTypeID: {
				VaultID: getEnv("VAULT_ID_ID", ""),
				Table:   getEnv("TABLE_ID", "persons"),
				Column:  getEnv("COLUMN_ID", "person_id"),
			},
			types.DataTypeDOB: {
				VaultID: getEnv("VAULT_ID_DOB", ""),
				Table:   getEnv("TABLE_DOB", "persons"),
				Column:  getEnv("COLUMN_DOB", "date_of_birth"),
			},
			types.DataTypeSSN: {
				VaultID: getEnv("VAULT_ID_SSN", ""),
				Table:   getEnv("TABLE_SSN", "persons"),
				Column:  getEnv("COLUMN_SSN", "ssn"),
			},
		},
	}

	// Validate required fields
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// validateConfig validates that required configuration fields are present
func validateConfig(config *types.Config) error {
	if config.VaultURL == "" {
		return fmt.Errorf("vault_url is required")
	}

	if config.BearerToken == "" {
		return fmt.Errorf("bearer_token is required")
	}

	if config.BatchSize <= 0 {
		return fmt.Errorf("batch_size must be > 0")
	}

	if config.MaxConcurrency <= 0 {
		return fmt.Errorf("max_concurrency must be > 0")
	}

	// Validate that all data type mappings have vault IDs
	for dataType, mapping := range config.DataTypeMappings {
		if mapping.VaultID == "" {
			return fmt.Errorf("vault_id for %s is required", dataType)
		}
		if mapping.Table == "" {
			return fmt.Errorf("table for %s is required", dataType)
		}
		if mapping.Column == "" {
			return fmt.Errorf("column for %s is required", dataType)
		}
	}

	return nil
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt gets an integer environment variable with a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intValue int
		if _, err := fmt.Sscanf(value, "%d", &intValue); err == nil {
			return intValue
		}
	}
	return defaultValue
}
