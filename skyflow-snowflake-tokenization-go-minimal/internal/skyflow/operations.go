package skyflow

import (
	"context"
	"fmt"
	"strings"

	"skyflow-snowflake-tokenization-go-minimal/pkg/types"
)

// ProcessTokenizeRequest processes a Snowflake tokenization request
func ProcessTokenizeRequest(ctx context.Context, client types.SkyflowClient, config *types.Config, dataType types.DataType, snowflakeReq types.SnowflakeRequest) (types.SnowflakeResponse, error) {
	// Get vault mapping for this data type
	mapping, ok := config.DataTypeMappings[dataType]
	if !ok {
		return types.SnowflakeResponse{}, fmt.Errorf("no vault mapping found for data type: %s", dataType)
	}

	// Convert Snowflake request to tokenize requests
	requests := make([]types.TokenizeRequest, 0, len(snowflakeReq.Data))
	for i, row := range snowflakeReq.Data {
		if len(row) < 2 {
			continue
		}

		// Extract value (second column)
		value, ok := row[1].(string)
		if !ok {
			continue
		}

		// Skip empty values
		if value == "" {
			continue
		}

		requests = append(requests, types.TokenizeRequest{
			RowIndex: i,
			Value:    value,
			VaultID:  mapping.VaultID,
			Table:    mapping.Table,
			Column:   mapping.Column,
		})
	}

	// Tokenize
	results, err := client.TokenizeBatch(ctx, requests)
	if err != nil {
		return types.SnowflakeResponse{}, fmt.Errorf("tokenization failed: %w", err)
	}

	// Convert results to Snowflake response
	responseData := make([][]interface{}, len(snowflakeReq.Data))
	for i := range snowflakeReq.Data {
		// Find result for this row
		var token string
		for _, result := range results {
			if result.RowIndex == i {
				if result.Error != nil {
					token = fmt.Sprintf("ERROR: %v", result.Error)
				} else {
					token = result.Value
				}
				break
			}
		}

		responseData[i] = []interface{}{i, token}
	}

	return types.SnowflakeResponse{Data: responseData}, nil
}

// ProcessDetokenizeRequest processes a Snowflake detokenization request
func ProcessDetokenizeRequest(ctx context.Context, client types.SkyflowClient, config *types.Config, dataType types.DataType, snowflakeReq types.SnowflakeRequest) (types.SnowflakeResponse, error) {
	// Get vault mapping for this data type
	mapping, ok := config.DataTypeMappings[dataType]
	if !ok {
		return types.SnowflakeResponse{}, fmt.Errorf("no vault mapping found for data type: %s", dataType)
	}

	// Convert Snowflake request to detokenize requests
	requests := make([]types.DetokenizeRequest, 0, len(snowflakeReq.Data))
	for i, row := range snowflakeReq.Data {
		if len(row) < 2 {
			continue
		}

		// Extract token (second column)
		token, ok := row[1].(string)
		if !ok {
			continue
		}

		// Skip empty tokens
		if token == "" {
			continue
		}

		requests = append(requests, types.DetokenizeRequest{
			RowIndex: i,
			Token:    token,
			VaultID:  mapping.VaultID,
		})
	}

	// Detokenize
	results, err := client.DetokenizeBatch(ctx, requests)
	if err != nil {
		return types.SnowflakeResponse{}, fmt.Errorf("detokenization failed: %w", err)
	}

	// Convert results to Snowflake response
	responseData := make([][]interface{}, len(snowflakeReq.Data))
	for i := range snowflakeReq.Data {
		// Find result for this row
		var value string
		for _, result := range results {
			if result.RowIndex == i {
				if result.Error != nil {
					value = fmt.Sprintf("ERROR: %v", result.Error)
				} else {
					value = result.Value
				}
				break
			}
		}

		responseData[i] = []interface{}{i, value}
	}

	return types.SnowflakeResponse{Data: responseData}, nil
}

// DetermineDataTypeFromPath extracts the data type from the API Gateway path
// Examples:
//   - /tokenize/name -> NAME
//   - /detokenize/ssn -> SSN
func DetermineDataTypeFromPath(path string) (types.DataType, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid path format: %s", path)
	}

	// Get the last part (data type)
	dataTypeStr := strings.ToUpper(parts[len(parts)-1])

	switch dataTypeStr {
	case "NAME":
		return types.DataTypeName, nil
	case "ID":
		return types.DataTypeID, nil
	case "DOB":
		return types.DataTypeDOB, nil
	case "SSN":
		return types.DataTypeSSN, nil
	default:
		return "", fmt.Errorf("unknown data type: %s", dataTypeStr)
	}
}

// DetermineOperationFromPath extracts the operation from the API Gateway path
// Examples:
//   - /tokenize/name -> "tokenize"
//   - /detokenize/ssn -> "detokenize"
func DetermineOperationFromPath(path string) (string, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid path format: %s", path)
	}

	operation := strings.ToLower(parts[0])
	if operation != "tokenize" && operation != "detokenize" {
		return "", fmt.Errorf("unknown operation: %s", operation)
	}

	return operation, nil
}
