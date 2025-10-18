package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"skyflow-snowflake-tokenization-go-minimal/internal/config"
	"skyflow-snowflake-tokenization-go-minimal/internal/skyflow"
	"skyflow-snowflake-tokenization-go-minimal/pkg/types"
)

var (
	cfg    *types.Config
	client types.SkyflowClient
)

// init loads configuration once at cold start
func init() {
	ctx := context.Background()

	// Load configuration
	var err error
	cfg, err = config.Load(ctx)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create Skyflow client
	client = skyflow.NewClient(cfg)

	log.Printf("Initialized with vault URL: %s", cfg.VaultURL)
	log.Printf("Batch size: %d, Max concurrency: %d", cfg.BatchSize, cfg.MaxConcurrency)
}

// handler processes incoming API Gateway requests
func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	log.Printf("Processing request: %s %s", request.HTTPMethod, request.Path)

	// Parse Snowflake request
	var snowflakeReq types.SnowflakeRequest
	if err := json.Unmarshal([]byte(request.Body), &snowflakeReq); err != nil {
		return errorResponse(400, fmt.Sprintf("Invalid request body: %v", err))
	}

	// Determine operation (tokenize/detokenize) from path
	operation, err := skyflow.DetermineOperationFromPath(request.Path)
	if err != nil {
		return errorResponse(400, fmt.Sprintf("Invalid path: %v", err))
	}

	// Determine data type (NAME/ID/DOB/SSN) from path
	dataType, err := skyflow.DetermineDataTypeFromPath(request.Path)
	if err != nil {
		return errorResponse(400, fmt.Sprintf("Invalid data type: %v", err))
	}

	log.Printf("Operation: %s, Data Type: %s, Rows: %d", operation, dataType, len(snowflakeReq.Data))

	// Process request
	var snowflakeResp types.SnowflakeResponse
	if operation == "tokenize" {
		snowflakeResp, err = skyflow.ProcessTokenizeRequest(ctx, client, cfg, dataType, snowflakeReq)
	} else {
		snowflakeResp, err = skyflow.ProcessDetokenizeRequest(ctx, client, cfg, dataType, snowflakeReq)
	}

	if err != nil {
		log.Printf("Operation failed: %v", err)
		return errorResponse(500, fmt.Sprintf("Operation failed: %v", err))
	}

	// Marshal response
	responseBody, err := json.Marshal(snowflakeResp)
	if err != nil {
		return errorResponse(500, fmt.Sprintf("Failed to marshal response: %v", err))
	}

	log.Printf("Completed successfully: %d rows processed", len(snowflakeResp.Data))

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(responseBody),
	}, nil
}

// errorResponse creates an error response
func errorResponse(statusCode int, message string) (events.APIGatewayProxyResponse, error) {
	errorBody := map[string]string{
		"error": message,
	}

	body, _ := json.Marshal(errorBody)

	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Body: string(body),
	}, nil
}

func main() {
	lambda.Start(handler)
}
