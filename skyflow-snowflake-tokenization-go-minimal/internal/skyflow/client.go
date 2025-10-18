package skyflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"skyflow-snowflake-tokenization-go-minimal/pkg/types"
)

// Client handles communication with Skyflow API
type Client struct {
	vaultURL       string
	bearerToken    string
	httpClient     *http.Client
	retrier        *Retrier
	batchSize      int
	maxConcurrency int
	bufferPool     *sync.Pool
	workerPool     *WorkerPool
}

// NewClient creates a new Skyflow client
func NewClient(config *types.Config) *Client {
	// Performance Optimization #1: HTTP Transport Configuration
	// - HTTP/2 enabled for connection multiplexing
	// - Increased connection pool sizes for high concurrency
	// - Optimized timeouts for Lambda environment
	client := &Client{
		vaultURL:    config.VaultURL,
		bearerToken: config.BearerToken,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        200, // Increased from default 100
				MaxIdleConnsPerHost: 50,  // 2-3x max_concurrency for bursts
				MaxConnsPerHost:     50,  // Prevent connection starvation
				IdleConnTimeout:     120 * time.Second,
				DisableKeepAlives:   false,
				ForceAttemptHTTP2:   true, // Enable HTTP/2 for multiplexing

				// Optimize TCP connection establishment
				DialContext: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 60 * time.Second,
				}).DialContext,

				// TLS optimization
				TLSHandshakeTimeout: 10 * time.Second,

				// Response optimization
				ResponseHeaderTimeout: 20 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		},
		retrier:        NewRetrier(config.MaxRetries, config.RetryDelayMs),
		batchSize:      config.BatchSize,
		maxConcurrency: config.MaxConcurrency,
	}

	// Performance Optimization #2: Buffer Pooling
	// Reuse buffers for JSON marshaling to reduce allocations
	client.bufferPool = &sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4096)) // 4KB initial capacity
		},
	}

	// Performance Optimization #3: Worker Pool Pattern
	// Fixed pool of goroutines eliminates goroutine creation overhead
	client.workerPool = NewWorkerPool(config.MaxConcurrency)

	return client
}

// WorkerPool manages a fixed pool of worker goroutines
type WorkerPool struct {
	workers     int
	workChannel chan func()
	wg          sync.WaitGroup
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(workers int) *WorkerPool {
	wp := &WorkerPool{
		workers:     workers,
		workChannel: make(chan func(), workers*2), // Buffer = 2x workers
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}

	return wp
}

// worker is the goroutine that processes work items
func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for work := range wp.workChannel {
		work()
	}
}

// Submit submits work to the pool
func (wp *WorkerPool) Submit(work func()) {
	wp.workChannel <- work
}

// Wait waits for all work to complete
func (wp *WorkerPool) Wait() {
	close(wp.workChannel)
	wp.wg.Wait()
}

// makeRequest makes an HTTP request to Skyflow with buffer pooling
func (c *Client) makeRequest(ctx context.Context, url string, payload interface{}) ([]byte, error) {
	// Get buffer from pool for request
	buf := c.bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer c.bufferPool.Put(buf)

	// Marshal payload to JSON directly into pooled buffer
	encoder := json.NewEncoder(buf)
	if err := encoder.Encode(payload); err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", url, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)

	// Make request with retry logic (Performance Optimization #4: Adaptive Retry)
	var respBody []byte
	err = c.retrier.Do(ctx, func() error {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Get buffer from pool for response
		respBuf := c.bufferPool.Get().(*bytes.Buffer)
		respBuf.Reset()
		defer c.bufferPool.Put(respBuf)

		// Read response into pooled buffer
		if _, err := respBuf.ReadFrom(resp.Body); err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		// Check status code
		if resp.StatusCode != http.StatusOK {
			return &HTTPError{
				StatusCode: resp.StatusCode,
				Message:    respBuf.String(),
				Response:   resp,
			}
		}

		// Copy bytes to return (pool will be reused)
		respBody = make([]byte, respBuf.Len())
		copy(respBody, respBuf.Bytes())

		return nil
	})

	if err != nil {
		return nil, err
	}

	return respBody, nil
}

// TokenizeBatch tokenizes a batch of requests
func (c *Client) TokenizeBatch(ctx context.Context, requests []types.TokenizeRequest) ([]types.Result, error) {
	if len(requests) == 0 {
		return []types.Result{}, nil
	}

	// Split into batches
	batches := splitTokenizeBatches(requests, c.batchSize)
	results := make([]types.Result, len(requests))
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make([]error, 0)

	// Process batches using worker pool
	for _, batch := range batches {
		batch := batch // Capture for goroutine
		wg.Add(1)
		c.workerPool.Submit(func() {
			defer wg.Done()

			batchResults, err := c.tokenizeSingleBatch(ctx, batch)
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
				return
			}

			// Store results in correct positions
			mu.Lock()
			for _, result := range batchResults {
				results[result.RowIndex] = result
			}
			mu.Unlock()
		})
	}

	wg.Wait()

	if len(errors) > 0 {
		return nil, fmt.Errorf("tokenization failed with %d errors: %v", len(errors), errors[0])
	}

	return results, nil
}

// tokenizeSingleBatch tokenizes a single batch
func (c *Client) tokenizeSingleBatch(ctx context.Context, requests []types.TokenizeRequest) ([]types.Result, error) {
	if len(requests) == 0 {
		return []types.Result{}, nil
	}

	// Group by vault ID
	vaultGroups := make(map[string][]types.TokenizeRequest)
	for _, req := range requests {
		vaultGroups[req.VaultID] = append(vaultGroups[req.VaultID], req)
	}

	// Process each vault group
	var allResults []types.Result
	for vaultID, vaultRequests := range vaultGroups {
		// Build request payload
		records := make([]map[string]interface{}, len(vaultRequests))
		for i, req := range vaultRequests {
			records[i] = map[string]interface{}{
				"table":  req.Table,
				"fields": map[string]interface{}{req.Column: req.Value},
			}
		}

		payload := types.SkyflowTokenizeRequest{
			Records:      records,
			Tokenization: true,
			Upsert:       vaultRequests[0].Column, // Use column name for upsert
		}

		// Make request
		url := fmt.Sprintf("%s/v1/vaults/%s", c.vaultURL, vaultID)
		respBody, err := c.makeRequest(ctx, url, payload)
		if err != nil {
			return nil, fmt.Errorf("tokenize request failed: %w", err)
		}

		// Parse response
		var response types.SkyflowTokenizeResponse
		if err := json.Unmarshal(respBody, &response); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		// Extract results
		for i, record := range response.Records {
			if record.Error != nil {
				log.Printf("Tokenization error for row %d: %s", vaultRequests[i].RowIndex, record.Error.Message)
				allResults = append(allResults, types.Result{
					RowIndex: vaultRequests[i].RowIndex,
					Value:    "",
					Error:    fmt.Errorf("tokenization failed: %s", record.Error.Message),
				})
				continue
			}

			// Get token from response
			token, ok := record.Tokens[vaultRequests[i].Column]
			if !ok {
				allResults = append(allResults, types.Result{
					RowIndex: vaultRequests[i].RowIndex,
					Value:    "",
					Error:    fmt.Errorf("token not found in response"),
				})
				continue
			}

			allResults = append(allResults, types.Result{
				RowIndex: vaultRequests[i].RowIndex,
				Value:    token,
				Error:    nil,
			})
		}
	}

	return allResults, nil
}

// DetokenizeBatch detokenizes a batch of requests
func (c *Client) DetokenizeBatch(ctx context.Context, requests []types.DetokenizeRequest) ([]types.Result, error) {
	if len(requests) == 0 {
		return []types.Result{}, nil
	}

	// Split into batches
	batches := splitDetokenizeBatches(requests, c.batchSize)
	results := make([]types.Result, len(requests))
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make([]error, 0)

	// Process batches using worker pool
	for _, batch := range batches {
		batch := batch // Capture for goroutine
		wg.Add(1)
		c.workerPool.Submit(func() {
			defer wg.Done()

			batchResults, err := c.detokenizeSingleBatch(ctx, batch)
			if err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
				return
			}

			// Store results in correct positions
			mu.Lock()
			for _, result := range batchResults {
				results[result.RowIndex] = result
			}
			mu.Unlock()
		})
	}

	wg.Wait()

	if len(errors) > 0 {
		return nil, fmt.Errorf("detokenization failed with %d errors: %v", len(errors), errors[0])
	}

	return results, nil
}

// detokenizeSingleBatch detokenizes a single batch
func (c *Client) detokenizeSingleBatch(ctx context.Context, requests []types.DetokenizeRequest) ([]types.Result, error) {
	if len(requests) == 0 {
		return []types.Result{}, nil
	}

	// Group by vault ID
	vaultGroups := make(map[string][]types.DetokenizeRequest)
	for _, req := range requests {
		vaultGroups[req.VaultID] = append(vaultGroups[req.VaultID], req)
	}

	// Process each vault group
	var allResults []types.Result
	for vaultID, vaultRequests := range vaultGroups {
		// Build request payload
		detokenizationParameters := make([]types.SkyflowDetokenizeParam, len(vaultRequests))
		for i, req := range vaultRequests {
			detokenizationParameters[i] = types.SkyflowDetokenizeParam{
				Token:     req.Token,
				Redaction: "PLAIN_TEXT",
			}
		}

		payload := types.SkyflowDetokenizeRequest{
			DetokenizationParameters: detokenizationParameters,
		}

		// Make request
		url := fmt.Sprintf("%s/v1/vaults/%s/detokenize", c.vaultURL, vaultID)
		respBody, err := c.makeRequest(ctx, url, payload)
		if err != nil {
			return nil, fmt.Errorf("detokenize request failed: %w", err)
		}

		// Parse response
		var response types.SkyflowDetokenizeResponse
		if err := json.Unmarshal(respBody, &response); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		// Extract results
		for i, record := range response.Records {
			if record.Error != nil {
				log.Printf("Detokenization error for row %d: %s", vaultRequests[i].RowIndex, record.Error.Message)
				allResults = append(allResults, types.Result{
					RowIndex: vaultRequests[i].RowIndex,
					Value:    "",
					Error:    fmt.Errorf("detokenization failed: %s", record.Error.Message),
				})
				continue
			}

			// Get value from response (check both "value" and "valueStr")
			value := record.Value
			if value == "" {
				value = record.ValueStr
			}

			allResults = append(allResults, types.Result{
				RowIndex: vaultRequests[i].RowIndex,
				Value:    value,
				Error:    nil,
			})
		}
	}

	return allResults, nil
}

// splitTokenizeBatches splits tokenize requests into batches
func splitTokenizeBatches(requests []types.TokenizeRequest, batchSize int) [][]types.TokenizeRequest {
	var batches [][]types.TokenizeRequest
	for i := 0; i < len(requests); i += batchSize {
		end := i + batchSize
		if end > len(requests) {
			end = len(requests)
		}
		batches = append(batches, requests[i:end])
	}
	return batches
}

// splitDetokenizeBatches splits detokenize requests into batches
func splitDetokenizeBatches(requests []types.DetokenizeRequest, batchSize int) [][]types.DetokenizeRequest {
	var batches [][]types.DetokenizeRequest
	for i := 0; i < len(requests); i += batchSize {
		end := i + batchSize
		if end > len(requests) {
			end = len(requests)
		}
		batches = append(batches, requests[i:end])
	}
	return batches
}
