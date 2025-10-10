package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Configuration structures (reusing same config.json format as main.go)
type FileConfig struct {
	Skyflow SkyflowConfig `json:"skyflow"`
}

type SkyflowConfig struct {
	VaultURL    string        `json:"vault_url"`
	BearerToken string        `json:"bearer_token"`
	Vaults      []VaultConfig `json:"vaults"`
}

type VaultConfig struct {
	Name   string `json:"name"`
	ID     string `json:"id"`
	Column string `json:"column"`
}

// API response structures
type FetchResponse struct {
	Records []struct {
		Fields struct {
			SkyflowID string `json:"skyflow_id"`
		} `json:"fields"`
	} `json:"records"`
}

type DeletePayload struct {
	SkyflowIDs []string `json:"skyflow_ids"`
}

// Configuration constants (defaults - can be overridden via flags)
const (
	DEFAULT_FETCH_LIMIT              = 300  // Records per fetch request (skyflow_ids)
	DEFAULT_FETCH_BATCHES_PER_DELETE = 100  // Number of fetch batches to accumulate before deleting
	DEFAULT_DELETE_BATCH_SIZE        = 300  // Records per deletion batch
	DEFAULT_NOTIFICATION_INTERVAL    = 3000 // Progress notification frequency
	DEFAULT_MAX_FETCH_WORKERS        = 20   // Parallel fetch workers to get skyflow_ids
	DEFAULT_MAX_DELETE_WORKERS       = 20   // Parallel delete workers
	REQUEST_TIMEOUT                  = 30   // Request timeout in seconds
	MAX_RETRIES                      = 5    // Max retry attempts for 429/5xx errors
	RETRY_BACKOFF_BASE               = 2.0  // Base delay in seconds
	RETRY_BACKOFF_MAX                = 60.0 // Max delay between retries
)

// Runtime configuration (set from flags or defaults)
var (
	FETCH_LIMIT              int
	FETCH_BATCHES_PER_DELETE int
	DELETE_BATCH_SIZE        int
	NOTIFICATION_INTERVAL    int
	MAX_FETCH_WORKERS        int
	MAX_DELETE_WORKERS       int
	MAX_RECORDS              int // Maximum records to delete (0 = unlimited)
)

// TableStats tracks deletion progress for a table
type TableStats struct {
	TableName     string
	TotalDeleted  int64
	FailedBatches int64
	StartTime     time.Time
	EndTime       time.Time
	Success       bool
}

// Create HTTP client with connection pooling
func createHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  false,
		DisableKeepAlives:   false,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   REQUEST_TIMEOUT * time.Second,
	}
}

// Fetch records from table with retry logic
func fetchRecords(client *http.Client, vaultURL, tableName, bearerToken string, offset int) ([]string, int, error) {
	url := fmt.Sprintf("%s/%s?offset=%d&limit=%d", vaultURL, tableName, offset, FETCH_LIMIT)

	for attempt := 0; attempt < MAX_RETRIES; attempt++ {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+bearerToken)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, 0, fmt.Errorf("request failed: %w", err)
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Handle status codes
		switch resp.StatusCode {
		case 200:
			var fetchResp FetchResponse
			if err := json.Unmarshal(bodyBytes, &fetchResp); err != nil {
				return nil, resp.StatusCode, fmt.Errorf("failed to parse response: %w", err)
			}

			skyflowIDs := make([]string, 0, len(fetchResp.Records))
			for _, record := range fetchResp.Records {
				if record.Fields.SkyflowID != "" {
					skyflowIDs = append(skyflowIDs, record.Fields.SkyflowID)
				}
			}
			return skyflowIDs, 200, nil

		case 401:
			return nil, 401, fmt.Errorf("authentication failed - check token")

		case 404:
			// Table is empty or not found
			return []string{}, 404, nil

		case 429:
			if attempt < MAX_RETRIES-1 {
				backoffMultiplier := 1 << uint(attempt)
				delay := time.Duration(RETRY_BACKOFF_BASE*float64(backoffMultiplier)) * time.Second
				if delay > time.Duration(RETRY_BACKOFF_MAX)*time.Second {
					delay = time.Duration(RETRY_BACKOFF_MAX) * time.Second
				}
				fmt.Printf("[%s] ⚠️  Rate limited (429) fetching. Retrying in %.1fs (attempt %d/%d)\n",
					tableName, delay.Seconds(), attempt+1, MAX_RETRIES)
				time.Sleep(delay)
				continue
			}
			return nil, 429, fmt.Errorf("rate limit persists after %d attempts", MAX_RETRIES)

		default:
			if resp.StatusCode >= 500 && attempt < MAX_RETRIES-1 {
				backoffMultiplier := 1 << uint(attempt)
				delay := time.Duration(RETRY_BACKOFF_BASE*float64(backoffMultiplier)) * time.Second
				if delay > time.Duration(RETRY_BACKOFF_MAX)*time.Second {
					delay = time.Duration(RETRY_BACKOFF_MAX) * time.Second
				}
				fmt.Printf("[%s] ⚠️  Server error (%d). Retrying in %.1fs (attempt %d/%d)\n",
					tableName, resp.StatusCode, delay.Seconds(), attempt+1, MAX_RETRIES)
				time.Sleep(delay)
				continue
			}
			return nil, resp.StatusCode, fmt.Errorf("fetch failed with status %d: %s", resp.StatusCode, string(bodyBytes))
		}
	}

	return nil, 0, fmt.Errorf("max retries exceeded")
}

// Delete batch of records with retry logic
func deleteBatch(client *http.Client, vaultURL, tableName, bearerToken string, batch []string) (int, error) {
	url := fmt.Sprintf("%s/%s", vaultURL, tableName)
	payload := DeletePayload{SkyflowIDs: batch}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal payload: %w", err)
	}

	for attempt := 0; attempt < MAX_RETRIES; attempt++ {
		req, err := http.NewRequest("DELETE", url, bytes.NewReader(payloadBytes))
		if err != nil {
			return 0, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Bearer "+bearerToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return 0, fmt.Errorf("request failed: %w", err)
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// Handle status codes
		switch resp.StatusCode {
		case 200:
			return 200, nil

		case 429:
			if attempt < MAX_RETRIES-1 {
				backoffMultiplier := 1 << uint(attempt)
				delay := time.Duration(RETRY_BACKOFF_BASE*float64(backoffMultiplier)) * time.Second
				if delay > time.Duration(RETRY_BACKOFF_MAX)*time.Second {
					delay = time.Duration(RETRY_BACKOFF_MAX) * time.Second
				}
				fmt.Printf("[%s] ⚠️  Rate limited (429) deleting. Retrying in %.1fs (attempt %d/%d)\n",
					tableName, delay.Seconds(), attempt+1, MAX_RETRIES)
				time.Sleep(delay)
				continue
			}
			return 429, fmt.Errorf("rate limit persists after %d attempts", MAX_RETRIES)

		default:
			if resp.StatusCode >= 500 && attempt < MAX_RETRIES-1 {
				backoffMultiplier := 1 << uint(attempt)
				delay := time.Duration(RETRY_BACKOFF_BASE*float64(backoffMultiplier)) * time.Second
				if delay > time.Duration(RETRY_BACKOFF_MAX)*time.Second {
					delay = time.Duration(RETRY_BACKOFF_MAX) * time.Second
				}
				fmt.Printf("[%s] ⚠️  Server error (%d) deleting. Retrying in %.1fs (attempt %d/%d)\n",
					tableName, resp.StatusCode, delay.Seconds(), attempt+1, MAX_RETRIES)
				time.Sleep(delay)
				continue
			}
			return resp.StatusCode, fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(bodyBytes))
		}
	}

	return 0, fmt.Errorf("max retries exceeded")
}

// Delete all data from a single table
func deleteTableData(client *http.Client, vaultConfig VaultConfig, vaultURL, bearerToken string) *TableStats {
	tableName := vaultConfig.Column
	stats := &TableStats{
		TableName: tableName,
		StartTime: time.Now(),
	}

	fullVaultURL := fmt.Sprintf("%s/v1/vaults/%s", vaultURL, vaultConfig.ID)

	if MAX_RECORDS > 0 {
		fmt.Printf("\n[%s] Starting deletion process (max %d records)...\n", tableName, MAX_RECORDS)
	} else {
		fmt.Printf("\n[%s] Starting deletion process...\n", tableName)
	}

	iteration := 0
	isEmpty := false

	// Stream: fetch batches and delete immediately
	for {
		// Check if we've reached max records limit
		if MAX_RECORDS > 0 && stats.TotalDeleted >= int64(MAX_RECORDS) {
			fmt.Printf("[%s] ✓ Reached max records limit (%d deleted)\n", tableName, stats.TotalDeleted)
			break
		}

		iteration++

		// Fetch multiple batches in parallel to accumulate skyflow_ids
		fmt.Printf("[%s] Iteration %d: Fetching records in parallel...\n", tableName, iteration)

		type fetchResult struct {
			skyflowIDs []string
			statusCode int
			err        error
			offset     int
		}

		resultChan := make(chan fetchResult, FETCH_BATCHES_PER_DELETE)
		var fetchWg sync.WaitGroup

		// Launch parallel fetch workers
		for batchIdx := 0; batchIdx < FETCH_BATCHES_PER_DELETE; batchIdx++ {
			fetchWg.Add(1)
			offset := batchIdx * FETCH_LIMIT
			go func(off int) {
				defer fetchWg.Done()
				skyflowIDs, statusCode, err := fetchRecords(client, fullVaultURL, tableName, bearerToken, off)
				resultChan <- fetchResult{skyflowIDs, statusCode, err, off}
			}(offset)
		}

		// Close channel when all fetches complete
		go func() {
			fetchWg.Wait()
			close(resultChan)
		}()

		// Collect results
		var allSkyflowIDs []string
		firstFetch := true
		for result := range resultChan {
			if result.statusCode == 401 {
				fmt.Printf("[%s] ✗ Authentication failed - check token\n", tableName)
				stats.EndTime = time.Now()
				stats.Success = false
				return stats
			}

			if result.statusCode == 404 && iteration == 1 && firstFetch {
				fmt.Printf("[%s] ℹ️  Table is already empty\n", tableName)
				isEmpty = true
				break
			}

			if result.err != nil && result.statusCode != 404 {
				fmt.Printf("[%s] ⚠️  Fetch error: %v\n", tableName, result.err)
			}

			if len(result.skyflowIDs) > 0 {
				allSkyflowIDs = append(allSkyflowIDs, result.skyflowIDs...)
			}
			firstFetch = false
		}

		// If table was empty from the start
		if isEmpty {
			fmt.Printf("[%s] ✓ No records to delete (already empty)\n", tableName)
			stats.EndTime = time.Now()
			stats.Success = true
			return stats
		}

		if len(allSkyflowIDs) == 0 {
			if iteration == 1 {
				fmt.Printf("[%s] ℹ️  Table is empty\n", tableName)
			} else {
				fmt.Printf("[%s] No more records to delete (iteration %d)\n", tableName, iteration)
			}
			break
		}

		// Limit to MAX_RECORDS if specified
		if MAX_RECORDS > 0 {
			remaining := MAX_RECORDS - int(stats.TotalDeleted)
			if len(allSkyflowIDs) > remaining {
				allSkyflowIDs = allSkyflowIDs[:remaining]
				fmt.Printf("[%s] Iteration %d: Limiting to %d records (reaching max limit)\n", tableName, iteration, len(allSkyflowIDs))
			}
		}

		fmt.Printf("[%s] Iteration %d: Fetched %d records, deleting...\n", tableName, iteration, len(allSkyflowIDs))

		// Split into delete batches
		batches := make([][]string, 0)
		for i := 0; i < len(allSkyflowIDs); i += DELETE_BATCH_SIZE {
			end := i + DELETE_BATCH_SIZE
			if end > len(allSkyflowIDs) {
				end = len(allSkyflowIDs)
			}
			batches = append(batches, allSkyflowIDs[i:end])
		}

		// Delete batches in parallel with worker pool
		var deleteWg sync.WaitGroup
		deleteChan := make(chan []string, MAX_DELETE_WORKERS*2)

		// Start delete workers
		for i := 0; i < MAX_DELETE_WORKERS; i++ {
			deleteWg.Add(1)
			go func() {
				defer deleteWg.Done()
				for batch := range deleteChan {
					statusCode, err := deleteBatch(client, fullVaultURL, tableName, bearerToken, batch)
					if statusCode == 200 {
						newTotal := atomic.AddInt64(&stats.TotalDeleted, int64(len(batch)))
						if newTotal%int64(NOTIFICATION_INTERVAL) == 0 {
							fmt.Printf("[%s] Progress: %d records deleted\n", tableName, newTotal)
						}
					} else {
						atomic.AddInt64(&stats.FailedBatches, 1)
						if err != nil {
							fmt.Printf("[%s] ⚠️  Delete failed: %v\n", tableName, err)
						}
					}
				}
			}()
		}

		// Feed batches to delete workers
		for _, batch := range batches {
			deleteChan <- batch
		}
		close(deleteChan)
		deleteWg.Wait()
	}

	if stats.TotalDeleted > 0 {
		fmt.Printf("[%s] Total records deleted: %d\n", tableName, stats.TotalDeleted)
	}

	if stats.FailedBatches > 0 {
		fmt.Printf("[%s] ⚠️  WARNING: %d batches failed\n", tableName, stats.FailedBatches)
		stats.Success = false
	} else {
		if stats.TotalDeleted > 0 {
			fmt.Printf("[%s] ✓ Successfully emptied\n", tableName)
		} else {
			fmt.Printf("[%s] ✓ Table confirmed empty\n", tableName)
		}
		stats.Success = true
	}

	stats.EndTime = time.Now()
	return stats
}

// Delete all vaults sequentially (with parallel deletes within each vault)
func deleteAllVaultsParallel(client *http.Client, vaults []VaultConfig, vaultURL, bearerToken string) map[string]*TableStats {
	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	fmt.Printf("VAULT DELETION OPERATION\n")
	fmt.Printf("%s\n", strings.Repeat("=", 60))
	fmt.Printf("Vault URL: %s\n", vaultURL)
	fmt.Printf("Fetch Limit (per API call): %d records\n", FETCH_LIMIT)
	fmt.Printf("Parallel Fetch Workers: %d\n", MAX_FETCH_WORKERS)
	fmt.Printf("Fetch Batches Accumulated: %d batches (= %d IDs)\n", FETCH_BATCHES_PER_DELETE, FETCH_BATCHES_PER_DELETE*FETCH_LIMIT)
	fmt.Printf("Delete Batch Size: %d records\n", DELETE_BATCH_SIZE)
	fmt.Printf("Parallel Delete Workers: %d\n", MAX_DELETE_WORKERS)
	if MAX_RECORDS > 0 {
		fmt.Printf("Max Records Per Vault: %d\n", MAX_RECORDS)
	} else {
		fmt.Printf("Max Records Per Vault: unlimited\n")
	}
	fmt.Printf("Progress Notification Interval: Every %d records\n", NOTIFICATION_INTERVAL)
	fmt.Printf("\nVaults to process (sequentially, with parallel ops within each):\n")
	for _, v := range vaults {
		fmt.Printf("  - %s (Vault: %s, ID: %s)\n", v.Column, v.Name, v.ID)
	}
	fmt.Printf("%s\n", strings.Repeat("=", 60))

	startTime := time.Now()

	// Process vaults sequentially (parallelization happens within each vault)
	results := make(map[string]*TableStats)

	for _, vault := range vaults {
		stats := deleteTableData(client, vault, vaultURL, bearerToken)
		results[vault.Column] = stats
	}

	// Summary
	elapsed := time.Since(startTime)
	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	fmt.Printf("DELETION OPERATION COMPLETE\n")
	fmt.Printf("%s\n", strings.Repeat("=", 60))
	fmt.Printf("Total time elapsed: %.2f seconds\n", elapsed.Seconds())
	fmt.Printf("\nResults:\n")

	successCount := 0
	var failedTables []string
	for _, stats := range results {
		status := "✅"
		if !stats.Success {
			status = "❌"
			failedTables = append(failedTables, stats.TableName)
		} else {
			successCount++
		}
		fmt.Printf("  %s %s\n", status, stats.TableName)
	}

	fmt.Printf("\nSummary:\n")
	fmt.Printf("  Total tables: %d\n", len(results))
	fmt.Printf("  Successful: %d\n", successCount)
	fmt.Printf("  Failed: %d\n", len(failedTables))

	if len(failedTables) > 0 {
		fmt.Printf("\n❌ Failed tables: %v\n", failedTables)
		fmt.Printf("   Operation completed with errors\n")
		fmt.Printf("   Check authentication token or vault configuration\n")
	} else {
		fmt.Printf("\n✅ All tables successfully emptied!\n")
		fmt.Printf("   All vaults are now clear and ready for fresh data\n")
	}

	return results
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

func main() {
	// Command-line flags
	configFile := flag.String("config", "config.json", "Path to configuration file")
	bearerToken := flag.String("token", "", "Bearer token for authentication (overrides config)")
	vaultURL := flag.String("vault-url", "", "Skyflow vault URL (overrides config)")

	// Performance tuning flags
	fetchWorkers := flag.Int("fetch-workers", DEFAULT_MAX_FETCH_WORKERS, "Number of parallel fetch workers")
	deleteWorkers := flag.Int("delete-workers", DEFAULT_MAX_DELETE_WORKERS, "Number of parallel delete workers")
	fetchBatches := flag.Int("fetch-batches", DEFAULT_FETCH_BATCHES_PER_DELETE, "Number of fetch batches to accumulate")
	deleteBatchSize := flag.Int("delete-batch-size", DEFAULT_DELETE_BATCH_SIZE, "Delete batch size")
	maxRecords := flag.Int("max-records", 0, "Maximum records to delete per vault (0 = unlimited)")

	flag.Parse()

	// Set runtime configuration from flags
	MAX_FETCH_WORKERS = *fetchWorkers
	MAX_DELETE_WORKERS = *deleteWorkers
	FETCH_BATCHES_PER_DELETE = *fetchBatches
	DELETE_BATCH_SIZE = *deleteBatchSize
	MAX_RECORDS = *maxRecords
	FETCH_LIMIT = DEFAULT_FETCH_LIMIT
	NOTIFICATION_INTERVAL = DEFAULT_NOTIFICATION_INTERVAL

	// Load configuration file
	fmt.Printf("📋 Loading configuration from: %s\n", *configFile)
	fileConfig, err := loadConfigFile(*configFile)
	if err != nil {
		fmt.Printf("❌ Failed to load config file: %v\n", err)
		fmt.Printf("   Make sure config.json exists in the current directory\n")
		os.Exit(1)
	}

	// Determine bearer token
	finalBearerToken := *bearerToken
	if finalBearerToken == "" {
		finalBearerToken = fileConfig.Skyflow.BearerToken
	}
	if finalBearerToken == "" {
		fmt.Printf("❌ Error: Bearer token is required (use -token flag or set in config.json)\n")
		os.Exit(1)
	}

	// Determine vault URL
	finalVaultURL := *vaultURL
	if finalVaultURL == "" {
		finalVaultURL = fileConfig.Skyflow.VaultURL
	}
	if finalVaultURL == "" {
		fmt.Printf("❌ Error: Vault URL is required (use -vault-url flag or set in config.json)\n")
		os.Exit(1)
	}

	// Get vaults from config
	vaults := fileConfig.Skyflow.Vaults
	if len(vaults) == 0 {
		fmt.Printf("❌ Error: No vaults defined in config file\n")
		os.Exit(1)
	}

	// Warning message
	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	fmt.Printf("MULTI-VAULT PARALLEL BULK DELETE\n")
	fmt.Printf("*** WARNING: TEST USE ONLY ***\n")
	fmt.Printf("%s\n\n", strings.Repeat("=", 60))

	// Create HTTP client
	client := createHTTPClient()

	// Execute parallel deletion
	deleteAllVaultsParallel(client, vaults, finalVaultURL, finalBearerToken)
}
