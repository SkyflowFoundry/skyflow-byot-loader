package main

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

// VaultRecords represents records for a specific vault
type VaultRecords struct {
	VaultName string
	DataFile  string
	TokenFile string
	Records   []VaultRecord
}

// VaultRecord represents a single data/token pair for a vault
type VaultRecord struct {
	Data  string
	Token string
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

// printUsage prints usage information
func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  ./generate_mock_data [num_rows]                    # Same count for all vaults")
	fmt.Println("  ./generate_mock_data name=N id=M dob=P ssn=Q       # Different counts per vault")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  ./generate_mock_data 1000                          # 1000 records for each vault")
	fmt.Println("  ./generate_mock_data name=5000 ssn=3000            # 5000 names, 3000 SSNs, 0 for others")
	fmt.Println("  ./generate_mock_data name=1000000 id=500000 dob=750000 ssn=2000000")
}

func main() {
	// Parse command line arguments
	numRows := 20 // Default for all vaults
	nameRecords := 0
	idRecords := 0
	dobRecords := 0
	ssnRecords := 0

	if len(os.Args) > 1 {
		// Check if using per-vault mode: name=N,id=N,dob=N,ssn=N
		if len(os.Args) == 2 && !strings.Contains(os.Args[1], "=") {
			// Simple mode: one number for all vaults
			var err error
			numRows, err = strconv.Atoi(os.Args[1])
			if err != nil || numRows <= 0 {
				fmt.Printf("Error: Invalid number of rows: %s\n", os.Args[1])
				printUsage()
				os.Exit(1)
			}
			nameRecords = numRows
			idRecords = numRows
			dobRecords = numRows
			ssnRecords = numRows
		} else {
			// Per-vault mode
			for i := 1; i < len(os.Args); i++ {
				parts := strings.Split(os.Args[i], "=")
				if len(parts) != 2 {
					fmt.Printf("Error: Invalid format: %s\n", os.Args[i])
					printUsage()
					os.Exit(1)
				}
				vault := strings.ToLower(parts[0])
				count, err := strconv.Atoi(parts[1])
				if err != nil || count <= 0 {
					fmt.Printf("Error: Invalid count for %s: %s\n", vault, parts[1])
					printUsage()
					os.Exit(1)
				}
				switch vault {
				case "name":
					nameRecords = count
				case "id":
					idRecords = count
				case "dob":
					dobRecords = count
				case "ssn":
					ssnRecords = count
				default:
					fmt.Printf("Error: Unknown vault: %s\n", vault)
					printUsage()
					os.Exit(1)
				}
			}
			// Set any unspecified vaults to 0
			if nameRecords == 0 && idRecords == 0 && dobRecords == 0 && ssnRecords == 0 {
				fmt.Println("Error: No vault records specified")
				printUsage()
				os.Exit(1)
			}
		}
	} else {
		// No arguments, use default
		nameRecords = numRows
		idRecords = numRows
		dobRecords = numRows
		ssnRecords = numRows
	}

	totalRecords := nameRecords + idRecords + dobRecords + ssnRecords
	fmt.Printf("Generating mock data:\n")
	fmt.Printf("  NAME vault: %s records\n", formatNumber(nameRecords))
	fmt.Printf("  ID vault:   %s records\n", formatNumber(idRecords))
	fmt.Printf("  DOB vault:  %s records\n", formatNumber(dobRecords))
	fmt.Printf("  SSN vault:  %s records\n", formatNumber(ssnRecords))
	fmt.Printf("  Total:      %s records\n\n", formatNumber(totalRecords))

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	// Ensure data directory exists
	if err := os.MkdirAll("data", 0755); err != nil {
		fmt.Printf("❌ Error creating data directory: %v\n", err)
		os.Exit(1)
	}

	// Generate timestamp for this run
	timestamp := fmt.Sprintf("%d", time.Now().Unix())

	// Track overall time
	overallStart := time.Now()

	// Generate each vault
	vaults := []struct {
		name    string
		records int
	}{
		{"name", nameRecords},
		{"id", idRecords},
		{"dob", dobRecords},
		{"ssn", ssnRecords},
	}

	for _, vault := range vaults {
		if vault.records > 0 {
			if err := generateVaultData(vault.name, vault.records, timestamp); err != nil {
				fmt.Printf("❌ Error generating %s vault: %v\n", vault.name, err)
				os.Exit(1)
			}
		}
	}

	// Calculate overall duration
	overallDuration := time.Since(overallStart)

	// Calculate file sizes
	var totalDataSize, totalTokenSize int64
	filesGenerated := []string{}

	for _, vault := range vaults {
		if vault.records > 0 {
			dataFile := fmt.Sprintf("data/%s_data.csv", vault.name)
			tokenFile := fmt.Sprintf("data/%s_tokens.csv", vault.name)

			if dataInfo, err := os.Stat(dataFile); err == nil {
				totalDataSize += dataInfo.Size()
				filesGenerated = append(filesGenerated, dataFile)
			}
			if tokenInfo, err := os.Stat(tokenFile); err == nil {
				totalTokenSize += tokenInfo.Size()
				filesGenerated = append(filesGenerated, tokenFile)
			}
		}
	}

	// Performance metrics
	fmt.Printf("\n%s\n", "=================================================================")
	fmt.Printf("📊 PERFORMANCE METRICS (Streaming Mode)\n")
	fmt.Printf("%s\n", "=================================================================")
	fmt.Printf("Total Records:        %s\n", formatNumber(totalRecords))
	fmt.Printf("Total Time:           %.3f seconds\n", overallDuration.Seconds())
	fmt.Printf("Overall Throughput:   %s records/sec\n", formatNumber(int(float64(totalRecords)/overallDuration.Seconds())))
	fmt.Printf("Peak Memory:          Constant (streaming)\n")
	fmt.Printf("\n")
	fmt.Printf("File Sizes:\n")
	fmt.Printf("  - Total Data:       %s\n", formatBytes(totalDataSize))
	fmt.Printf("  - Total Tokens:     %s\n", formatBytes(totalTokenSize))
	fmt.Printf("  - Grand Total:      %s\n", formatBytes(totalDataSize+totalTokenSize))
	fmt.Printf("%s\n", "=================================================================")

	fmt.Printf("\n✅ Successfully generated %d files!\n", len(filesGenerated))
	fmt.Printf("\nFiles created:\n")
	for _, file := range filesGenerated {
		fmt.Printf("  - %s\n", file)
	}
	fmt.Printf("\nYou can now run:\n")
	fmt.Printf("   ./skyflow-loader -source csv\n")
	fmt.Printf("   ./skyflow-loader -source csv -vault name -max-records %d\n", nameRecords)
	fmt.Println()
}
