package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// EC2 Instance Metadata
type InstanceMetadata struct {
	InstanceType     string
	InstanceID       string
	AvailabilityZone string
	Architecture     string
}

// System information
type SystemInfo struct {
	CPUCores          int
	CPUModel          string
	MemoryTotalGB     float64
	MemoryAvailableGB float64
	CPUUsagePercent   float64
	NetworkGbps       float64
	ENAEnabled        bool
}

type Report struct {
	Instance  InstanceMetadata
	System    SystemInfo
	IsEC2     bool
	Timestamp string
}

// IMDSv2 client
type IMDSClient struct {
	token       string
	tokenExpiry time.Time
	client      *http.Client
}

func NewIMDSClient() *IMDSClient {
	return &IMDSClient{
		client: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (c *IMDSClient) getToken() error {
	if time.Now().Before(c.tokenExpiry) && c.token != "" {
		return nil
	}

	req, err := http.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "300")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to get IMDSv2 token: status %d", resp.StatusCode)
	}

	token, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	c.token = string(token)
	c.tokenExpiry = time.Now().Add(5 * time.Minute)
	return nil
}

func (c *IMDSClient) fetchMetadata(path string) (string, error) {
	if err := c.getToken(); err != nil {
		return "", err
	}

	url := "http://169.254.169.254/latest/meta-data/" + path
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token", c.token)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("metadata fetch failed: status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(data)), nil
}

func isEC2Instance() bool {
	client := NewIMDSClient()
	_, err := client.fetchMetadata("instance-id")
	return err == nil
}

func getInstanceMetadata() (*InstanceMetadata, error) {
	client := NewIMDSClient()

	instanceType, err := client.fetchMetadata("instance-type")
	if err != nil {
		return nil, fmt.Errorf("failed to get instance type: %w", err)
	}

	instanceID, _ := client.fetchMetadata("instance-id")
	az, _ := client.fetchMetadata("placement/availability-zone")

	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x86_64"
	} else if arch == "arm64" {
		arch = "aarch64"
	}

	return &InstanceMetadata{
		InstanceType:     instanceType,
		InstanceID:       instanceID,
		AvailabilityZone: az,
		Architecture:     arch,
	}, nil
}

func getSystemInfo() (*SystemInfo, error) {
	info := &SystemInfo{
		CPUCores: runtime.NumCPU(),
	}

	// CPU model
	if cpuInfo, err := os.Open("/proc/cpuinfo"); err == nil {
		defer cpuInfo.Close()
		scanner := bufio.NewScanner(cpuInfo)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "model name") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					info.CPUModel = strings.TrimSpace(parts[1])
					break
				}
			}
		}
	}

	// Memory
	if memInfo, err := os.Open("/proc/meminfo"); err == nil {
		defer memInfo.Close()
		scanner := bufio.NewScanner(memInfo)
		memTotal := int64(0)
		memAvailable := int64(0)

		for scanner.Scan() {
			line := scanner.Text()
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				value, err := strconv.ParseInt(fields[1], 10, 64)
				if err != nil {
					continue
				}
				if strings.HasPrefix(fields[0], "MemTotal:") {
					memTotal = value
				} else if strings.HasPrefix(fields[0], "MemAvailable:") {
					memAvailable = value
				}
			}
		}

		info.MemoryTotalGB = float64(memTotal) / 1024 / 1024
		info.MemoryAvailableGB = float64(memAvailable) / 1024 / 1024
	}

	// CPU usage
	info.CPUUsagePercent = estimateCPUUsage()

	// ENA
	info.ENAEnabled = checkENASupport()

	return info, nil
}

func estimateCPUUsage() float64 {
	stat1 := readProcStat()
	time.Sleep(100 * time.Millisecond)
	stat2 := readProcStat()

	if stat1 == nil || stat2 == nil {
		return 0
	}

	total1 := stat1[0] + stat1[1] + stat1[2] + stat1[3]
	total2 := stat2[0] + stat2[1] + stat2[2] + stat2[3]
	idle1 := stat1[3]
	idle2 := stat2[3]

	totalDelta := total2 - total1
	idleDelta := idle2 - idle1

	if totalDelta == 0 {
		return 0
	}

	return 100.0 * float64(totalDelta-idleDelta) / float64(totalDelta)
}

func readProcStat() []int64 {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil
	}

	line := scanner.Text()
	if !strings.HasPrefix(line, "cpu ") {
		return nil
	}

	fields := strings.Fields(line)
	if len(fields) < 5 {
		return nil
	}

	values := make([]int64, 4)
	for i := 0; i < 4; i++ {
		val, err := strconv.ParseInt(fields[i+1], 10, 64)
		if err != nil {
			return nil
		}
		values[i] = val
	}

	return values
}

func checkENASupport() bool {
	if _, err := os.Stat("/sys/module/ena"); err == nil {
		return true
	}

	entries, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.Name() == "lo" {
			continue
		}
		driverPath := fmt.Sprintf("/sys/class/net/%s/device/driver", entry.Name())
		if link, err := os.Readlink(driverPath); err == nil {
			if strings.Contains(link, "ena") {
				return true
			}
		}
	}

	return false
}

func estimateNetworkBandwidth(instanceType string) float64 {
	parts := strings.Split(instanceType, ".")
	if len(parts) != 2 {
		return 0
	}

	family := parts[0]
	size := parts[1]

	// Network-optimized
	if strings.HasSuffix(family, "n") {
		switch size {
		case "large", "xlarge", "2xlarge", "4xlarge":
			return 25.0
		case "8xlarge":
			return 50.0
		case "16xlarge", "24xlarge":
			return 100.0
		default:
			return 25.0
		}
	}

	// Standard families
	switch {
	case strings.HasPrefix(family, "t3"), strings.HasPrefix(family, "t4g"):
		return 5.0
	case strings.HasPrefix(family, "m5"), strings.HasPrefix(family, "m6"), strings.HasPrefix(family, "m7"):
		switch size {
		case "large", "xlarge", "2xlarge", "4xlarge", "8xlarge":
			return 10.0
		case "12xlarge", "16xlarge":
			return 20.0
		case "24xlarge":
			return 25.0
		default:
			return 10.0
		}
	case strings.HasPrefix(family, "c5"), strings.HasPrefix(family, "c6"), strings.HasPrefix(family, "c7"):
		switch size {
		case "large", "xlarge", "2xlarge", "4xlarge":
			return 10.0
		case "9xlarge", "12xlarge":
			return 12.0
		case "18xlarge", "24xlarge":
			return 25.0
		default:
			return 10.0
		}
	case strings.HasPrefix(family, "r5"), strings.HasPrefix(family, "r6"), strings.HasPrefix(family, "r7"):
		switch size {
		case "large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge":
			return 10.0
		case "16xlarge", "24xlarge":
			return 25.0
		default:
			return 10.0
		}
	default:
		return 0
	}
}

func printReport(report *Report) {
	if report.IsEC2 {
		fmt.Println("EC2 Instance:")
		fmt.Printf("  Type:         %s\n", report.Instance.InstanceType)
		fmt.Printf("  Instance ID:  %s\n", report.Instance.InstanceID)
		fmt.Printf("  Zone:         %s\n", report.Instance.AvailabilityZone)
		fmt.Printf("  Architecture: %s\n", report.Instance.Architecture)
		fmt.Println()
	} else {
		fmt.Println("System (Non-EC2):")
	}

	fmt.Println("Resources:")
	fmt.Printf("  CPU Cores:    %d\n", report.System.CPUCores)
	if report.System.CPUModel != "" {
		fmt.Printf("  CPU Model:    %s\n", report.System.CPUModel)
	}
	fmt.Printf("  Memory Total: %.1f GB\n", report.System.MemoryTotalGB)
	fmt.Printf("  Memory Free:  %.1f GB\n", report.System.MemoryAvailableGB)
	if report.System.NetworkGbps > 0 {
		fmt.Printf("  Network:      %.1f Gbps", report.System.NetworkGbps)
		if report.System.ENAEnabled {
			fmt.Printf(" (ENA)")
		}
		fmt.Println()
	}
	fmt.Println()

	fmt.Println("Current State:")
	fmt.Printf("  CPU Usage:    %.1f%%\n", report.System.CPUUsagePercent)
}

func main() {
	flag.Parse()

	onEC2 := isEC2Instance()

	report := &Report{
		IsEC2:     onEC2,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if onEC2 {
		instance, err := getInstanceMetadata()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to get EC2 metadata: %v\n", err)
			os.Exit(1)
		}
		report.Instance = *instance

		// Estimate network bandwidth
		report.System.NetworkGbps = estimateNetworkBandwidth(instance.InstanceType)
	}

	sysInfo, err := getSystemInfo()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get system information: %v\n", err)
		os.Exit(1)
	}
	report.System = *sysInfo

	printReport(report)
}
