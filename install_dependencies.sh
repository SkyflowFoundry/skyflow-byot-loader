#!/bin/bash

# Skyflow Multi-Vault BYOT Loader - Go Version (v4)
# Dependency Installation Script

echo "Installing dependencies for Skyflow BYOT Loader (Go version)..."
echo "================================================================"
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null
then
    echo "❌ Go is not installed"
    echo ""
    echo "Please install Go 1.21 or higher:"
    echo ""
    echo "macOS:"
    echo "  brew install go"
    echo ""
    echo "Linux:"
    echo "  wget https://go.dev/dl/go1.21.0.linux-amd64.tar.gz"
    echo "  sudo tar -C /usr/local -xzf go1.21.0.linux-amd64.tar.gz"
    echo "  export PATH=\$PATH:/usr/local/go/bin"
    echo ""
    echo "Or visit: https://go.dev/doc/install"
    exit 1
fi

# Check Go version
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "✅ Go version detected: $GO_VERSION"
echo ""

# Initialize Go module (if not already done)
if [ ! -f "go.mod" ]; then
    echo "Initializing Go module..."
    go mod init skyflow-byot-loader
    echo ""
fi

# Download dependencies
echo "Downloading Go module dependencies..."
echo "  - github.com/snowflakedb/gosnowflake (Snowflake connector)"
echo "  - golang.org/x/term (secure password input)"
echo "  - Standard library packages"
echo ""
go mod download
go mod tidy

if [ $? -eq 0 ]; then
    echo ""
    echo "================================================================"
    echo "✅ All dependencies installed successfully!"
    echo "================================================================"
    echo ""
    echo "Dependencies installed:"
    echo "  ✅ Snowflake Go driver (github.com/snowflakedb/gosnowflake)"
    echo "  ✅ Secure password input (golang.org/x/term)"
    echo "  ✅ Go standard library"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Configure the application:"
    echo "   cp config.example.json config.json"
    echo "   # Edit config.json with your credentials and settings"
    echo ""
    echo "2. Build the loader:"
    echo "   go build -o skyflow-loader main.go"
    echo ""
    echo "3. Run with interactive credential prompts (recommended):"
    echo "   ./skyflow-loader"
    echo "   # Will prompt for Skyflow bearer token"
    echo ""
    echo "4. Run with token via CLI (overrides config and prompts):"
    echo "   ./skyflow-loader -token \"YOUR_BEARER_TOKEN\""
    echo ""
    echo "5. Run with Snowflake data source (will prompt for credentials):"
    echo "   ./skyflow-loader -source snowflake"
    echo "   # Will prompt for Skyflow bearer token and Snowflake password"
    echo ""
    echo "6. Run with custom options:"
    echo "   ./skyflow-loader -vault dob -concurrency 64"
    echo ""
    echo "7. View all options:"
    echo "   ./skyflow-loader -help"
    echo ""
    echo "8. Run 4 processes in parallel (maximum performance):"
    echo "   ./skyflow-loader -vault name &"
    echo "   ./skyflow-loader -vault id &"
    echo "   ./skyflow-loader -vault dob &"
    echo "   ./skyflow-loader -vault ssn &"
    echo "   wait"
    echo ""
    echo "================================================================"
    echo "Performance Notes:"
    echo "================================================================"
    echo ""
    echo "Key features:"
    echo "  ✅ True concurrent goroutines"
    echo "  ✅ Multiple data sources (CSV and Snowflake)"
    echo "  ✅ Interactive credential prompts (secure password input)"
    echo "  ✅ Optimized JSON serialization"
    echo "  ✅ Low memory overhead"
    echo "  ✅ Compiled binary for fast execution"
    echo ""
    echo "Data sources supported:"
    echo "  ✅ CSV files (local)"
    echo "  ✅ Snowflake (with multiple query modes)"
    echo ""
    echo "Security features:"
    echo "  ✅ Credentials can be entered interactively (not stored in config)"
    echo "  ✅ Passwords hidden during input"
    echo "  ✅ CLI flags override config file values"
    echo ""
else
    echo ""
    echo "================================================================"
    echo "❌ ERROR: Dependency installation failed"
    echo "================================================================"
    exit 1
fi
