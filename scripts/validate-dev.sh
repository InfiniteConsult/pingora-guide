#!/usr/bin/env bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Helper function to check HTTP endpoints
check_http() {
    local name=$1
    local url=$2
    local extra_args=$3

    echo -n "Checking $name ($url)... "

    # We use -s (silent) and -o /dev/null to hide body, -w to get status code
    # We capture stdout/stderr to ensure we catch connection errors
    if curl -s -o /dev/null --fail $extra_args "$url"; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
    fi
}

# Helper function to check TCP (for gRPC)
check_tcp() {
    local name=$1
    local host=$2
    local port=$3

    echo -n "Checking $name ($host:$port)... "
    if nc -zv -w 2 "$host" "$port" &> /dev/null; then
        echo -e "${GREEN}OK${NC}"
    else
        echo -e "${RED}FAILED${NC}"
    fi
}

echo "--- 🚀 Starting Pingora City Validation ---"
echo ""

# 1. Basic Upstreams (Echo Servers)
echo "--- 1. Basic Upstreams (Load Balancing Targets) ---"
check_http "Blue (IP)"  "http://172.28.0.20:8080"
check_http "Blue (DNS)" "http://blue.pingora.local:8080"
check_http "Green (IP)"  "http://172.28.0.21:8080"
check_http "Green (DNS)" "http://green.pingora.local:8080"
echo ""

# 2. Advanced Upstream (Nginx)
echo "--- 2. Advanced Upstream (Nginx) ---"
# Port 80
check_http "HTTP (IP)"  "http://172.28.0.22"
check_http "HTTP (DNS)" "http://advanced.pingora.local"

# Port 443 (HTTPS) - Requires -k for self-signed
check_http "HTTPS (IP)"  "https://172.28.0.22" "-k"
check_http "HTTPS (DNS)" "https://advanced.pingora.local" "-k"

# Port 8081 (H2C) - Requires --http2-prior-knowledge
check_http "H2C (IP)"  "http://172.28.0.22:8081" "--http2-prior-knowledge"
check_http "H2C (DNS)" "http://advanced.pingora.local:8081" "--http2-prior-knowledge"
echo ""

# 3. gRPC Upstream
echo "--- 3. gRPC Upstream (TCP Check) ---"
check_tcp "gRPC (IP)"  "172.28.0.23" "9000"
check_tcp "gRPC (DNS)" "grpc.pingora.local" "9000"

echo ""
echo "--- ✅ Validation Complete ---"