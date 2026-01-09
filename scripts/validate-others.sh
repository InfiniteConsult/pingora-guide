#!/usr/bin/env bash

# Colors
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

DEV_IP="172.28.0.10"
DEV_DNS="dev.pingora.local"
PORT="6145"
LOG_FILE="server.log"

echo -e "${CYAN}--- 1. Starting Background Service (Example 05) on Dev ---${NC}"
echo "Redirecting logs to $LOG_FILE. Waiting 10s for startup..."

# Start in background, redirecting ALL output to a file on the host
docker exec pingora_dev bash -c "RUST_LOG=info cargo run --example 05_background_services" > "$LOG_FILE" 2>&1 &

sleep 5

# Function to simply fire a request (Fire and Forget)
fire_request() {
    local container=$1
    local target=$2
    local method=$3

    echo -n "From $container -> Connecting to $method ($target:$PORT)... "
    # We ignore the output here; we only care that the connection happens
    docker exec "$container" sh -c "echo 'HELLO' | nc -w 1 $target $PORT" > /dev/null 2>&1
    echo "Sent."
}

echo ""
echo -e "${CYAN}--- 2. Generating Traffic ---${NC}"

# Loop through both clients (4 total requests)
for client in pingora_client_1 pingora_client_2; do
    fire_request "$client" "$DEV_IP"  "IP Address"
    fire_request "$client" "$DEV_DNS" "DNS Name"
done

# Wait for the Metric Exporter (runs every 2s) to catch up and log the new count
echo "Waiting 5s for metrics to update..."
sleep 5

echo ""
echo -e "${CYAN}--- 3. Graceful Shutdown ---${NC}"
echo "Sending SIGTERM to application..."
docker exec pingora_dev pkill -TERM -f "05_background_services"
sleep 2

echo ""
echo -e "${CYAN}--- 4. Verifying Logs ---${NC}"

# Check the log file for the expected state
if grep -q "Current Total Connections: 4" "$LOG_FILE"; then
    echo -e "${GREEN}SUCCESS: Found 'Current Total Connections: 4' in logs.${NC}"
    rm "$LOG_FILE"
    exit 0
else
    echo -e "${RED}FAILED: Could not find expected connection count.${NC}"
    echo "--- Last 10 lines of $LOG_FILE ---"
    tail -n 10 "$LOG_FILE"
    rm "$LOG_FILE"
    exit 1
fi