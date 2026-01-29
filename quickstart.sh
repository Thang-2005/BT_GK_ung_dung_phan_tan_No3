#!/bin/bash

# Distributed Key-Value Store - Quick Start Script
# This script helps you quickly set up and test the system

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Distributed KV Store - Quick Start${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check Python version
echo -e "${BLUE}[1/5] Checking Python version...${NC}"
python3 --version
if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Python 3 not found!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python 3 detected${NC}"
echo ""

# Create project structure
echo -e "${BLUE}[2/5] Setting up project structure...${NC}"
mkdir -p distributed-kv-store
cd distributed-kv-store

# Download files (placeholder - in real usage, files would be created)
echo -e "${GREEN}✓ Project structure created${NC}"
echo ""

# Run tests
echo -e "${BLUE}[3/5] Running tests...${NC}"
python3 test_system.py || {
    echo -e "${RED}Tests failed. Please check the logs.${NC}"
    exit 1
}
echo -e "${GREEN}✓ All tests passed${NC}"
echo ""

# Start cluster
echo -e "${BLUE}[4/5] Starting cluster (3 nodes)...${NC}"
python3 start_cluster.py 3 &
CLUSTER_PID=$!
sleep 5
echo -e "${GREEN}✓ Cluster started (PID: $CLUSTER_PID)${NC}"
echo ""

# Example client operations
echo -e "${BLUE}[5/5] Running example operations...${NC}"
cat << 'EOF' > example_client.py
from client import KVStoreClient
import time

client = KVStoreClient([
    ("localhost", 5001),
    ("localhost", 5002),
    ("localhost", 5003)
])

print("\n=== Example Operations ===\n")

# PUT
print("1. Storing data...")
client.put("user:1", "Alice")
client.put("user:2", "Bob")
client.put("product:100", "Laptop")

time.sleep(1)

# GET
print("\n2. Retrieving data...")
client.get("user:1")
client.get("user:2")
client.get("product:100")

# DELETE
print("\n3. Deleting data...")
client.delete("user:2")

print("\n4. Verify deletion...")
client.get("user:2")

print("\n✓ Example operations completed!")
EOF

python3 example_client.py
echo ""

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Cluster is running on:"
echo "  - Node 1: localhost:5001"
echo "  - Node 2: localhost:5002"
echo "  - Node 3: localhost:5003"
echo ""
echo "To interact with the cluster:"
echo "  python3 client.py"
echo ""
echo "To stop the cluster:"
echo "  kill $CLUSTER_PID"
echo ""
echo -e "${BLUE}Press Ctrl+C to stop the cluster and exit${NC}"

# Wait for user to stop
trap "kill $CLUSTER_PID 2>/dev/null; echo ''; echo 'Cluster stopped. Goodbye!'; exit 0" INT TERM

wait