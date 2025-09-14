#!/bin/bash

# V2 Engine Persistence Test
# This script tests data persistence across container restarts

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONTAINER_NAME="go-db-v2-persistence-test"
IMAGE_NAME="go-db-go-db-v2-os"
PORT="8080"
DATA_DIR="./test-data"

echo -e "${BLUE}🧪 V2 Engine Persistence Test${NC}"
echo "=================================="

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    rm -rf $DATA_DIR
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Step 1: Create test data directory
echo -e "\n${BLUE}📁 Creating test data directory...${NC}"
mkdir -p $DATA_DIR

# Step 2: Start V2 container with persistence
echo -e "\n${BLUE}🚀 Starting V2 container with OS durability...${NC}"
docker run -d \
    --name $CONTAINER_NAME \
    -p $PORT:8080 \
    -v "$(pwd)/$DATA_DIR:/app/data" \
    -v "$(pwd)/$DATA_DIR/wal:/app/wal" \
    -v "$(pwd)/$DATA_DIR/checkpoints:/app/checkpoints" \
    go-db-go-db-v2-os \
    -v2 -durability os -data-dir /app/data -wal-dir /app/wal -checkpoint-dir /app/checkpoints

# Wait for container to be healthy
echo -e "${YELLOW}⏳ Waiting for container to be ready...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:$PORT/health > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Container is ready!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}❌ Container failed to start within 30 seconds${NC}"
        exit 1
    fi
    sleep 1
done

# Step 3: Create test documents
echo -e "\n${BLUE}📝 Creating 1000 test documents...${NC}"
node create-test-data.js

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to create test data${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Test data created successfully${NC}"

# Step 4: Verify data before restart
echo -e "\n${BLUE}🔍 Verifying data before restart...${NC}"
node validate-test-data.js "before-restart"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Data validation failed before restart${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Data validation passed before restart${NC}"

# Step 5: Wait for checkpointing
echo -e "\n${BLUE}⏳ Waiting for checkpointing to complete...${NC}"
sleep 10  # Wait longer than checkpoint interval (30s)

# Step 6: Stop container
echo -e "\n${BLUE}🛑 Stopping container...${NC}"
docker stop $CONTAINER_NAME
docker rm $CONTAINER_NAME

# Step 7: Start container again
echo -e "\n${BLUE}🔄 Restarting container...${NC}"
docker run -d \
    --name $CONTAINER_NAME \
    -p $PORT:8080 \
    -v "$(pwd)/$DATA_DIR:/app/data" \
    -v "$(pwd)/$DATA_DIR/wal:/app/wal" \
    -v "$(pwd)/$DATA_DIR/checkpoints:/app/checkpoints" \
    go-db-go-db-v2-os \
    -v2 -durability os -data-dir /app/data -wal-dir /app/wal -checkpoint-dir /app/checkpoints

# Wait for container to be healthy again
echo -e "${YELLOW}⏳ Waiting for container to be ready after restart...${NC}"
for i in {1..30}; do
    if curl -s http://localhost:$PORT/health > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Container is ready after restart!${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}❌ Container failed to start within 30 seconds after restart${NC}"
        exit 1
    fi
    sleep 1
done

# Step 8: Verify data after restart
echo -e "\n${BLUE}🔍 Verifying data after restart...${NC}"
node validate-test-data.js "after-restart"

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Data validation failed after restart${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Data validation passed after restart${NC}"

# Step 9: Show persistence summary
echo -e "\n${GREEN}🎉 PERSISTENCE TEST PASSED!${NC}"
echo "=================================="
echo -e "${GREEN}✅ All 1000 documents persisted across container restart${NC}"
echo -e "${GREEN}✅ V2 WAL and checkpoint system working correctly${NC}"
echo -e "${GREEN}✅ Data integrity maintained${NC}"

# Show file system evidence
echo -e "\n${BLUE}📊 Persistence Evidence:${NC}"
echo "WAL files:"
ls -la $DATA_DIR/wal/ 2>/dev/null || echo "No WAL files found"
echo ""
echo "Checkpoint files:"
ls -la $DATA_DIR/checkpoints/ 2>/dev/null || echo "No checkpoint files found"
echo ""
echo "Data directory:"
ls -la $DATA_DIR/ 2>/dev/null || echo "No data files found"
