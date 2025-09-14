#!/bin/bash
# Script to compare different database configurations using the optimized stress test

set -e

echo "🚀 GO-DB Configuration Performance Comparison"
echo "=============================================="
echo ""
echo "This script compares V1 (Dual-Write) and V2 (WAL) storage engines:"
echo "• V1 Engine: Traditional dual-write with optional no-saves mode"
echo "• V2 Engine: WAL-based with configurable durability levels"
echo ""

# Array of configurations to test
declare -a configs=(
    "go-db-dual-write:Dual-Write Mode (Default - Maximum Data Safety)"
    "go-db-no-saves:No-Saves Mode (Maximum Performance)"
    "go-db-v2-memory:V2 Engine - Memory Durability (Fastest)"
    "go-db-v2-os:V2 Engine - OS Durability (Balanced)"
    "go-db-v2-full:V2 Engine - Full Durability (Safest)"
)

# Results directory
RESULTS_DIR="config-comparison-results"
mkdir -p "$RESULTS_DIR"

# Clean up function
cleanup() {
    echo "🧹 Cleaning up..."
    docker-compose -f ../docker-compose-configs.yml down -v 2>/dev/null || true
    echo "✅ Cleanup complete"
}

# Set trap to cleanup on script exit
trap cleanup EXIT

echo "🧪 Testing ${#configs[@]} configurations with optimized stress test..."
echo ""

for config_pair in "${configs[@]}"; do
    IFS=':' read -r service_name description <<< "$config_pair"
    
    echo "========================================"
    echo "🔧 Testing: $description"
    echo "   Service: $service_name"
    echo "========================================"
    
    # Start the service
    echo "🏁 Starting $service_name..."
    docker-compose -f ../docker-compose-configs.yml up -d "$service_name"
    
    # Wait for service to be ready
    echo "⏳ Waiting for service to be ready..."
    sleep 10
    
    # Check if service is healthy
    max_retries=30
    retry_count=0
    while [ $retry_count -lt $max_retries ]; do
        if curl -s http://localhost:8080/health > /dev/null 2>&1; then
            echo "✅ Service is ready!"
            break
        fi
        echo "   Waiting... (attempt $((retry_count + 1))/$max_retries)"
        sleep 2
        retry_count=$((retry_count + 1))
    done
    
    if [ $retry_count -eq $max_retries ]; then
        echo "❌ Service failed to start properly"
        docker-compose -f ../docker-compose-configs.yml logs "$service_name"
        continue
    fi
    
    # Run the benchmark
    echo "🏃 Running optimized stress test..."
    result_file="$RESULTS_DIR/${service_name}-results.txt"
    
    # Capture both stdout and stderr, but show progress
    if k6 run stress-test-optimized.js 2>&1 | tee "$result_file"; then
        echo "✅ Test completed successfully"
        
        # Extract key metrics
        echo "📊 Key Results:"
        grep -E "(http_req_duration|http_reqs|errors)" "$result_file" | grep -E "(avg=|rate=)" || echo "   Metrics extraction failed"
        
    else
        echo "❌ Test failed"
    fi
    
    # Stop the service and clean volumes
    echo "🛑 Stopping $service_name..."
    docker-compose -f ../docker-compose-configs.yml down -v
    
    echo ""
    echo "⏸️  Waiting 5 seconds before next test..."
    sleep 5
    echo ""
done

echo "🏁 All tests completed!"
echo ""
echo "📋 Results Summary:"
echo "=================="
echo ""
echo "V1 Engine (Dual-Write):"
echo "• go-db-dual-write: Writes to both memory and disk for maximum safety"
echo "• go-db-no-saves: Memory-only for maximum performance"
echo ""
echo "V2 Engine (WAL-based):"
echo "• go-db-v2-memory: WAL in memory only (fastest, no persistence)"
echo "• go-db-v2-os: WAL synced to OS (balanced performance/safety)"
echo "• go-db-v2-full: WAL synced to disk (safest, full durability)"
echo ""
for config_pair in "${configs[@]}"; do
    IFS=':' read -r service_name description <<< "$config_pair"
    result_file="$RESULTS_DIR/${service_name}-results.txt"
    
    if [ -f "$result_file" ]; then
        echo ""
        echo "🔧 $description:"
        echo "   File: $result_file"
        
        # Extract P95 and throughput
        p95=$(grep "p(95)" "$result_file" | grep "http_req_duration" | sed -n 's/.*p(95)=\([^[:space:]]*\).*/\1/p' | head -1)
        throughput=$(grep "http_reqs" "$result_file" | sed -n 's/.*http_reqs[^:]*:[^0-9]*\([0-9.]*\).*/\1/p' | head -1)
        
        [ -n "$p95" ] && echo "   P95 Latency: $p95"
        [ -n "$throughput" ] && echo "   Throughput: $throughput req/s"
    else
        echo ""
        echo "❌ $description: No results file found"
    fi
done

echo ""
echo "📁 All detailed results saved in: $RESULTS_DIR/"
echo "🎯 Use these results to update the README documentation!"
