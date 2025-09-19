#!/bin/bash
# Script to compare maximum throughput of different database configurations

set -e

echo "🚀 GO-DB Maximum Throughput Comparison"
echo "======================================"
echo ""
echo "This script tests the maximum possible throughput of each engine:"
echo "• V1 Engine: Dual-Write and No-Saves modes"
echo "• V2 Engine: Memory, OS, and Full durability levels"
echo ""

# Array of configurations to test
declare -a configs=(
    # "go-db-dual-write:V1 Dual-Write Mode (Legacy)"
    # "go-db-no-saves:V1 No-Saves Mode (Maximum Performance)"
    # "go-db-v2-memory:V2 Engine - Memory Durability"
    # "go-db-v2-os:V2 Engine - OS Durability"
    "go-db-v2-full:V2 Engine - Full Durability"
)

# Results directory
RESULTS_DIR="config-thruput-comparison-results"
mkdir -p "$RESULTS_DIR"

# Clean up function
cleanup() {
    echo "🧹 Cleaning up..."
    docker-compose -f ../docker-compose-configs.yml down -v 2>/dev/null || true
    echo "✅ Cleanup complete"
}

# Set trap to cleanup on script exit
trap cleanup EXIT

echo "🧪 Testing ${#configs[@]} configurations for maximum throughput..."
echo "⚠️  WARNING: This test will push engines to their absolute limits!"
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
    sleep 15
    
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
    
    # Run the maximum throughput benchmark
    echo "🏃 Running maximum throughput test..."
    result_file="$RESULTS_DIR/${service_name}-max-thruput-results.txt"
    
    # Choose stress test based on engine type
    if [[ "$service_name" == *"v2"* ]]; then
        stress_test="max-throughput-v2.js"
        echo "   Using V2 max throughput test"
    else
        stress_test="max-throughput-v1.js"
        echo "   Using V1 max throughput test"
    fi
    
    # Capture both stdout and stderr, but show progress
    if k6 run "$stress_test" 2>&1 | tee "$result_file"; then
        echo "✅ Test completed successfully"
        
        # Extract key metrics
        echo "📊 Key Results:"
        echo "   Max Throughput: $(grep -o 'http_reqs.*[0-9]*\.[0-9]*/s' "$result_file" | head -1 | cut -d' ' -f2 || echo 'N/A')"
        echo "   P95 Latency: $(grep -o 'p(95)=[0-9]*\.[0-9]*ms' "$result_file" | head -1 | cut -d'=' -f2 || echo 'N/A')"
        echo "   Error Rate: $(grep -o 'http_req_failed.*[0-9]*\.[0-9]*%' "$result_file" | head -1 | cut -d' ' -f2 || echo 'N/A')"
        echo "   Max VUs: $(grep -o 'vus_max.*[0-9]*' "$result_file" | head -1 | cut -d' ' -f2 || echo 'N/A')"
        
    else
        echo "❌ Test failed"
    fi
    
    # Stop the service and clean volumes
    echo "🛑 Stopping $service_name..."
    docker-compose -f ../docker-compose-configs.yml down -v
    
    echo ""
    echo "⏸️  Waiting 10 seconds before next test..."
    sleep 10
    echo ""
done

echo "🎉 Maximum throughput comparison complete!"
echo ""
echo "📊 Results Summary:"
echo "==================="

# Generate summary table
echo "| Configuration     | Max Throughput | P95 Latency | Error Rate | Max VUs |"
echo "| ----------------- | -------------- | ----------- | ---------- | ------- |"

for config_pair in "${configs[@]}"; do
    IFS=':' read -r service_name description <<< "$config_pair"
    result_file="$RESULTS_DIR/${service_name}-max-thruput-results.txt"
    
    if [ -f "$result_file" ]; then
        throughput=$(grep -o 'http_reqs.*[0-9]*\.[0-9]*/s' "$result_file" | head -1 | cut -d' ' -f2 | cut -d'/' -f1 || echo 'N/A')
        latency=$(grep -o 'p(95)=[0-9]*\.[0-9]*ms' "$result_file" | head -1 | cut -d'=' -f2 || echo 'N/A')
        error_rate=$(grep -o 'http_req_failed.*[0-9]*\.[0-9]*%' "$result_file" | head -1 | cut -d' ' -f2 || echo 'N/A')
        max_vus=$(grep -o 'vus_max.*[0-9]*' "$result_file" | head -1 | cut -d' ' -f2 || echo 'N/A')
        
        printf "| %-17s | %-13s | %-11s | %-9s | %-6s |\n" \
            "$description" "$throughput" "$latency" "$error_rate" "$max_vus"
    else
        printf "| %-17s | %-13s | %-11s | %-9s | %-6s |\n" \
            "$description" "FAILED" "N/A" "N/A" "N/A"
    fi
done

echo ""
echo "📁 Detailed results saved to: $RESULTS_DIR/"
echo ""
echo "🔍 Analysis:"
echo "• Higher throughput = better performance under load"
echo "• Lower latency = faster response times"
echo "• Lower error rate = more reliable under stress"
echo "• Higher Max VUs = can handle more concurrent users"
echo ""
echo "⚠️  Note: These are maximum throughput tests with no artificial delays"
echo "   Real-world performance will be lower due to application logic overhead"
