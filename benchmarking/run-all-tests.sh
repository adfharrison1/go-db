#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if port is open
check_port() {
    nc -z localhost 8080 2>/dev/null
}

# Function to wait for service to be ready
wait_for_service() {
    local max_attempts=30
    local attempt=1
    
    print_status "Waiting for go-db service to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if check_port; then
            print_success "go-db service is ready!"
            return 0
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "go-db service failed to start within 60 seconds"
    return 1
}

# Function to run k6 test
run_k6_test() {
    local test_file=$1
    local test_name=$2
    
    print_status "Running k6 test: $test_name"
    echo "----------------------------------------"
    
    if k6 run "$test_file"; then
        print_success "k6 test '$test_name' completed successfully"
    else
        print_error "k6 test '$test_name' failed"
        return 1
    fi
    
    echo ""
}

# Function to run wrk test
run_wrk_test() {
    local test_script=$1
    local test_name=$2
    
    print_status "Running wrk test: $test_name"
    echo "----------------------------------------"
    
    if [ -x "$test_script" ]; then
        if ./"$test_script"; then
            print_success "wrk test '$test_name' completed successfully"
        else
            print_error "wrk test '$test_name' failed"
            return 1
        fi
    else
        print_error "wrk test script '$test_script' is not executable"
        return 1
    fi
    
    echo ""
}

# Main execution
main() {
    echo "========================================"
    echo "    go-db Benchmarking Suite"
    echo "========================================"
    echo ""
    
    # Check prerequisites
    print_status "Checking prerequisites..."
    
    if ! command_exists k6; then
        print_error "k6 is not installed. Please install k6 first."
        echo "Installation instructions:"
        echo "  macOS: brew install k6"
        echo "  Linux: https://k6.io/docs/getting-started/installation/"
        exit 1
    fi
    
    if ! command_exists wrk; then
        print_error "wrk is not installed. Please install wrk first."
        echo "Installation instructions:"
        echo "  macOS: brew install wrk"
        echo "  Linux: apt-get install wrk"
        exit 1
    fi
    
    if ! command_exists docker; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command_exists nc; then
        print_error "netcat (nc) is not installed. Please install netcat first."
        exit 1
    fi
    
    print_success "All prerequisites are installed"
    echo ""
    
    # Check if go-db container is running
    print_status "Checking if go-db container is running..."
    
    if ! check_port; then
        print_warning "go-db container is not running on port 8080"
        print_status "Please start your go-db container first:"
        echo "  docker run -p 8080:8080 -v \$(pwd)/data:/app/data go-db"
        echo ""
        print_status "Waiting for you to start the container..."
        wait_for_service || exit 1
    else
        print_success "go-db container is running"
    fi
    
    echo ""
    
    # Make wrk scripts executable
    print_status "Making wrk scripts executable..."
    chmod +x wrk-*.sh
    print_success "wrk scripts are now executable"
    echo ""
    
    # Run k6 tests
    print_status "Starting k6 performance tests..."
    echo "========================================"
    
    local k6_tests=(
        "crud-load-test.js:CRUD Load Test"
        "batch-operations-test.js:Batch Operations Test"
        "index-performance-test.js:Index Performance Test"
        "streaming-test.js:Streaming Performance Test"
        "stress-test.js:Stress Test"
    )
    
    local k6_failed=0
    for test in "${k6_tests[@]}"; do
        IFS=':' read -r test_file test_name <<< "$test"
        if ! run_k6_test "$test_file" "$test_name"; then
            k6_failed=$((k6_failed + 1))
        fi
    done
    
    echo ""
    
    # Run wrk tests
    print_status "Starting wrk performance tests..."
    echo "========================================"
    
    local wrk_tests=(
        "wrk-insert.sh:Insert Performance"
        "wrk-find.sh:Find Performance"
        "wrk-update.sh:Update Performance"
    )
    
    local wrk_failed=0
    for test in "${wrk_tests[@]}"; do
        IFS=':' read -r test_script test_name <<< "$test"
        if ! run_wrk_test "$test_script" "$test_name"; then
            wrk_failed=$((wrk_failed + 1))
        fi
    done
    
    echo ""
    
    # Summary
    echo "========================================"
    echo "           Test Summary"
    echo "========================================"
    
    local total_k6_tests=${#k6_tests[@]}
    local total_wrk_tests=${#wrk_tests[@]}
    local total_tests=$((total_k6_tests + total_wrk_tests))
    local total_failed=$((k6_failed + wrk_failed))
    local total_passed=$((total_tests - total_failed))
    
    echo "k6 Tests: $((total_k6_tests - k6_failed))/$total_k6_tests passed"
    echo "wrk Tests: $((total_wrk_tests - wrk_failed))/$total_wrk_tests passed"
    echo "Total: $total_passed/$total_tests tests passed"
    echo ""
    
    if [ $total_failed -eq 0 ]; then
        print_success "All tests completed successfully! 🎉"
        exit 0
    else
        print_error "$total_failed test(s) failed"
        exit 1
    fi
}

# Run main function
main "$@"
