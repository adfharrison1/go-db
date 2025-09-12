# go-db Benchmarking Suite

This directory contains comprehensive load testing scripts for the go-db database using k6 and wrk tools.

## Prerequisites

### Install k6

```bash
# macOS
brew install k6
```

### Install wrk

```bash
# macOS
brew install wrk

```

## Quick Start

1. **Start go-db with Docker Compose:**

   ```bash
   docker-compose up -d
   ```

2. **Run a basic test:**

   ```bash
   k6 run crud-load-test.js
   ```

3. **Run all tests:**

   ```bash
   ./run-all-tests.sh
   ```

## Docker

The benchmarking suite is designed to work with go-db running in Docker. Use Docker Compose for easy setup:

```bash
# Start go-db
docker-compose up -d

# Run benchmarks
k6 run crud-load-test.js

# Stop go-db
docker-compose down
```

### Managing Test Data

To start with a clean database between test runs:

```bash
# Stop the service and remove the volume (deletes all data)
docker-compose down -v

# Start fresh
docker-compose up -d
```

**Note**: The `-v` flag removes the Docker volume, which will delete all stored data. Use this when you want to start with a completely clean database for testing.

## Test Scripts

### k6 Tests

#### 1. CRUD Load Test (`crud-load-test.js`)

- **Purpose**: Tests basic CRUD operations under load
- **Duration**: 2 minutes
- **Users**: 10 concurrent users
- **Operations**: Insert → Get → Update → Find → Delete
- **Thresholds**: P95 < 100ms, Error rate < 10%

```bash
k6 run crud-load-test.js
```

#### 2. Batch Operations Test (`batch-operations-test.js`)

- **Purpose**: Tests batch insert and update operations
- **Duration**: 3 minutes
- **Users**: 5 concurrent users
- **Operations**: Batch insert (10-60 docs) → Batch update
- **Thresholds**: P95 < 200ms, Error rate < 5%

```bash
k6 run batch-operations-test.js
```

#### 3. Index Performance Test (`index-performance-test.js`)

- **Purpose**: Compares indexed vs non-indexed query performance on identical datasets
- **Duration**: 1 minute (15s ramp up, 30s steady, 15s ramp down)
- **Users**: 15 concurrent users
- **Dataset**: 100,000 documents in both indexed and non-indexed collections (inserted in 100 batches of 1,000)
- **Setup Time**: ~3-5 minutes for data insertion (setupTimeout: 5m)
- **Operations**: Same age queries on both collections for direct comparison
- **Thresholds**: P95 < 200ms, Error rate < 10%
- **Analysis**: Includes custom analysis script for detailed performance metrics

```bash
# Run test with analysis
k6 run index-performance-test.js 2>&1 | node analyze-results.js

# Run test without analysis (raw output)
k6 run index-performance-test.js
```

**What it tests:**

- Creates identical 100,000 document datasets in two collections (using 100 batches of 1,000 documents each)
- Creates an age index on one collection only
- Runs the same age queries on both collections
- **Validates data consistency**: Ensures both queries return exactly the same documents
- Compares response times to measure index effectiveness (only for validated queries)
- Shows win/loss statistics, speedup ranges, and performance by result count

#### 4. Streaming Performance Test (`streaming-test.js`)

- **Purpose**: Tests streaming endpoint performance with large datasets
- **Duration**: 2 minutes
- **Users**: 8 concurrent users
- **Operations**: Streaming queries, filtered streaming
- **Thresholds**: P95 < 500ms, Error rate < 10%

```bash
k6 run streaming-test.js
```

#### 5. Configuration Comparison Testing

**Configuration Testing:**

We provide testing of different database configurations to help you choose the right setup for your use case.

**Available Test Configurations:**

| Configuration  | Mode          | Throughput | P95 Latency | Success Rate | Best For                                    |
| -------------- | ------------- | ---------- | ----------- | ------------ | ------------------------------------------- |
| **Dual-Write** | Memory + Disk | ~84 req/s  | ~3.76s      | 100%         | Production systems requiring zero data loss |
| **No-Saves**   | Memory Only   | ~299 req/s | ~445ms      | 100%         | Caching, temporary data, benchmarking       |

**📊 Performance Insights:**

- **Dual-write mode** provides maximum data safety with immediate disk persistence (~84 req/s, ~3.76s P95)
- **No-saves mode** provides maximum performance with memory-only operations (~299 req/s, ~445ms P95)
- **100% success rate** in both modes (no eventual consistency issues)
- **Background retry queue** handles failed disk writes automatically in dual-write mode
- **3.6x throughput improvement** in no-saves mode vs dual-write mode
- **8.4x latency improvement** in no-saves mode vs dual-write mode

**Quick Single Configuration Test:**

```bash
# Test dual-write mode (default)
docker-compose up -d
k6 run stress-test-optimized.js
docker-compose down -v

# Test no-saves mode (maximum performance)
docker-compose run --rm go-db -no-saves -port 8080 &
k6 run stress-test-optimized.js
docker-compose down
```

**Manual Testing:**

```bash
# Start dual-write mode
docker-compose up -d

# Run stress test
k6 run stress-test-optimized.js

# Stop and clean up
docker-compose down -v
```

**What the comparison script does:**

- Tests all 4 configurations with identical workloads
- Uses isolated volumes for clean test environments
- Captures detailed performance metrics
- Generates comparison reports
- Saves results in `config-comparison-results/` directory

**⚠️ Understanding Success Rates:**

The success rates below 100% in background/no-save configurations are **expected behavior** due to eventual consistency:

- **Write operations**: Always succeed (documents created in memory)
- **Read operations**: May fail with 404 if document hasn't been persisted yet
- **Lower success rates**: Indicate more aggressive caching vs persistence trade-offs
- **This is not a bug**: It's the expected behavior of eventually consistent systems

For applications requiring 100% read consistency, use transaction saves. For high-performance applications that can handle occasional 404s on recently created data, background saves provide excellent performance.

**Individual Stress Tests:**

For reference, we also provide individual stress test files:

- **`stress-test.js`**: Basic stress test (4 operation types, 50/50 read/write ratio)
- **`stress-test-optimized.js`**: Optimized workload (6 operation types, 67/33 read/write ratio, indexed queries)

The comparison script uses the optimized test for more realistic performance evaluation.

### wrk Tests

#### 1. Insert Performance (`wrk-insert.sh`)

- **Purpose**: High-throughput insert testing
- **Threads**: 12
- **Connections**: 400
- **Duration**: 30 seconds

```bash
chmod +x wrk-insert.sh
./wrk-insert.sh
```

#### 2. Find Performance (`wrk-find.sh`)

- **Purpose**: High-throughput find testing
- **Threads**: 12
- **Connections**: 400
- **Duration**: 30 seconds

```bash
chmod +x wrk-find.sh
./wrk-find.sh
```

#### 3. Update Performance (`wrk-update.sh`)

- **Purpose**: High-throughput update testing
- **Threads**: 12
- **Connections**: 400
- **Duration**: 30 seconds

```bash
chmod +x wrk-update.sh
./wrk-update.sh
```

## Running All Tests

### Automated Test Suite

```bash
chmod +x run-all-tests.sh
./run-all-tests.sh
```

This script will:

1. Check prerequisites (k6, wrk, docker)
2. Start go-db container
3. Wait for container to be ready
4. Run all k6 tests sequentially
5. Run all wrk tests
6. Generate a summary report
7. Clean up containers

### Manual Test Execution

#### Run k6 tests with custom options:

```bash
# Run with JSON output
k6 run --out json=results.json crud-load-test.js

# Run with custom duration
k6 run --duration 5m crud-load-test.js

# Run with custom VUs (virtual users)
k6 run --vus 50 crud-load-test.js

# Run with custom stages
k6 run --stage 30s:20,1m:20,30s:0 crud-load-test.js
```

#### Run wrk tests with custom options:

```bash
# Custom threads and connections
wrk -t16 -c800 -d60s -s insert.lua http://localhost:8080/collections/test

# Custom duration
wrk -t12 -c400 -d2m -s find.lua http://localhost:8080/collections/test/find
```

## Understanding Results

### k6 Output Metrics

- **http_req_duration**: Response time statistics

  - `avg`: Average response time
  - `min`: Minimum response time
  - `max`: Maximum response time
  - `p(95)`: 95th percentile response time
  - `p(99)`: 99th percentile response time

- **http_req_failed**: Failed request rate
- **http_reqs**: Total requests and rate
- **vus**: Virtual users (concurrent users)
- **iterations**: Total test iterations

### wrk Output Metrics

- **Requests/sec**: Throughput (requests per second)
- **Transfer/sec**: Data transfer rate
- **Latency**: Response time statistics
  - `avg`: Average latency
  - `stdev`: Standard deviation
  - `max`: Maximum latency
  - `+/- stdev`: 68% of requests within this range

### Index Performance Analysis (`analyze-results.js`)

The index performance test includes a custom analysis script that processes k6 output and provides detailed statistics:

```bash
# Run with analysis
k6 run index-performance-test.js 2>&1 | node analyze-results.js
```

**Analysis Output:**

- **Summary Statistics**: Win/loss ratios, total comparisons
- **Performance Metrics**: Average response times, speedup calculations
- **Range Analysis**: Min/max response times and speedup ranges
- **Best/Worst Cases**: Specific examples of index performance
- **Performance by Result Count**: How speedup varies with query selectivity

**Example Output:**

```
📊 SUMMARY STATISTICS:
   Total comparisons: 4530
   Indexed wins: 2453 (54.2%)
   Non-indexed wins: 2033 (44.9%)
   Ties: 44 (1.0%)

⚡ PERFORMANCE METRICS:
   Average indexed query time: 23.67ms
   Average non-indexed query time: 24.32ms
   Average speedup: 1.26x

🏆 BEST INDEX PERFORMANCE:
   Age 53: 14.07x speedup
   Indexed: 10.40ms (32 results)
   Non-indexed: 146.39ms (16 results)
```

## Performance Expectations

### Baseline Performance Targets

| Operation               | Target RPS | Target P95 Latency | Target Error Rate |
| ----------------------- | ---------- | ------------------ | ----------------- |
| Single Insert           | > 1,000    | < 50ms             | < 1%              |
| Single Find             | > 2,000    | < 20ms             | < 1%              |
| Single Update           | > 1,000    | < 50ms             | < 1%              |
| Batch Insert (100 docs) | > 100      | < 200ms            | < 1%              |
| Batch Update (100 docs) | > 100      | < 200ms            | < 1%              |
| Indexed Query           | > 5,000    | < 10ms             | < 1%              |
| Streaming (1000 docs)   | > 50       | < 500ms            | < 1%              |

### Resource Usage Targets

- **Memory**: < 100MB baseline, < 500MB under load
- **CPU**: < 50% average utilization
- **Disk I/O**: Minimal during normal operations

## Troubleshooting

### Common Issues

1. **Connection refused errors**

   - Ensure go-db is running on port 8080
   - Test connectivity: `curl http://localhost:8080/collections/test/find`

2. **High error rates**

   - Reduce concurrent users in test scripts
   - Check system resource usage

3. **Slow response times**

   - Check if indexes are created for test collections
   - Monitor disk I/O during persistence operations
   - Consider adjusting persistence settings

4. **Test failures**
   - Ensure test data is properly set up
   - Check for collection name conflicts
   - Verify API endpoint availability

### Debug Mode

Run tests with verbose output:

```bash
k6 run --verbose crud-load-test.js
```

### Container Monitoring

Monitor container resources during tests:

```bash
# In another terminal
docker stats <container_id>
```

## Customization

### Modifying Test Parameters

Edit the `options` object in k6 scripts:

```javascript
export const options = {
  stages: [
    { duration: '30s', target: 10 }, // Ramp up
    { duration: '1m', target: 10 }, // Stay
    { duration: '30s', target: 0 }, // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<100'], // Adjust latency threshold
    http_req_failed: ['rate<0.1'], // Adjust error rate threshold
  },
};
```

### Adding Custom Metrics

```javascript
import { Rate, Trend } from 'k6/metrics';

const customErrorRate = new Rate('custom_errors');
const customTrend = new Trend('custom_trend');

export default function () {
  // Your test logic
  customErrorRate.add(1); // Add error
  customTrend.add(responseTime); // Add timing
}
```

## Contributing

When adding new tests:

1. Follow the naming convention: `{test-type}-test.js`
2. Include proper setup/teardown if needed
3. Set appropriate thresholds
4. Add documentation to this README
5. Update the `run-all-tests.sh` script if needed

## License

This benchmarking suite is part of the go-db project and follows the same license terms.
