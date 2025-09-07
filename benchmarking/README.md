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

- **Purpose**: Compares indexed vs non-indexed query performance
- **Duration**: 3 minutes
- **Users**: 15 concurrent users
- **Operations**: Indexed queries, non-indexed queries, compound queries
- **Thresholds**: P95 < 50ms, Error rate < 10%

```bash
k6 run index-performance-test.js
```

#### 4. Streaming Performance Test (`streaming-test.js`)

- **Purpose**: Tests streaming endpoint performance with large datasets
- **Duration**: 2 minutes
- **Users**: 8 concurrent users
- **Operations**: Streaming queries, filtered streaming
- **Thresholds**: P95 < 500ms, Error rate < 10%

```bash
k6 run streaming-test.js
```

#### 5. Stress Test (`stress-test.js`)

- **Purpose**: Tests system limits and failure points
- **Duration**: 5 minutes
- **Users**: Ramp from 0 to 100 users
- **Operations**: Mixed CRUD operations under high load
- **Thresholds**: P95 < 1000ms, Error rate < 20%

```bash
k6 run stress-test.js
```

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
