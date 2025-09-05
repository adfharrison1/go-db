# Load Testing Scripts

This directory contains scripts for load testing your go-db server.

## insert_docs_load.go

A load testing script that inserts random user documents into your running go-db server.

### Usage

```bash
# Basic usage - insert 1000 users to localhost:8080
go run test_scripts/insert_docs_load.go 1000

# Specify a different server URL
go run test_scripts/insert_docs_load.go 500 http://localhost:9090

# Small test - insert 10 users
go run test_scripts/insert_docs_load.go 10
```

### Generated Data

The script generates random users with:

- **Name**: Random 6-letter name (e.g., "Abcdef")
- **Age**: Random age between 18 and 99
- **Email**: Format `<name>@example.com` (e.g., "Abcdef@example.com")

### Output

The script provides real-time progress updates and final statistics including:

- Success/failure counts
- Insert rate (users per second)
- Total time taken
- Success percentage

### Prerequisites

- Your go-db server must be running (default: `http://localhost:8080`)
- The `/collections/users/insert` endpoint must be available
- The server should accept JSON POST requests with user data

### Example Output

```
Starting load test: inserting 1000 users to http://localhost:8080
Press Ctrl+C to stop early
Progress: 100/1000 users (10.0%) - Rate: 234.5 users/sec - Success: 100, Errors: 0
Progress: 200/1000 users (20.0%) - Rate: 241.2 users/sec - Success: 200, Errors: 0
...
============================================================
LOAD TEST COMPLETE
============================================================
Total users attempted: 1000
Successful inserts:    1000
Failed inserts:        0
Success rate:          100.00%
Total time:            4.231s
Average rate:          236.32 users/sec
Average time per user: 4.231ms

Load test completed successfully!
```
