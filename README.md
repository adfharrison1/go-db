# GO-DB

A minimal Mongo-like database written in Go, optimized for speed and efficiency with advanced indexing, streaming capabilities, and production-ready concurrency.

## Features

- **Fast Document Storage**: MessagePack + LZ4 compression for optimal performance
- **Collections & Documents**: MongoDB-like document structure with flexible schemas
- **REST API**: Comprehensive HTTP endpoints for CRUD operations
- **Advanced Indexing**: Create and manage indexes for improved query performance
- **Streaming Support**: Memory-efficient document streaming for large datasets
- **High Concurrency**: Three-level hybrid locking for maximum concurrent throughput
- **Thread Safety**: Production-ready concurrent read/write operations
- **In-Memory + Persistent**: Fast in-memory operations with disk persistence
- **Docker Ready**: Containerized deployment
- **Graceful Shutdown**: Automatic data persistence on shutdown

## Performance

- **~2.2x faster** than JSON storage (6.7ms vs 14.5ms for 1000 documents)
- **~8x smaller** file sizes (87% space savings)
- **50% fewer allocations** during serialization
- **~6.4M documents/second** streaming throughput
- **High-concurrency reads**: Multiple concurrent readers per collection
- **Fine-grained updates**: Document-level locking minimizes contention
- **Thread-safe indexing**: Concurrent index operations with RWMutex protection

## Getting Started

```bash
git clone https://github.com/adfharrison/go-db.git
cd go-db
go mod tidy
go run cmd/go-db.go
```

The server will start on `:8080` and automatically load/save data to `go-db_data.godb`.

## Command Line Options

go-db supports various configuration options via command line flags:

```bash
# Basic usage
go run cmd/go-db.go

# Custom port and memory settings
go run cmd/go-db.go -port 9090 -max-memory 2048

# Enable background auto-save every 5 minutes (recommended for production)
go run cmd/go-db.go -background-save 5m

# Custom data directory and file
go run cmd/go-db.go -data-dir /var/lib/go-db -data-file myapp.godb

# Show all options
go run cmd/go-db.go -help
```

### Available Options

| Flag          | Default           | Description                                   |
| ------------- | ----------------- | --------------------------------------------- |
| `-port`       | `8080`            | Server port                                   |
| `-data-file`  | `go-db_data.godb` | Data file path for persistence                |
| `-data-dir`   | `.`               | Data directory for storage                    |
| `-max-memory` | `1024`            | Maximum memory usage in MB                    |
| `-no-saves`   | `false`           | Disable automatic disk writes (shutdown only) |
| `-help`       | `false`           | Show help message                             |

### Data Safety & Performance Modes

GO-DB offers two operational modes that balance **data safety** vs **performance** based on your requirements.

#### 🔒 **Dual-Write Mode (Default - Maximum Safety)**

**Configuration:**

```bash
# Default behavior - dual-write mode enabled
go run cmd/go-db.go

# Docker
docker-compose up  # Uses dual-write mode by default
```

**Characteristics:**

- ✅ **Immediate Persistence**: Every write operation saves to memory AND disk immediately
- ✅ **Zero Data Loss**: Guaranteed data consistency across restarts
- ✅ **100% Success Rate**: No eventual consistency issues
- ✅ **Background Retry**: Failed disk writes are queued and retried automatically
- ⚠️ **Disk I/O Overhead**: Each write operation includes disk I/O

**Performance Metrics** (100 concurrent users, 1m45s stress test):

- **Throughput**: ~84 requests/second
- **P95 Response Time**: ~3.76s
- **Average Response Time**: ~668ms
- **Success Rate**: 100% (zero failures)
- **Use Case**: Production systems, critical data, financial applications

#### ⚡ **No-Saves Mode (Maximum Performance)**

**Configuration:**

```bash
# No automatic saves - data only saved on graceful shutdown
go run cmd/go-db.go -no-saves

# Docker
docker-compose run --rm go-db -no-saves -port 8080
```

**Characteristics:**

- 🚀 **Maximum Performance**: No disk I/O during operations
- 📈 **High Concurrency**: Excellent scaling under load
- ⚠️ **Data Loss Risk**: Data only saved on graceful shutdown (SIGINT/SIGTERM)
- 🔄 **Memory Only**: All operations happen in memory

**Performance Metrics** (100 concurrent users, 1m45s stress test):

- **Throughput**: ~299 requests/second
- **P95 Response Time**: ~445ms
- **Average Response Time**: ~112ms
- **Success Rate**: 100% (memory operations)
- **Use Case**: Caching layers, temporary analytics, high-performance scenarios

#### 📊 **Mode Comparison Summary**

| Mode           | Throughput | P95 Latency | Success Rate | Data Safety | Best For                    |
| -------------- | ---------- | ----------- | ------------ | ----------- | --------------------------- |
| **Dual-Write** | ~84 req/s  | ~3.76s      | 100%         | Maximum     | Production, critical data   |
| **No-Saves**   | ~299 req/s | ~445ms      | 100%         | Minimal     | Caching, analytics, testing |

#### 🎯 **Choosing the Right Mode**

**Use Dual-Write Mode when:**

- Data loss is unacceptable
- Financial or critical business data
- Production systems
- Regulatory compliance requirements
- You need immediate persistence

**Use No-Saves Mode when:**

- Maximum performance is critical
- Data can be recreated from other sources
- Analytics and reporting workloads
- Caching scenarios
- Testing and development environments

## Batch Operations

For high-performance scenarios, go-db supports **atomic batch operations** that can process up to 1000 documents in a single request:

- **Batch Insert**: Insert multiple documents simultaneously with automatic ID generation
- **Batch Update**: Update multiple documents by ID with full atomicity guarantees
- **Performance**: Batch operations are typically 2-3x faster than individual operations
- **Atomicity**: All operations succeed or all fail (no partial success)
- **Data Integrity**: Complete rollback on any validation failure
- **Limits**: Maximum 1000 documents/operations per batch request

**Use Cases:**

- Data migrations and bulk imports
- Periodic data synchronization
- High-throughput data processing
- ETL pipeline endpoints
- Critical data operations requiring consistency

## API Reference

### Collection Operations

#### Insert Document

```http
POST /collections/{collection}
Content-Type: application/json

{
  "name": "Alice",
  "age": 30,
  "email": "alice@example.com"
}
```

**Response**: `201 Created` with the created document (including generated `_id`)

#### Batch Insert Documents

Insert up to 1000 documents in a single request for improved performance:

```http
POST /collections/{collection}/batch
Content-Type: application/json

{
  "documents": [
    {
      "name": "Alice",
      "age": 30,
      "email": "alice@example.com"
    },
    {
      "name": "Bob",
      "age": 25,
      "email": "bob@example.com"
    },
    {
      "name": "Charlie",
      "age": 35,
      "email": "charlie@example.com"
    }
  ]
}
```

**Response**: `201 Created`

```json
{
  "success": true,
  "message": "Batch insert completed successfully",
  "inserted_count": 3,
  "collection": "users",
  "documents": [
    {
      "_id": "1",
      "name": "Alice",
      "age": 30,
      "email": "alice@example.com"
    },
    {
      "_id": "2",
      "name": "Bob",
      "age": 25,
      "email": "bob@example.com"
    },
    {
      "_id": "3",
      "name": "Charlie",
      "age": 35,
      "email": "charlie@example.com"
    }
  ]
}
```

#### Find Documents

```http
GET /collections/{collection}/find
GET /collections/{collection}/find?age=30&city=New%20York
```

**Response**: `200 OK` with JSON array of documents

**Index Optimization**: The find endpoints automatically use indexes when available for faster queries. If you create an index on a field (e.g., `age`), queries filtering by that field will use the index for optimal performance. Multiple indexes are combined using AND logic for compound queries.

#### Find Documents with Pagination

The database supports both **offset/limit** and **cursor-based** pagination for efficient data retrieval:

**Offset/Limit Pagination:**

```http
GET /collections/{collection}/find?limit=10&offset=20
GET /collections/{collection}/find?age=30&limit=5&offset=0
```

**Cursor-Based Pagination:**

```http
GET /collections/{collection}/find?limit=10&after=eyJpZCI6IjEwIiwidGltZXN0YW1wIjoiMjAyNS0wNy0xM1QxOTo0NDoyMS4yNzc3ODkrMDE6MDAifQ==
GET /collections/{collection}/find?limit=10&before=eyJpZCI6IjIwIiwidGltZXN0YW1wIjoiMjAyNS0wNy0xM1QxOTo0NDoyMS4yNzc3ODkrMDE6MDAifQ==
```

**Pagination Response Format:**

```json
{
  "documents": [...],
  "hasNext": true,
  "hasPrev": false,
  "total": 100,
  "nextCursor": "eyJpZCI6IjEwIiwidGltZXN0YW1wIjoiMjAyNS0wNy0xM1QxOTo0NDoyMS4yNzc3ODkrMDE6MDAifQ==",
  "prevCursor": null
}
```

**⚠️ Note**: Cannot mix cursor-based (`after`/`before`) and offset-based (`offset`) pagination in the same request.

#### Find Documents with Streaming

```http
GET /collections/{collection}/find_with_stream
GET /collections/{collection}/find_with_stream?age=30
```

**Response**: `200 OK` with chunked JSON array (memory efficient for large datasets)

**Index Optimization**: Like the regular find endpoint, streaming also uses indexes when available for optimal performance.

**⚠️ Important**: This endpoint does NOT apply pagination - it streams ALL matching documents. Use with caution for large datasets. For paginated queries, use the `/collections/{collection}/find` endpoint instead.

### Document Operations

#### Get Document by ID

```http
GET /collections/{collection}/documents/{id}
```

**Response**: `200 OK` with document JSON

#### Update Document (Partial)

```http
PATCH /collections/{collection}/documents/{id}
Content-Type: application/json

{
  "age": 31,
  "city": "Boston"
}
```

**Response**: `200 OK` with the full updated document

```json
{
  "_id": "1",
  "name": "Alice",
  "age": 31,
  "city": "Boston",
  "email": "alice@example.com"
}
```

#### Replace Document

```http
PUT /collections/{collection}/documents/{id}
Content-Type: application/json

{
  "name": "Alice Smith",
  "age": 32,
  "position": "Senior Developer",
  "salary": 95000
}
```

**Response**: `200 OK` with the completely replaced document

```json
{
  "_id": "1",
  "name": "Alice Smith",
  "age": 32,
  "position": "Senior Developer",
  "salary": 95000
}
```

**Note**: PUT completely replaces the document content, while PATCH performs partial updates. All existing fields not included in the PUT request will be removed.

#### Batch Update Documents

Update up to 1000 documents in a single request using their IDs:

```http
PATCH /collections/{collection}/batch
Content-Type: application/json

{
  "operations": [
    {
      "id": "1",
      "updates": {
        "age": 31,
        "salary": 75000,
        "department": "Senior Engineering"
      }
    },
    {
      "id": "2",
      "updates": {
        "age": 26,
        "salary": 60000,
        "position": "Sales Manager"
      }
    },
    {
      "id": "3",
      "updates": {
        "active": false,
        "end_date": "2024-12-31"
      }
    }
  ]
}
```

**Response**: `200 OK` (all successful) or `500 Internal Server Error` (any failures)

```json
{
  "success": true,
  "message": "Batch update completed successfully",
  "updated_count": 3,
  "failed_count": 0,
  "collection": "users",
  "documents": [
    {
      "_id": "1",
      "name": "Alice",
      "age": 31,
      "salary": 75000,
      "department": "Senior Engineering"
    },
    {
      "_id": "2",
      "name": "Bob",
      "age": 26,
      "salary": 60000,
      "position": "Sales Manager"
    },
    {
      "_id": "3",
      "name": "Charlie",
      "active": false,
      "end_date": "2024-12-31"
    }
  ]
}
```

**Atomic Behavior**: Batch updates are atomic - either all operations succeed or none are applied. If any document in the batch doesn't exist or any operation fails, the entire batch operation fails and no changes are made.

**Error Response**: If any operation fails, the API returns `500 Internal Server Error` with an error message indicating which operation failed.

#### Delete Document

```http
DELETE /collections/{collection}/documents/{id}
```

**Response**: `204 No Content`

### Index Operations

#### Create Index

```http
POST /collections/{collection}/indexes/{field}
```

**Response**: `201 Created`

```json
{
  "success": true,
  "message": "Index created successfully",
  "collection": "users",
  "field": "email"
}
```

**Note**: The `_id` field is automatically indexed and cannot be manually indexed.

**Error Responses**:

- `400 Bad Request`: Field name is required or trying to index `_id` field
- `500 Internal Server Error`: Index creation failed

#### Get Indexes

```http
GET /collections/{collection}/indexes
```

**Response**: `200 OK`

```json
{
  "success": true,
  "collection": "users",
  "indexes": ["_id", "email", "age"],
  "index_count": 3
}
```

**Note**: The `_id` field is automatically indexed and will always appear in the indexes list for existing collections.

## Usage Examples

### Basic CRUD Operations

```bash
# Insert a document
curl -X POST http://localhost:8080/collections/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "age": 30, "email": "alice@example.com"}'

# Find all documents
curl http://localhost:8080/collections/users/find

# Find documents with filter
curl "http://localhost:8080/collections/users/find?age=30&city=New%20York"

# Find documents with offset/limit pagination
curl "http://localhost:8080/collections/users/find?limit=10&offset=20"

# Find documents with cursor-based pagination
curl "http://localhost:8080/collections/users/find?limit=10&after=eyJpZCI6IjEwIiwidGltZXN0YW1wIjoiMjAyNS0wNy0xM1QxOTo0NDoyMS4yNzc3ODkrMDE6MDAifQ=="

# Get document by ID
curl http://localhost:8080/collections/users/documents/1

# Update document (partial)
curl -X PATCH http://localhost:8080/collections/users/documents/1 \
  -H "Content-Type: application/json" \
  -d '{"age": 31}'

# Replace document
curl -X PUT http://localhost:8080/collections/users/documents/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith", "age": 32, "position": "Senior Developer"}'

# Delete document
curl -X DELETE http://localhost:8080/collections/users/documents/1
```

### Indexing for Performance

```bash
# Create index on email field
curl -X POST http://localhost:8080/collections/users/indexes/email

# Create index on age field
curl -X POST http://localhost:8080/collections/users/indexes/age

# Get all indexes for a collection
curl http://localhost:8080/collections/users/indexes

# Note: _id field is automatically indexed and cannot be manually indexed
```

### Streaming Large Datasets

```bash
# Stream all documents (memory efficient, no pagination)
curl http://localhost:8080/collections/users/find_with_stream

# Stream with filters (no pagination)
curl "http://localhost:8080/collections/users/find_with_stream?age=30"

# ⚠️ Warning: These endpoints stream ALL matching documents
# For paginated queries, use the /find endpoint instead
```

## Testing

### Unit Tests

Run all tests:

```bash
go test ./...
```

Run tests with verbose output:

```bash
go test ./... -v
```

Run specific test packages:

```bash
go test ./pkg/storage/... -v
go test ./pkg/api/... -v
go test ./pkg/indexing/... -v
```

Run benchmarks:

```bash
go test ./pkg/storage/... -bench=.
```

### Load Testing

A load testing script is available to test insert performance in test_scripts

## Storage Format

The database uses a custom binary format with:

- **Header**: 8-byte magic identifier (`GODB`) + version info
- **Data**: LZ4-compressed MessagePack serialization
- **File Extension**: `.godb`
- **Indexes**: Separate storage with optimized lookup structures

## Architecture

The database follows a clean architecture with separated concerns:

- **API Layer**: HTTP handlers with dependency injection
- **Storage Engine**: In-memory storage with hybrid locking, persistence, and concurrency control
- **Index Engine**: Thread-safe optimized indexing for fast queries
- **Domain Layer**: Core business interfaces and types

### Concurrency & Thread Safety

GO-DB implements a **hybrid locking strategy** for optimal performance under concurrent load:

#### **Three-Level Locking Architecture:**

1. **Collection-Level Read Locks** 📖

   - Used by: `GetById`, `FindAll`, streaming operations
   - Coordinate with structural write operations
   - Allow multiple concurrent readers

2. **Collection-Level Write Locks** ✍️

   - Used by: `Insert`, `Delete`, `BatchInsert`, persistence operations
   - Protect map structure modifications
   - Serialize with all other collection access

3. **Document-Level Locks** 🔒
   - Used by: `Update`, `Replace`, `BatchUpdate`
   - Protect individual document content
   - Enable fine-grained concurrency for content modifications

#### **Thread-Safe Components:**

- ✅ **Storage Engine**: Hybrid collection + document-level locking
- ✅ **Index Engine**: RWMutex protection with concurrent read/write operations
- ✅ **Individual Indexes**: Per-index locking for inverted index operations
- ✅ **ID Generation**: Atomic counters for collision-free ID assignment
- ✅ **Persistence**: Coordinated with collection locks for safe concurrent saves

#### **Concurrency Benefits:**

- **High Read Throughput**: Multiple concurrent readers per collection
- **Efficient Updates**: Document-level locks minimize contention
- **Safe Structural Changes**: Collection locks prevent map corruption
- **Index Consistency**: Thread-safe index updates during document operations
- **Production Ready**: Handles real-world concurrent workloads

## Docker

### Quick Start (Production Mode)

```bash
# Start go-db with default configuration (transaction saves enabled)
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```

### Development & Testing Configurations

For development, testing, or performance evaluation, you can use different configurations:

**Available Configurations:**

| Configuration           | Command                                              | Mode       | Best For                  |
| ----------------------- | ---------------------------------------------------- | ---------- | ------------------------- |
| **Production**          | `docker-compose up -d`                               | Dual-Write | Production, data safety   |
| **Performance Testing** | `docker-compose run --rm go-db -no-saves -port 8080` | No-Saves   | High-performance, caching |

**Examples:**

```bash
# Production mode (dual-write, maximum safety)
docker-compose up -d

# High-performance testing (no-saves mode)
docker-compose run --rm go-db -no-saves -port 8080

# Custom memory limit
docker-compose run --rm go-db -max-memory=2048 -port 8080

# Stop the service
docker-compose down
```

**Custom Configuration:**

You can also run with custom flags:

```bash
# No-saves mode with custom memory limit
docker-compose run --rm go-db -no-saves -max-memory=2048 -port 8080

# Dual-write mode with custom data directory
docker-compose run --rm go-db -data-dir=/tmp/go-db -port 8080
```

### Data Persistence

- **Production mode** (`docker-compose up`): Uses `go-db-data` volume with dual-write persistence
- **Performance mode**: Uses temporary container storage (data lost on container stop)

Data persists between container restarts in production mode.

## Advanced Features

For detailed information about the advanced storage engine features, see [ADVANCED_ENGINE.md](ADVANCED_ENGINE.md).
