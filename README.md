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

| Flag                | Default           | Description                                  |
| ------------------- | ----------------- | -------------------------------------------- |
| `-port`             | `8080`            | Server port                                  |
| `-data-file`        | `go-db_data.godb` | Data file path for persistence               |
| `-data-dir`         | `.`               | Data directory for storage                   |
| `-max-memory`       | `1024`            | Maximum memory usage in MB                   |
| `-background-save`  | `0` (disabled)    | Background save interval (e.g., `5m`, `30s`) |
| `-transaction-save` | `true`            | Save to disk after every write transaction   |
| `-help`             | `false`           | Show help message                            |

### Data Safety & Performance Modes

GO-DB offers different operational modes that balance **data safety** vs **performance** based on your requirements.

#### 🔒 **Transaction Saves Mode (Default - Maximum Safety)**

**Configuration:**

```bash
# Default behavior - transaction saves enabled
go run cmd/go-db.go

# Docker
docker-compose up  # Uses transaction saves by default
```

**Characteristics:**

- ✅ **Immediate Persistence**: Every write operation saves to disk
- ✅ **Zero Data Loss**: Guaranteed data consistency across restarts
- ✅ **ACID Compliance**: Full transactional integrity
- ⚠️ **Lower Throughput**: Disk I/O limits performance under high load

**Performance Metrics** (100 concurrent users, 5-minute stress test):

- **Throughput**: 70.8 requests/second
- **P95 Response Time**: 3.46s
- **Average Response Time**: 805ms
- **Success Rate**: 100% (zero failures)
- **Use Case**: Production systems requiring immediate persistence and zero data loss

#### ⚡ **High-Throughput Mode (Optimized Performance)**

**Configuration:**

```bash
# ⚠️ WARNING: No automatic saves - data only saved on graceful shutdown
go run cmd/go-db.go -transaction-save=false

# 🔧 RECOMMENDED: Add periodic saves for data safety
go run cmd/go-db.go -transaction-save=false -background-save=5m

# Docker (no automatic saves)
docker-compose run --rm go-db -transaction-save=false -port 8080
```

**Characteristics:**

- 🚀 **Maximum Performance**: No disk I/O bottlenecks
- 📈 **High Concurrency**: Excellent scaling under load
- ⚠️ **HIGH Data Loss Risk**: Data only saved on graceful shutdown (SIGINT/SIGTERM)
- 🔄 **Manual Configuration**: Must explicitly add `-background-save` for any automatic persistence

**Performance Metrics** (100 concurrent users, 5-minute stress test):

- **Throughput**: 487.8 requests/second (**6.9x higher**)
- **P95 Response Time**: 45ms (**98.7% faster**)
- **Average Response Time**: 10ms (**98.8% faster**)
- **Success Rate**: 49.2% (eventual consistency issues)
- **Use Case**: Caching layers, temporary analytics, testing environments (combine with `-background-save` for production)

#### 🕐 **Background Saves Mode (Balanced Approach)**

**Configuration:**

```bash
# Auto-save every 5 minutes (disables transaction saves)
go run cmd/go-db.go -background-save 5m

# Auto-save every 30 seconds
go run cmd/go-db.go -background-save 30s
```

**Characteristics:**

- ⚖️ **Balanced Performance**: Good throughput with periodic persistence
- 🛡️ **Limited Data Loss**: Only lose data since last background save
- ⏰ **Configurable Safety**: Adjust save interval based on requirements
- 🔄 **Automatic**: No manual intervention required

**Performance Metrics** (100 concurrent users, 5-minute stress test):

**Background Saves (1s interval):**

- **Throughput**: 514.8 requests/second (**7.3x higher than transaction saves**)
- **P95 Response Time**: 104ms (**97% faster**)
- **Average Response Time**: 23ms (**97.1% faster**)
- **Success Rate**: 72.5% (some eventual consistency issues)

**Background Saves (5s interval):**

- **Throughput**: 480.2 requests/second (**6.8x higher than transaction saves**)
- **P95 Response Time**: 35ms (**99% faster**)
- **Average Response Time**: 8ms (**99% faster**)
- **Success Rate**: 49.1% (more eventual consistency issues)

**Use Case**: Applications that can tolerate small amounts of data loss and occasional read inconsistencies for significantly better performance

#### 📊 **Mode Comparison Summary**

| Mode                  | Throughput  | P95 Latency | Success Rate | Data Safety | Best For                         |
| --------------------- | ----------- | ----------- | ------------ | ----------- | -------------------------------- |
| **Transaction Saves** | 70.8 req/s  | 3.46s       | 100%         | Maximum     | Financial systems, critical data |
| **Background 1s**     | 514.8 req/s | 104ms       | 72.5%        | High        | Web applications, development    |
| **Background 5s**     | 480.2 req/s | 35ms        | 49.1%        | Medium      | High-performance, fault-tolerant |
| **No Auto Saves**     | 487.8 req/s | 45ms        | 49.2%        | Minimal     | Caching, temporary analytics     |

**⚠️ Success Rate Notes:** Lower success rates in background/no-save modes are due to eventual consistency - read operations may encounter 404 errors when accessing recently created documents that haven't been persisted yet. This is expected behavior in high-throughput, eventually consistent systems.

#### 🎯 **Choosing the Right Mode**

**Use Transaction Saves when:**

- Data loss is unacceptable
- Financial or critical business data
- Regulatory compliance requirements
- Low to medium throughput requirements

**Use High-Throughput Mode when:**

- Maximum performance is critical
- Data can be recreated from other sources
- Analytics and reporting workloads
- Real-time applications with external persistence

**Use Background Saves when:**

- Balanced performance and safety needs
- Web applications with user-generated content
- Systems that can tolerate minimal data loss
- Development and testing environments

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

| Configuration              | Command                                                                  | Transaction Saves | Background Saves | Best For                          |
| -------------------------- | ------------------------------------------------------------------------ | ----------------- | ---------------- | --------------------------------- |
| **Production**             | `docker-compose up -d`                                                   | ✅ Enabled        | ❌ Disabled      | Production, data safety           |
| **Transaction Testing**    | `docker-compose -f docker-compose-configs.yml up -d go-db-transaction`   | ✅ Enabled        | ❌ Disabled      | Transaction save testing          |
| **Balanced (1s saves)**    | `docker-compose -f docker-compose-configs.yml up -d go-db-background-1s` | ❌ Disabled       | ✅ Every 1s      | Development, balanced performance |
| **Performance (5s saves)** | `docker-compose -f docker-compose-configs.yml up -d go-db-background-5s` | ❌ Disabled       | ✅ Every 5s      | High-performance scenarios        |
| **Pure Performance**       | `docker-compose -f docker-compose-configs.yml up -d go-db-no-saves`      | ❌ Disabled       | ❌ Disabled      | Benchmarking, caching             |

**Examples:**

```bash
# Development with balanced performance (1-second saves)
docker-compose -f docker-compose-configs.yml up -d go-db-background-1s

# High-performance testing (5-second saves)
docker-compose -f docker-compose-configs.yml up -d go-db-background-5s

# Pure performance testing (no automatic saves)
docker-compose -f docker-compose-configs.yml up -d go-db-no-saves

# Stop any configuration
docker-compose -f docker-compose-configs.yml down -v
```

**Custom Configuration:**

You can also run with custom flags:

```bash
# Custom background save interval
docker-compose run --rm go-db -background-save=30s -port 8080

# Disable transaction saves with custom memory limit
docker-compose run --rm go-db -transaction-save=false -max-memory=2048 -port 8080
```

### Data Persistence

- **Production mode** (`docker-compose up`): Uses `go-db-data` volume
- **Configuration testing**: Each config uses isolated volumes:
  - `go-db-transaction-data`
  - `go-db-background-1s-data`
  - `go-db-background-5s-data`
  - `go-db-no-saves-data`

Data persists between container restarts for all configurations.

## Advanced Features

For detailed information about the advanced storage engine features, see [ADVANCED_ENGINE.md](ADVANCED_ENGINE.md).
