# GO-DB

A minimal Mongo-like database written in Go, optimized for speed and efficiency with advanced indexing and streaming capabilities.

## Features

- **Fast Document Storage**: MessagePack + LZ4 compression for optimal performance
- **Collections & Documents**: MongoDB-like document structure with flexible schemas
- **REST API**: Comprehensive HTTP endpoints for CRUD operations
- **Advanced Indexing**: Create and manage indexes for improved query performance
- **Streaming Support**: Memory-efficient document streaming for large datasets
- **In-Memory + Persistent**: Fast in-memory operations with disk persistence
- **Docker Ready**: Containerized deployment
- **Graceful Shutdown**: Automatic data persistence on shutdown

## Performance

- **~2.2x faster** than JSON storage (6.7ms vs 14.5ms for 1000 documents)
- **~8x smaller** file sizes (87% space savings)
- **50% fewer allocations** during serialization
- **~5.2M documents/second** streaming throughput
- **Indexed queries** framework in place (implementation in progress)

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

### Data Safety

**✅ Improved:** By default, data is automatically saved to disk after every write transaction (insert, update, delete), providing immediate persistence and data safety.

#### Persistence Options

1. **Transaction Saves (Default)**: Data saved after every write operation

   ```bash
   # Default behavior - transaction saves enabled
   go run cmd/go-db.go

   # Explicitly disable transaction saves
   go run cmd/go-db.go -transaction-save=false
   ```

2. **Background Saves**: Data saved periodically on a timer

   ```bash
   # Auto-save every 5 minutes (disables transaction saves for performance)
   go run cmd/go-db.go -background-save 5m

   # Auto-save every 30 seconds
   go run cmd/go-db.go -background-save 30s
   ```

**Note:** Background saves automatically disable transaction saves for better performance. Choose the option that best fits your use case:

- **Transaction saves**: Best for data consistency and immediate persistence
- **Background saves**: Best for high-throughput scenarios where some data loss is acceptable

## Batch Operations

For high-performance scenarios, go-db supports batch operations that can process up to 1000 documents in a single request:

- **Batch Insert**: Insert multiple documents simultaneously with automatic ID generation
- **Batch Update**: Update multiple documents by ID with partial failures supported
- **Performance**: Batch operations are typically 2-3x faster than individual operations
- **Atomicity**: Each operation within a batch is processed independently (partial success possible)
- **Limits**: Maximum 1000 documents/operations per batch request

**Use Cases:**

- Data migrations and bulk imports
- Periodic data synchronization
- High-throughput data processing
- ETL pipeline endpoints

## API Reference

### Collection Operations

#### Insert Document

```http
POST /collections/{collection}/insert
Content-Type: application/json

{
  "name": "Alice",
  "age": 30,
  "email": "alice@example.com"
}
```

**Response**: `201 Created` with document ID

#### Batch Insert Documents

Insert up to 1000 documents in a single request for improved performance:

```http
POST /collections/{collection}/batch/insert
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
  "collection": "users"
}
```

#### Find Documents

```http
GET /collections/{collection}/find
GET /collections/{collection}/find?age=30&city=New%20York
```

**Response**: `200 OK` with JSON array of documents

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

**⚠️ Important**: This endpoint does NOT apply pagination - it streams ALL matching documents. Use with caution for large datasets. For paginated queries, use the `/collections/{collection}/find` endpoint instead.

### Document Operations

#### Get Document by ID

```http
GET /collections/{collection}/documents/{id}
```

**Response**: `200 OK` with document JSON

#### Update Document

```http
PUT /collections/{collection}/documents/{id}
Content-Type: application/json

{
  "age": 31,
  "city": "Boston"
}
```

**Response**: `200 OK`

#### Batch Update Documents

Update up to 1000 documents in a single request using their IDs:

```http
PUT /collections/{collection}/batch/update
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

**Response**: `200 OK` (all successful) or `206 Partial Content` (some failures)

```json
{
  "success": true,
  "message": "Batch update completed successfully",
  "updated_count": 3,
  "failed_count": 0,
  "collection": "users"
}
```

For partial failures:

```json
{
  "success": true,
  "message": "Batch update completed with some errors",
  "updated_count": 2,
  "failed_count": 1,
  "collection": "users",
  "errors": ["operation 1: document with id 999 not found"]
}
```

#### Delete Document

```http
DELETE /collections/{collection}/documents/{id}
```

**Response**: `204 No Content`

### Index Operations

#### Create Index

```http
POST /collections/{collection}/indexes
Content-Type: application/json

{
  "field": "email"
}
```

**Response**: `201 Created`

#### Get Indexes

```http
GET /collections/{collection}/indexes
```

**Response**: `200 OK` with array of index field names

#### Drop Index

```http
DELETE /collections/{collection}/indexes/{field}
```

**Response**: `204 No Content`

## Usage Examples

### Basic CRUD Operations

```bash
# Insert a document
curl -X POST http://localhost:8080/collections/users/insert \
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

# Update document
curl -X PUT http://localhost:8080/collections/users/documents/1 \
  -H "Content-Type: application/json" \
  -d '{"age": 31}'

# Delete document
curl -X DELETE http://localhost:8080/collections/users/documents/1
```

### Indexing for Performance

```bash
# Create index on email field
curl -X POST http://localhost:8080/collections/users/indexes \
  -H "Content-Type: application/json" \
  -d '{"field": "email"}'

# Create index on age field
curl -X POST http://localhost:8080/collections/users/indexes \
  -H "Content-Type: application/json" \
  -d '{"field": "age"}'

# List all indexes
curl http://localhost:8080/collections/users/indexes

# Drop index
curl -X DELETE http://localhost:8080/collections/users/indexes/email
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
- **Storage Engine**: In-memory storage with LRU caching and persistence
- **Index Engine**: Optimized indexing for fast queries
- **Domain Layer**: Core business interfaces and types

## Docker

```bash
docker build -t go-db .
docker run -p 8080:8080 -v $(pwd)/data:/app/data go-db
```

## Advanced Features

For detailed information about the advanced storage engine features, see [ADVANCED_ENGINE.md](ADVANCED_ENGINE.md).
