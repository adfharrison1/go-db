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
- **~6.3M documents/second** streaming throughput
- **Indexed queries** for sub-millisecond response times

## Getting Started

```bash
git clone https://github.com/adfharrison/go-db.git
cd go-db
go mod tidy
go run cmd/go-db.go
```

The server will start on `:8080` and automatically load/save data to `go-db_data.godb`.

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

#### Find Documents

```http
GET /collections/{collection}/find
GET /collections/{collection}/find?age=30&city=New%20York
```

**Response**: `200 OK` with JSON array of documents

#### Find Documents with Streaming

```http
GET /collections/{collection}/find_with_stream
GET /collections/{collection}/find_with_stream?age=30
```

**Response**: `200 OK` with chunked JSON array (memory efficient for large datasets)

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
# Stream all documents (memory efficient)
curl http://localhost:8080/collections/users/find_with_stream

# Stream with filters
curl "http://localhost:8080/collections/users/find_with_stream?age=30"
```

## Testing

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
