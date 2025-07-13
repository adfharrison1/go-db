# GO-DB

A minimal Mongo-like database written in Go, optimized for speed and efficiency.

## Features

- **Fast Document Storage**: MessagePack + LZ4 compression for optimal performance
- **Collections & Documents**: MongoDB-like document structure with flexible schemas
- **REST API**: Simple HTTP endpoints for CRUD operations
- **In-Memory + Persistent**: Fast in-memory operations with disk persistence
- **Docker Ready**: Containerized deployment
- **Graceful Shutdown**: Automatic data persistence on shutdown

## Performance

- **~2.2x faster** than JSON storage (6.7ms vs 14.5ms for 1000 documents)
- **~8x smaller** file sizes (87% space savings)
- **50% fewer allocations** during serialization

## Getting Started

```bash
git clone https://github.com/adfharrison/go-db.git
cd go-db
go mod tidy
go run cmd/go-db.go
```

The server will start on `:8080` and automatically load/save data to `go-db_data.godb`.

## API Endpoints

- `POST /collections/{name}/insert` - Insert a document
- `GET /collections/{name}/find` - Find all documents in collection

## Storage Format

The database uses a custom binary format with:

- **Header**: 8-byte magic identifier (`GODB`) + version info
- **Data**: LZ4-compressed MessagePack serialization
- **File Extension**: `.godb`

## Docker

```bash
docker build -t go-db .
docker run -p 8080:8080 -v $(pwd)/data:/app/data go-db
```
