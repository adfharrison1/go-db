# GO-DB

A high-performance, production-ready document database written in Go with two storage engines: a traditional dual-write engine (V1) and an advanced Write-Ahead Logging engine (V2).

## 🚀 Quick Start

```bash
# Clone and run
git clone https://github.com/adfharrison/go-db.git
cd go-db
go mod tidy

# V1 Engine (Dual-Write) - Default
go run cmd/go-db.go

# V2 Engine (WAL) - Recommended
go run cmd/go-db.go -v2
```

The server starts on `:8080` with automatic data persistence.

## 🏗️ Storage Engines

GO-DB offers two storage engines optimized for different use cases:

### **V1 Engine: Dual-Write (Legacy)**

- **Architecture**: Traditional dual-write to memory + disk
- **Performance**: ~81 req/s, 3.65s P95 latency (Dual-Write) / ~458 req/s, 171ms P95 (No-Saves)
- **Safety**: Maximum data safety with immediate persistence
- **Use Case**: Legacy systems, maximum safety requirements

### **V2 Engine: Write-Ahead Logging (Recommended)**

- **Architecture**: WAL-based with automatic checkpointing
- **Performance**: ~250 req/s, 630ms P95 latency (**3x faster than Dual-Write**)
- **Safety**: ACID compliance with crash recovery
- **Use Case**: Production systems, high-performance applications

## 📊 Engine Comparison

| Feature               | V1 Dual-Write | V1 No-Saves | V2 WAL     | Winner         |
| --------------------- | ------------- | ----------- | ---------- | -------------- |
| **Write Performance** | ~81 req/s     | ~458 req/s  | ~250 req/s | 🏆 V1 No-Saves |
| **P95 Latency**       | ~3.65s        | ~171ms      | ~630ms     | 🏆 V1 No-Saves |
| **Memory Usage**      | 100%          | 100%        | 70-80%     | 🏆 V2          |
| **Data Safety**       | Maximum       | Minimal     | ACID       | 🤝 V1/V2       |
| **Recovery**          | Manual        | Manual      | Automatic  | 🏆 V2          |
| **Disk I/O**          | High          | None        | Low        | 🏆 V1 No-Saves |
| **Cleanup**           | Manual        | Manual      | Automatic  | 🏆 V2          |

## ⚙️ Command Line Options

### **Basic Options**

| Flag          | Default           | Description        | V1  | V2  |
| ------------- | ----------------- | ------------------ | --- | --- |
| `-port`       | `8080`            | Server port        | ✅  | ✅  |
| `-data-file`  | `go-db_data.godb` | Data file path     | ✅  | ❌  |
| `-data-dir`   | `.`               | Data directory     | ✅  | ✅  |
| `-max-memory` | `1024`            | Max memory (MB)    | ✅  | ✅  |
| `-no-saves`   | `false`           | Disable auto-saves | ✅  | ❌  |
| `-v2`         | `false`           | Use V2 WAL engine  | ❌  | ✅  |
| `-help`       | `false`           | Show help          | ✅  | ✅  |

### **V1-Specific Options**

```bash
# V1 Engine - Dual-Write Mode (Default)
go run cmd/go-db.go

# V1 Engine - No-Saves Mode (High Performance)
go run cmd/go-db.go -no-saves

# V1 Engine - Custom Configuration
go run cmd/go-db.go -port 9090 -max-memory 2048 -data-dir /var/lib/go-db
```

### **V2-Specific Options**

```bash
# V2 Engine - Default Configuration
go run cmd/go-db.go -v2

# V2 Engine - Custom Configuration
go run cmd/go-db.go -v2 -port 9090 -max-memory 2048 -data-dir /var/lib/go-db

# V2 Engine - High Performance
go run cmd/go-db.go -v2 -max-memory 4096
```

## 🎯 Choosing the Right Engine

### **Use V1 Engine When:**

- ✅ Maximum data safety is critical
- ✅ Legacy system compatibility required
- ✅ Simple dual-write architecture preferred
- ✅ Financial or regulatory compliance needs
- ✅ You need immediate disk persistence

### **Use V2 Engine When:**

- 🚀 High performance is required
- 🚀 Production workloads
- 🚀 Automatic recovery needed
- 🚀 Memory efficiency important
- 🚀 Modern ACID compliance required

## 🔧 V1 Engine Features

### **Dual-Write Architecture**

- **Immediate Persistence**: Every write saves to memory + disk
- **Zero Data Loss**: Guaranteed consistency across restarts
- **Background Retry**: Failed writes are queued and retried
- **Two Modes**: Dual-write (default) or no-saves (performance)

### **Performance Modes**

#### **Dual-Write Mode (Default)**

```bash
go run cmd/go-db.go
```

- **Throughput**: ~81 req/s
- **P95 Latency**: ~3.65s
- **Data Safety**: Maximum
- **Use Case**: Production, critical data

#### **No-Saves Mode (Performance)**

```bash
go run cmd/go-db.go -no-saves
```

- **Throughput**: ~458 req/s
- **P95 Latency**: ~171ms
- **Data Safety**: Minimal (shutdown only)
- **Use Case**: Caching, analytics, testing

## 🚀 V2 Engine Features

### **Write-Ahead Logging Architecture**

- **ACID Compliance**: Full ACID transactions
- **Automatic Recovery**: Sub-second crash recovery
- **Checkpointing**: Intelligent periodic snapshots
- **Memory Optimization**: 20-30% lower memory usage
- **Automatic Cleanup**: Smart file retention policies

### **Advanced Features**

- **Durability Levels**: Configurable persistence guarantees
- **Concurrent Operations**: Lock-free reads, optimistic writes
- **Background Workers**: Asynchronous checkpointing and cleanup
- **File Management**: Automatic WAL and checkpoint cleanup
- **Recovery**: Automatic recovery from crashes and power failures

### **Durability Levels**

| Level              | Description    | Performance | Safety           |
| ------------------ | -------------- | ----------- | ---------------- |
| `DurabilityNone`   | No persistence | Fastest     | None             |
| `DurabilityMemory` | Memory only    | Fast        | Low              |
| `DurabilityOS`     | OS page cache  | Good        | Medium (Default) |
| `DurabilityFull`   | Full fsync     | Slower      | Highest          |

## 📁 File Organization

### **V1 Engine Files**

```
./
├── go-db_data.godb          # Main data file
└── go-db_data.godb.idx      # Index file
```

### **V2 Engine Files**

```
./
├── wal/                     # Write-Ahead Log files
│   ├── wal_1757848291.log  # Current WAL
│   └── wal_1757848296.log  # Previous WALs
├── checkpoints/             # Checkpoint files
│   ├── checkpoint_1757848296.json
│   └── latest_checkpoint.json
└── data/                    # Data files (if any)
```

## 🔌 API Reference

Both engines provide identical REST APIs:

### **Collection Operations**

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

#### Batch Insert

```http
POST /collections/{collection}/batch
Content-Type: application/json

{
  "documents": [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
  ]
}
```

#### Find Documents

```http
GET /collections/{collection}/find
GET /collections/{collection}/find?age=30&city=New%20York
```

#### Pagination

```http
# Offset/Limit
GET /collections/{collection}/find?limit=10&offset=20

# Cursor-based
GET /collections/{collection}/find?limit=10&after=cursor
```

#### Streaming

```http
GET /collections/{collection}/find_with_stream
```

### **Document Operations**

#### Get by ID

```http
GET /collections/{collection}/documents/{id}
```

#### Update (Partial)

```http
PATCH /collections/{collection}/documents/{id}
Content-Type: application/json

{
  "age": 31,
  "city": "Boston"
}
```

#### Replace (Complete)

```http
PUT /collections/{collection}/documents/{id}
Content-Type: application/json

{
  "name": "Alice Smith",
  "age": 32,
  "position": "Senior Developer"
}
```

#### Batch Update

```http
PATCH /collections/{collection}/batch
Content-Type: application/json

{
  "operations": [
    {
      "id": "1",
      "updates": {"age": 31, "salary": 75000}
    }
  ]
}
```

#### Delete

```http
DELETE /collections/{collection}/documents/{id}
```

### **Index Operations**

#### Create Index

```http
POST /collections/{collection}/indexes/{field}
```

#### Get Indexes

```http
GET /collections/{collection}/indexes
```

## 🧪 Testing

### **Unit Tests**

```bash
# All tests
go test ./...

# V1 engine tests
go test ./pkg/storage/... -v

# V2 engine tests
go test ./pkg/storage/v2/... -v

# API tests
go test ./pkg/api/... -v
```

### **Integration Tests**

```bash
# V1 integration tests
go test -tags integration ./pkg/api/... -v

# V2 integration tests
go test -tags integration -run TestAPI_IntegrationV2 ./pkg/api/... -v
```

### **Performance Benchmarks**

```bash
# V1 benchmarks
go test ./pkg/storage/... -bench=.

# V2 benchmarks
go test ./pkg/storage/v2/... -bench=.
```

## 🐳 Docker

### **V1 Engine**

```bash
# Production mode (dual-write)
docker-compose up -d

# Performance mode (no-saves)
docker-compose run --rm go-db -no-saves -port 8080
```

### **V2 Engine**

```bash
# V2 engine
docker-compose run --rm go-db -v2 -port 8080

# V2 with custom config
docker-compose run --rm go-db -v2 -max-memory 2048 -port 8080
```

## 📈 Performance Examples

### **V1 Engine Performance**

```bash
# Dual-write mode
go run cmd/go-db.go
# Throughput: ~81 req/s, P95: ~3.65s

# No-saves mode
go run cmd/go-db.go -no-saves
# Throughput: ~458 req/s, P95: ~171ms
```

### **V2 Engine Performance**

```bash
# Default configuration (OS Durability)
go run cmd/go-db.go -v2
# Throughput: ~257 req/s, P95: ~630ms

# Memory Durability (Fastest)
go run cmd/go-db.go -v2 -durability memory
# Throughput: ~253 req/s, P95: ~632ms

# Full Durability (Safest)
go run cmd/go-db.go -v2 -durability full
# Throughput: ~242 req/s, P95: ~683ms
```

## 🔧 Configuration Examples

### **V1 Engine Configuration**

```bash
# Production setup
go run cmd/go-db.go \
  -port 8080 \
  -data-dir /var/lib/go-db \
  -max-memory 2048

# High-performance setup
go run cmd/go-db.go \
  -port 8080 \
  -no-saves \
  -max-memory 4096
```

### **V2 Engine Configuration**

```bash
# Production setup
go run cmd/go-db.go -v2 \
  -port 8080 \
  -data-dir /var/lib/go-db \
  -max-memory 2048

# High-performance setup
go run cmd/go-db.go -v2 \
  -port 8080 \
  -max-memory 4096
```

## 🚨 Migration Guide

### **V1 to V2 Migration**

1. **Backup V1 Data**

   ```bash
   # Export data from V1
   curl http://localhost:8080/collections/users/find > users_backup.json
   ```

2. **Start V2 Engine**

   ```bash
   go run cmd/go-db.go -v2
   ```

3. **Import Data**

   ```bash
   # Import data to V2
   curl -X POST http://localhost:8080/collections/users/batch \
     -H "Content-Type: application/json" \
     -d @users_backup.json
   ```

4. **Validate and Switch**
   - Run integration tests
   - Monitor performance
   - Update application configuration

## 🔍 Monitoring

### **Health Check**

```bash
curl http://localhost:8080/health
```

### **V2 Engine Monitoring**

```bash
# Check WAL files
ls -la wal/

# Check checkpoint files
ls -la checkpoints/

# Monitor memory usage
curl http://localhost:8080/health | jq '.memory'
```

## 📚 Advanced Documentation

- **[V2 Engine Details](pkg/storage/v2/README.md)**: Comprehensive V2 engine documentation
- **[Integration Tests](pkg/api/README_INTEGRATION_TESTS.md)**: Testing documentation
- **[Benchmarking](benchmarking/README.md)**: Performance testing guide

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with Go and modern database principles
- Inspired by MongoDB's document model
- WAL architecture based on PostgreSQL's approach
- Performance optimizations from various open-source databases

---

**Recommendation**: For new projects, use the V2 engine (`-v2` flag) for better performance, automatic recovery, and modern ACID compliance. V1 engine remains available for legacy compatibility.
