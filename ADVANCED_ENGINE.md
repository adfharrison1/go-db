# Advanced Storage Engine

## Overview

The Storage Engine is a sophisticated database engine that implements proper memory management, lazy loading, LRU caching, streaming capabilities, and optimized persistence. It's designed to handle large datasets efficiently while maintaining predictable memory usage and providing excellent performance.

## Key Features

### 🧠 Memory Management

- **LRU Caching**: Least Recently Used cache for collections with configurable capacity
- **Memory Limits**: Configurable maximum memory usage with automatic eviction
- **Lazy Loading**: Collections loaded only when accessed
- **Background Workers**: Automatic saving and cleanup with graceful shutdown

### 📊 Streaming Support

- **Document Streaming**: Stream documents without buffering entire collections
- **Channel-based**: Non-blocking streaming with Go channels (buffered)
- **Memory Efficient**: Constant memory usage regardless of collection size
- **Concurrent Streaming**: Multiple streams can operate simultaneously
- **Filter Support**: Stream with optional filtering for targeted data retrieval

### 📄 Pagination Support

- **Dual Pagination Models**: Support for both offset/limit and cursor-based pagination
- **Offset/Limit Pagination**: Traditional pagination with skip/limit for predictable navigation
- **Cursor-Based Pagination**: Performance-optimized pagination using document IDs and timestamps
- **Hybrid Response Format**: Unified response structure with pagination metadata
- **Filter Integration**: Pagination works seamlessly with document filtering
- **Index Optimization**: Pagination leverages existing indexes for optimal performance

### 🔄 Advanced Persistence

- **Optimized Format**: MessagePack + LZ4 compression for speed and size
- **Background Saving**: Automatic periodic saving of dirty collections
- **Graceful Shutdown**: Automatic data persistence on application exit
- **File Validation**: Magic bytes and version checking for data integrity

### 🏗️ Modular Architecture

- **Separated Concerns**: Each component in its own file for maintainability
- **Comprehensive Testing**: Unit and integration tests for all components
- **Thread Safety**: Full concurrency support with RWMutex protection
- **Error Handling**: Robust error handling with context preservation
- **Interface-based Design**: Clean separation between storage and indexing engines
- **API Dependency Injection**: Handlers accept storage and index engines for testability
- **Unified Find Methods**: Shared logic between FindAll and FindAllStream operations
- **Multi-Field Index Intersection**: Index-optimized queries support multi-field filters using intersection of multiple indexes for AND queries (e.g., `age=25 AND city='Boston'`).

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    StorageEngine                            │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   LRU Cache │  │ Collections │  │ Background  │        │
│  │  (lru.go)   │  │  Metadata   │  │   Workers   │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Format    │  │ Persistence │  │  Streaming  │        │
│  │ (format.go) │  │(persistence)│  │(streaming.go)│        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
│                                                             │
│  ┌─────────────┐                                            │
│  │ IndexEngine │  # Embedded for internal use               │
│  │ (indexing)  │                                            │
│  └─────────────┘                                            │
└─────────────────────────────────────────────────────────────┘
```

### Dependency Injection Pattern

The system uses dependency injection in the **API layer** for testability:

```go
// API handlers accept interfaces for testing
type Handler struct {
    storage domain.StorageEngine
    indexer domain.IndexEngine
}

// Server creates concrete implementations
func NewServer() *Server {
    dbEngine := storage.NewStorageEngine()      // Creates its own index engine
    indexEngine := indexing.NewIndexEngine()    // Separate instance for API

    return &Server{
        api: api.NewHandler(dbEngine, indexEngine), // DI for testability
    }
}
```

**Note**: The storage engine embeds its own index engine internally, while the API layer uses dependency injection to accept both storage and index engines for testing purposes.

### File Structure

```
pkg/
├── api/                    # HTTP API layer
│   ├── handlers.go         # Handler constructor
│   ├── find_all.go         # FindAll handler
│   ├── find_all_with_stream.go # FindAllWithStream handler
│   ├── create_index.go     # Index creation handler
│   ├── routes.go           # Route registration
│   ├── mock_storage.go     # Mock storage for testing
│   ├── mock_index.go       # Mock index for testing
│   └── handlers_test.go    # API tests
├── storage/                # Storage engine
│   ├── storage.go          # Main engine with core logic
│   ├── lru.go              # LRU cache implementation
│   ├── collection.go       # Collection management
│   ├── options.go          # Configuration options
│   ├── format.go           # Binary format (MessagePack + LZ4)
│   ├── streaming.go        # Streaming functionality
│   ├── persistence.go      # File I/O and background workers
│   └── *_test.go           # Storage tests
├── indexing/               # Index engine
│   ├── indexing.go         # Index implementation
│   └── indexing_test.go    # Index tests
└── domain/                 # Core interfaces
    ├── storage.go          # Storage interface
    └── indexing.go         # Index interface
```

### Collection States

```go
type CollectionState int

const (
    CollectionStateUnloaded CollectionState = iota // Not in memory
    CollectionStateLoading                         // Currently being loaded
    CollectionStateLoaded                          // Fully loaded in memory
    CollectionStateDirty                           // Modified since last save
)
```

## Usage Examples

### Basic Setup

```go
// Create engine with default settings (1GB memory limit)
engine := storage.NewStorageEngine()

// Or with custom configuration
engine := storage.NewStorageEngine(
    storage.WithMaxMemory(2048),                    // 2GB limit
    storage.WithDataDir("./data"),                  // Custom data directory
    storage.WithBackgroundSave(5*time.Minute),      // Auto-save every 5 minutes
)

// Start background workers (optional)
engine.StartBackgroundWorkers()
defer engine.StopBackgroundWorkers()
```

### Collection Operations

```go
// Create a collection
err := engine.CreateCollection("users")

// Insert documents
doc := data.Document{"name": "Alice", "age": 30}
err = engine.Insert("users", doc)

// Find all documents (loads entire collection)
docs, err := engine.FindAll("users")

// Find all documents (loads entire collection)
docs, err := engine.FindAll("users", nil)

// Find with filter (currently uses full collection scan)
filter := map[string]interface{}{"age": 30}
docs, err := engine.FindAll("users", filter)

// Find with multi-field filter (uses index intersection for AND queries if indexes exist)
filter := map[string]interface{}{ "age": 25, "city": "Boston" }
docs, err := engine.FindAll("users", filter)

// Find with pagination - offset/limit approach
paginationOptions := &domain.PaginationOptions{
    Limit:    10,
    Offset:   20,
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, paginationOptions)
// result.Documents contains the paginated documents
// result.HasNext indicates if there are more pages
// result.Total contains the total count

// Find with pagination - cursor-based approach
cursorOptions := &domain.PaginationOptions{
    Limit:    10,
    After:    "eyJpZCI6IjEwIiwidGltZXN0YW1wIjoiMjAyNS0wNy0xM1QxOTo0NDoyMS4yNzc3ODkrMDE6MDAifQ==",
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, cursorOptions)
// result.NextCursor contains the cursor for the next page
// result.PrevCursor contains the cursor for the previous page

// Find with filter and pagination
filter := map[string]interface{}{"age": 30}
result, err := engine.FindAll("users", filter, paginationOptions)

// Stream documents (memory efficient, no pagination)
docChan, err := engine.FindAllStream("users", nil)
if err != nil {
    return err
}

// Stream with filter (currently uses full collection scan)
filter := map[string]interface{}{"age": 30}
docChan, err := engine.FindAllStream("users", filter)

// Stream with multi-field filter (also uses index intersection)
docChan, err := engine.FindAllStream("users", map[string]interface{}{ "age": 25, "city": "Boston" })

// Process documents one at a time
for doc := range docChan {
    processDocument(doc)
}
```

### Pagination Implementation

The storage engine provides comprehensive pagination support with two distinct approaches:

#### Pagination Options

```go
type PaginationOptions struct {
    Limit     int    // Number of documents per page
    Offset    int    // Skip N documents (offset-based pagination)
    After     string // Cursor for next page (cursor-based pagination)
    Before    string // Cursor for previous page (cursor-based pagination)
    MaxLimit  int    // Maximum allowed limit
}
```

#### Response Format

```go
type PaginationResult struct {
    Documents   []Document // The actual documents
    HasNext     bool       // Whether there are more pages
    HasPrev     bool       // Whether there are previous pages
    Total       int64      // Total number of documents (for offset pagination)
    NextCursor  string     // Encoded cursor for next page
    PrevCursor  string     // Encoded cursor for previous page
}
```

#### Offset/Limit Pagination

Traditional pagination using skip/limit approach:

```go
// First page
options := &domain.PaginationOptions{
    Limit:    10,
    Offset:   0,
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, options)

// Second page
options = &domain.PaginationOptions{
    Limit:    10,
    Offset:   10,
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, options)
```

**Pros:**

- Predictable navigation (can jump to any page)
- Easy to implement in UI components
- Familiar to developers

**Cons:**

- Performance degrades with large offsets
- Can miss documents if collection changes between requests

#### Cursor-Based Pagination

Performance-optimized pagination using document identifiers:

```go
// First page
options := &domain.PaginationOptions{
    Limit:    10,
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, options)

// Next page using cursor
options = &domain.PaginationOptions{
    Limit:    10,
    After:    result.NextCursor,
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, options)

// Previous page using cursor
options = &domain.PaginationOptions{
    Limit:    10,
    Before:   result.PrevCursor,
    MaxLimit: 1000,
}
result, err := engine.FindAll("users", nil, options)
```

**Pros:**

- Consistent performance regardless of page number
- Handles concurrent modifications gracefully
- Better for real-time data

**Cons:**

- Cannot jump to arbitrary pages
- Requires cursor management in client code

#### Cursor Format

Cursors are base64-encoded JSON containing document ID and timestamp:

```json
{
  "id": "10",
  "timestamp": "2025-07-13T19:44:21.277789+01:00"
}
```

#### Best Practices

1. **Choose the Right Approach:**

   - Use **offset/limit** for admin interfaces, reports, or when you need random page access
   - Use **cursor-based** for real-time feeds, infinite scroll, or high-performance scenarios

2. **Limit Management:**

   - Always set reasonable limits (default: 50, max: 1000)
   - Use `MaxLimit` to prevent abuse

3. **Filter Integration:**

   - Pagination works seamlessly with document filters
   - Indexes are automatically used when available

4. **Streaming vs Pagination:**
   - Use **FindAll** with pagination for controlled data retrieval
   - Use **FindAllStream** for large datasets without pagination

### Memory Monitoring

```go
// Get current memory usage statistics
stats := engine.GetMemoryStats()
fmt.Printf("Memory usage: %d MB\n", stats["alloc_mb"])
fmt.Printf("Cache size: %d collections\n", stats["cache_size"])
fmt.Printf("Goroutines: %d\n", stats["num_goroutines"])
fmt.Printf("Total collections: %d\n", stats["collections"])
```

### Persistence

```go
// Load existing data
err := engine.LoadCollectionMetadata("database.godb")

// Manual save
err = engine.SaveToFile("database.godb")

// Background saving (automatic)
engine.StartBackgroundWorkers()
```

## Configuration Options

### StorageOption Functions

```go
// Set maximum memory usage in MB
storage.WithMaxMemory(1024) // 1GB limit

// Set data directory for collection files
storage.WithDataDir("./data")

// Enable background saving with interval
storage.WithBackgroundSave(5 * time.Minute)
```

## Performance Characteristics

| Operation                    | Memory Impact               | Performance  |
| ---------------------------- | --------------------------- | ------------ |
| **Startup**                  | ~1MB (metadata only)        | Very Fast    |
| **Collection Load**          | ~2-5MB per collection       | Fast         |
| **Document Insert**          | Constant                    | Very Fast    |
| **Document Find (no index)** | Full collection scan        | O(n)         |
| **Document Find (indexed)**  | Index lookup only           | O(log n)     |
| **Streaming**                | Constant (100 doc buffer)   | Very Fast    |
| **Pagination (offset)**      | Full scan + skip            | O(n)         |
| **Pagination (cursor)**      | Index-based navigation      | O(log n)     |
| **Background Save**          | Minimal (dirty collections) | Non-blocking |

### Performance Benchmarks

Based on our test results:

- **Streaming Throughput**: ~5.2M documents/second (in-memory operations)
- **LRU Cache Operations**: ~1.4M operations/second
- **File I/O**: ~2.2x faster than JSON, ~8x smaller files
- **Memory Allocations**: 50% reduction vs JSON serialization
- **Indexed Queries**: Multi-field index intersection for AND queries is implemented. Queries with multiple indexed fields use all available indexes for maximum efficiency. Both FindAll and FindAllStream share this logic.
- **Filtered Streaming**: Maintains high throughput with filter and index support.

### Scalability

- **Collections**: Unlimited (limited by disk space)
- **Documents per Collection**: Unlimited (limited by disk space)
- **Concurrent Access**: Thread-safe with RWMutex
- **Memory Usage**: Bounded by configuration
- **Streaming**: Constant memory regardless of collection size

## File Format

### Binary Format Specification

Each collection file uses an optimized binary format:

```
┌─────────────────────────────────────────────────────────────┐
│                        File Header                          │
├─────────────────────────────────────────────────────────────┤
│ Magic Bytes (4 bytes): "GODB"                              │
│ Version (2 bytes): 0x0001                                  │
│ Flags (2 bytes): Reserved for future use                   │
├─────────────────────────────────────────────────────────────┤
│                    Compressed Data                         │
│ LZ4-compressed MessagePack serialization of collection     │
└─────────────────────────────────────────────────────────────┘
```

### Format Benefits

- **MessagePack**: Faster serialization than JSON
- **LZ4 Compression**: High-speed compression with good ratio
- **Binary Format**: Reduced parsing overhead
- **Version Header**: Forward compatibility support

## Thread Safety

### Concurrency Model

The engine is fully thread-safe using:

- **RWMutex**: For collection access (read/write locks)
- **Channel-based**: For streaming operations
- **Atomic Operations**: For statistics and counters
- **Background Workers**: For non-blocking persistence

### Locking Strategy

```go
// Read operations (multiple concurrent readers)
engine.mu.RLock()
defer engine.mu.RUnlock()

// Write operations (exclusive access)
engine.mu.Lock()
defer engine.mu.Unlock()
```

## Error Handling

### Error Types

1. **Collection Errors**: Not found, already exists, invalid name
2. **I/O Errors**: File not found, permission denied, disk full
3. **Format Errors**: Corrupted files, invalid magic bytes
4. **Memory Errors**: Out of memory, cache full

### Error Recovery

- **Graceful Degradation**: Continue operation when possible
- **Context Preservation**: Wrap errors with operation context
- **Validation**: Check file integrity before loading
- **Fallback**: Use default values when configuration fails

## Testing Strategy

### Test Coverage

- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end functionality
- **Performance Tests**: Benchmarking and profiling
- **Concurrency Tests**: Thread safety validation

### Test Categories

```bash
# Run all tests
go test ./pkg/storage/... -v

# Run specific test suites
go test ./pkg/storage/... -run TestLRU
go test ./pkg/storage/... -run TestStreaming
go test ./pkg/storage/... -run TestPersistence

# Run benchmarks
go test ./pkg/storage/... -bench=.
```

## Best Practices

### Memory Configuration

- **Start with 1GB** limit for most applications
- **Monitor memory usage** with `GetMemoryStats()`
- **Adjust based on** available RAM and dataset size
- **Use streaming** for collections >10,000 documents

### Collection Design

- **Keep collections focused** on specific data types
- **Avoid monolithic collections** that grow too large
- **Use meaningful names** for better organization
- **Consider access patterns** when designing collections

### Performance Optimization

- **Enable background saving** for write-heavy workloads
- **Use streaming** for read-heavy operations on large collections
- **Monitor cache hit rates** to optimize memory usage
- **Batch operations** when possible

### Error Handling

- **Always check errors** from engine operations
- **Handle graceful shutdown** properly
- **Validate input data** before insertion
- **Monitor background worker health**

## Migration from Previous Versions

### API Compatibility

The current engine maintains compatibility with previous versions:

```go
// Previous API still works
docs, err := engine.FindAll("users")
err = engine.Insert("users", doc)

// New advanced features available
docChan, err := engine.FindAllStream("users")
stats := engine.GetMemoryStats()
engine.StartBackgroundWorkers()
```

### Migration Steps

1. **Update imports** (if using old package names)
2. **Add configuration** for memory limits and background saving
3. **Enable streaming** for large collections
4. **Add error handling** for new error types

## Future Enhancements

### Planned Features

1. **Query Optimization**: Index-based queries with streaming
2. **Compression Options**: Configurable compression algorithms
3. **Replication**: Multi-node support with consistency guarantees
4. **Backup Strategies**: Incremental and full backup support
5. **Monitoring**: Metrics collection and health checks

### Extension Points

The engine is designed for easy extension:

- **Custom Storage Backends**: S3, Azure, GCS integration
- **Custom Compression**: Different algorithms per collection
- **Custom Eviction Strategies**: Time-based, size-based, etc.
- **Custom Background Workers**: Indexing, cleanup, etc.
- **Custom Serialization**: Protocol Buffers, Avro, etc.

## Troubleshooting

### Common Issues

1. **High Memory Usage**: Reduce max memory or enable LRU eviction
2. **Slow Performance**: Check if streaming is appropriate
3. **File Corruption**: Validate files with magic bytes
4. **Background Worker Issues**: Check error logs and restart

### Debugging

```go
// Enable verbose logging
log.SetLevel(log.DebugLevel)

// Monitor memory usage
stats := engine.GetMemoryStats()
log.Printf("Memory stats: %+v", stats)

// Check collection states
for name, info := range engine.collections {
    log.Printf("Collection %s: %s", name, info.State)
}
```
