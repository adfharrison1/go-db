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
└─────────────────────────────────────────────────────────────┘
```

### File Structure

```
pkg/storage/
├── storage.go              # Main engine with core logic
├── lru.go                  # LRU cache implementation
├── collection.go           # Collection management
├── options.go              # Configuration options
├── format.go               # Binary format (MessagePack + LZ4)
├── streaming.go            # Streaming functionality
├── persistence.go          # File I/O and background workers
├── storage_engine_test.go  # Integration tests
├── lru_cache_test.go       # LRU cache tests
├── format_test.go          # Format tests
├── persistence_test.go     # Persistence tests
└── streaming_test.go       # Streaming tests
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

// Stream documents (memory efficient)
docChan, err := engine.FindAllStream("users")
if err != nil {
    return err
}

// Process documents one at a time
for doc := range docChan {
    processDocument(doc)
}
```

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

### Memory Usage

| Operation                   | Memory Impact              | Performance  |
| --------------------------- | -------------------------- | ------------ |
| **Startup**                 | ~1MB (metadata only)       | Very Fast    |
| **First Collection Access** | Collection size            | Fast         |
| **Streaming**               | Constant (~1KB per doc)    | Excellent    |
| **LRU Eviction**            | Reduced by collection size | Fast         |
| **Background Save**         | Minimal (async)            | Non-blocking |

### Performance Benchmarks

Based on our test results:

- **Streaming Throughput**: ~6.3M documents/second
- **LRU Cache Operations**: ~1.4M operations/second
- **File I/O**: ~2.2x faster than JSON, ~8x smaller files
- **Memory Allocations**: 50% reduction vs JSON serialization

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
