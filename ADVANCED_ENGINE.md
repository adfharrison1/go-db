# Advanced Storage Engine

## Overview

The Advanced Storage Engine is a sophisticated database engine that implements proper memory management, lazy loading, LRU caching, and streaming capabilities. It's designed to handle large datasets efficiently while maintaining predictable memory usage.

## Key Features

### 🧠 Memory Management

- **LRU Caching**: Least Recently Used cache for collections
- **Memory Limits**: Configurable maximum memory usage
- **Lazy Loading**: Collections loaded only when accessed
- **Background Workers**: Automatic saving and cleanup

### 📊 Streaming Support

- **Document Streaming**: Stream documents without buffering entire collections
- **Channel-based**: Non-blocking streaming with Go channels
- **Memory Efficient**: Constant memory usage regardless of collection size

### 🔄 Advanced Loading

- **Metadata-only Loading**: Load collection info without data at startup
- **On-demand Loading**: Load collections only when first accessed
- **Dirty Tracking**: Track modified collections for efficient saving

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                AdvancedStorageEngine                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   LRU Cache │  │ Collections │  │ Background  │        │
│  │             │  │  Metadata   │  │   Workers   │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
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
// Create engine with 1GB memory limit
engine := NewAdvancedStorageEngine(
    WithMaxMemory(1024),
    WithDataDir("./data"),
    WithBackgroundSave(5*time.Minute),
)

// Start background workers
engine.StartBackgroundWorkers()
defer engine.StopBackgroundWorkers()
```

### Lazy Loading

```go
// Collection metadata loaded at startup, but data loaded on first access
err := engine.LoadCollectionMetadata("database.godb")

// Collection loaded only when accessed
collection, err := engine.GetCollection("users")
if err != nil {
    // Collection doesn't exist or failed to load
}
```

### Streaming Documents

```go
// Stream documents without loading entire collection into memory
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
```

## Configuration Options

### StorageOption Functions

```go
// Set maximum memory usage in MB
WithMaxMemory(1024) // 1GB limit

// Set data directory for collection files
WithDataDir("./data")

// Enable background saving with interval
WithBackgroundSave(5 * time.Minute)
```

## Memory Management Strategy

### LRU Cache Implementation

The LRU cache uses a combination of:

- **Doubly-linked list**: For O(1) access and eviction
- **Hash map**: For O(1) lookups
- **Thread-safe operations**: Using RWMutex

```go
type LRUCache struct {
    mu       sync.RWMutex
    capacity int
    list     *list.List
    cache    map[string]*list.Element
}
```

### Memory Eviction

When the cache reaches capacity:

1. **Least recently used** collection is identified
2. **Collection is evicted** from memory
3. **Metadata is preserved** for future loading
4. **Dirty collections are saved** before eviction

### Collection Lifecycle

```
1. Startup: Load metadata only
2. First Access: Load collection into cache
3. Operations: Mark as dirty when modified
4. Background Save: Save dirty collections periodically
5. Memory Pressure: Evict least used collections
6. Shutdown: Save all dirty collections
```

## Performance Characteristics

### Memory Usage

| Operation                   | Memory Impact              | Performance |
| --------------------------- | -------------------------- | ----------- |
| **Startup**                 | ~1MB (metadata only)       | Very Fast   |
| **First Collection Access** | Collection size            | Fast        |
| **Streaming**               | Constant (~1KB per doc)    | Excellent   |
| **LRU Eviction**            | Reduced by collection size | Fast        |

### Scalability

- **Collections**: Unlimited (limited by disk space)
- **Documents per Collection**: Unlimited (limited by disk space)
- **Concurrent Access**: Thread-safe with RWMutex
- **Memory Usage**: Bounded by configuration

## File Structure

### Per-Collection Files

```
data/
├── collections/
│   ├── users.godb
│   ├── posts.godb
│   └── comments.godb
└── metadata.godb
```

### File Format

Each collection file uses the same format as the original engine:

- **Header**: 8-byte magic identifier + version
- **Data**: LZ4-compressed MessagePack serialization

## Background Workers

### Automatic Saving

```go
// Background worker saves dirty collections every 5 minutes
engine := NewAdvancedStorageEngine(
    WithBackgroundSave(5 * time.Minute),
)

engine.StartBackgroundWorkers()
```

### Graceful Shutdown

```go
// Stop background workers and save all dirty collections
engine.StopBackgroundWorkers()
```

## Error Handling

### Common Error Scenarios

1. **Collection Not Found**: Returns error, doesn't create automatically
2. **Disk I/O Errors**: Wrapped with context using `fmt.Errorf`
3. **Memory Pressure**: Automatic LRU eviction
4. **Corrupted Files**: Validation with magic bytes and version checking

### Recovery Strategies

- **Metadata Corruption**: Rebuild from collection files
- **Collection File Corruption**: Skip corrupted collections
- **Memory Exhaustion**: Force LRU eviction
- **Background Worker Failure**: Restart workers automatically

## Migration from Basic Engine

### API Compatibility

The advanced engine maintains compatibility with the basic engine:

```go
// Basic engine methods still work
docs, err := engine.FindAll("users")
err = engine.Insert("users", doc)

// New advanced methods available
docChan, err := engine.FindAllStream("users")
stats := engine.GetMemoryStats()
```

### Migration Path

1. **Replace engine creation**:

   ```go
   // Old
   engine := storage.NewStorageEngine()

   // New
   engine := storage.NewAdvancedStorageEngine(
       WithMaxMemory(1024),
       WithBackgroundSave(5*time.Minute),
   )
   ```

2. **Update initialization**:

   ```go
   // Old
   engine.LoadFromFile("data.godb")

   // New
   engine.LoadCollectionMetadata("data.godb")
   engine.StartBackgroundWorkers()
   ```

3. **Add streaming where beneficial**:
   ```go
   // For large collections, use streaming
   docChan, err := engine.FindAllStream("large_collection")
   ```

## Best Practices

### Memory Configuration

- **Start with 1GB** limit for most applications
- **Monitor memory usage** with `GetMemoryStats()`
- **Adjust based on** available RAM and dataset size

### Collection Design

- **Keep collections focused** on specific data types
- **Avoid monolithic collections** that grow too large
- **Use streaming** for collections with >10,000 documents

### Performance Optimization

- **Enable background saving** for write-heavy workloads
- **Use streaming** for read-heavy operations on large collections
- **Monitor cache hit rates** to optimize memory usage

## Future Enhancements

### Planned Features

1. **Per-collection file storage** (currently loads entire file)
2. **Compression options** (different algorithms)
3. **Index integration** with the existing indexing system
4. **Query optimization** with streaming filters
5. **Replication** and backup strategies

### Extension Points

The engine is designed for easy extension:

- **Custom storage backends** (S3, etc.)
- **Custom compression algorithms**
- **Custom eviction strategies**
- **Custom background workers**
