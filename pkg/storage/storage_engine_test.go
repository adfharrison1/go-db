package storage

import (
	"os"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStorageEngine(t *testing.T) {
	tests := []struct {
		name     string
		options  []StorageOption
		expected *StorageEngine
	}{
		{
			name:    "default options",
			options: []StorageOption{},
			expected: &StorageEngine{
				maxMemoryMB:    1024,
				dataDir:        ".",
				backgroundSave: false,
				saveInterval:   5 * time.Minute,
			},
		},
		{
			name: "custom options",
			options: []StorageOption{
				WithMaxMemory(2048),
				WithDataDir("/tmp"),
				WithBackgroundSave(1 * time.Minute),
			},
			expected: &StorageEngine{
				maxMemoryMB:    2048,
				dataDir:        "/tmp",
				backgroundSave: true,
				saveInterval:   1 * time.Minute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewStorageEngine(tt.options...)

			assert.Equal(t, tt.expected.maxMemoryMB, engine.maxMemoryMB)
			assert.Equal(t, tt.expected.dataDir, engine.dataDir)
			assert.Equal(t, tt.expected.backgroundSave, engine.backgroundSave)
			assert.Equal(t, tt.expected.saveInterval, engine.saveInterval)
			assert.NotNil(t, engine.cache)
			assert.NotNil(t, engine.collections)
			assert.NotNil(t, engine.indexes)
			assert.NotNil(t, engine.metadata)
			assert.NotNil(t, engine.stopChan)
		})
	}
}

func TestStorageEngine_InsertAndFind(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test inserting documents
	doc1 := data.Document{"name": "Alice", "age": 30}
	doc2 := data.Document{"name": "Bob", "age": 25}

	err := engine.Insert("users", doc1)
	require.NoError(t, err)

	err = engine.Insert("users", doc2)
	require.NoError(t, err)

	// Test finding all documents
	docs, err := engine.FindAll("users")
	require.NoError(t, err)
	assert.Len(t, docs, 2)

	// Verify document IDs were generated
	ids := make(map[string]bool)
	for _, doc := range docs {
		id, exists := doc["_id"]
		assert.True(t, exists)
		assert.NotEmpty(t, id)
		ids[id.(string)] = true
	}
	assert.Len(t, ids, 2)
}

func TestStorageEngine_GetCollection(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test getting non-existent collection
	_, err := engine.GetCollection("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Create collection and test getting it
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Equal(t, "test", collection.Name)
	assert.NotNil(t, collection.Documents)
}

func TestStorageEngine_CreateCollection(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test creating new collection
	err := engine.CreateCollection("test")
	require.NoError(t, err)

	// Verify collection exists in metadata
	engine.mu.RLock()
	info, exists := engine.collections["test"]
	engine.mu.RUnlock()

	assert.True(t, exists)
	assert.Equal(t, "test", info.Name)
	assert.Equal(t, CollectionStateLoaded, info.State)

	// Test creating duplicate collection
	err = engine.CreateCollection("test")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestStorageEngine_Streaming(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	docs := []data.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test streaming
	docChan, err := engine.FindAllStream("users")
	require.NoError(t, err)

	receivedDocs := make([]data.Document, 0)
	for doc := range docChan {
		receivedDocs = append(receivedDocs, doc)
	}

	assert.Len(t, receivedDocs, 3)
}

func TestStorageEngine_MemoryStats(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert some data
	for i := 0; i < 10; i++ {
		doc := data.Document{"id": i, "data": "test"}
		err := engine.Insert("test", doc)
		require.NoError(t, err)
	}

	stats := engine.GetMemoryStats()

	assert.NotNil(t, stats)
	assert.Contains(t, stats, "alloc_mb")
	assert.Contains(t, stats, "total_alloc_mb")
	assert.Contains(t, stats, "sys_mb")
	assert.Contains(t, stats, "num_goroutines")
	assert.Contains(t, stats, "cache_size")
	assert.Contains(t, stats, "collections")

	// Verify reasonable values
	assert.GreaterOrEqual(t, stats["alloc_mb"], uint64(0))
	assert.GreaterOrEqual(t, stats["cache_size"], 0)
	assert.GreaterOrEqual(t, stats["collections"], 0)
}

func TestStorageEngine_BackgroundWorkers(t *testing.T) {
	engine := NewStorageEngine(WithBackgroundSave(100 * time.Millisecond))

	// Start background workers
	engine.StartBackgroundWorkers()

	// Give them time to start
	time.Sleep(50 * time.Millisecond)

	// Stop background workers
	engine.StopBackgroundWorkers()

	// Verify stop channel is closed
	select {
	case <-engine.stopChan:
		// Expected
	default:
		t.Error("stop channel should be closed")
	}
}

func TestStorageEngine_Persistence(t *testing.T) {
	tempFile := "test_storage.godb"
	defer os.Remove(tempFile)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test data
	docs := []data.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Save to file
	err := engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(tempFile)
	require.NoError(t, err)

	// Create new engine and load metadata
	newEngine := NewStorageEngine()
	defer newEngine.StopBackgroundWorkers()

	err = newEngine.LoadCollectionMetadata(tempFile)
	require.NoError(t, err)

	// Verify collection metadata was loaded
	newEngine.mu.RLock()
	info, exists := newEngine.collections["users"]
	newEngine.mu.RUnlock()

	assert.True(t, exists)
	assert.Equal(t, "users", info.Name)
	assert.Equal(t, int64(2), info.DocumentCount)
	assert.Equal(t, CollectionStateUnloaded, info.State)
}

func TestStorageEngine_Concurrency(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test concurrent inserts
	const numGoroutines = 10
	const docsPerGoroutine = 10

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			for j := 0; j < docsPerGoroutine; j++ {
				doc := data.Document{
					"goroutine": id,
					"doc_id":    j,
					"data":      "concurrent test",
				}
				err := engine.Insert("concurrent", doc)
				require.NoError(t, err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all documents were inserted
	docs, err := engine.FindAll("concurrent")
	require.NoError(t, err)
	assert.Len(t, docs, numGoroutines*docsPerGoroutine)
}

func TestStorageEngine_ErrorHandling(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test invalid collection name
	err := engine.CreateCollection("")
	assert.Error(t, err)

	// Test inserting to non-existent collection (should create it)
	doc := data.Document{"test": "data"}
	err = engine.Insert("new_collection", doc)
	assert.NoError(t, err)

	// Verify collection was created
	collection, err := engine.GetCollection("new_collection")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 1)
}
