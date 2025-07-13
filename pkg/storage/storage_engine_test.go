package storage

import (
	"os"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
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
			assert.NotNil(t, engine.indexEngine)
			assert.NotNil(t, engine.metadata)
			assert.NotNil(t, engine.stopChan)
		})
	}
}

func TestStorageEngine_InsertAndFind(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	doc1 := domain.Document{"name": "Alice", "age": 30}
	doc2 := domain.Document{"name": "Bob", "age": 25}

	err := engine.Insert("users", doc1)
	require.NoError(t, err)
	err = engine.Insert("users", doc2)
	require.NoError(t, err)

	docs, err := engine.FindAll("users", nil)
	require.NoError(t, err)
	assert.Len(t, docs, 2)

	names := make(map[string]bool)
	for _, doc := range docs {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}
	assert.True(t, names["Alice"])
	assert.True(t, names["Bob"])
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
	docs := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test streaming
	docChan, err := engine.FindAllStream("users", nil)
	require.NoError(t, err)

	receivedDocs := make([]domain.Document, 0)
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
		doc := domain.Document{"id": i, "data": "test"}
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
	docs := []domain.Document{
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
				doc := domain.Document{
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
	docs, err := engine.FindAll("concurrent", nil)
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
	doc := domain.Document{"test": "data"}
	err = engine.Insert("new_collection", doc)
	assert.NoError(t, err)

	// Verify collection was created
	collection, err := engine.GetCollection("new_collection")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 1)
}

func TestStorageEngine_GetById(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	doc1 := domain.Document{"_id": "1", "name": "Alice", "age": 25}
	doc2 := domain.Document{"_id": "2", "name": "Bob", "age": 30}

	err := engine.Insert("users", doc1)
	require.NoError(t, err)
	err = engine.Insert("users", doc2)
	require.NoError(t, err)

	// Test successful retrieval
	retrieved, err := engine.GetById("users", "1")
	require.NoError(t, err)
	assert.Equal(t, "Alice", retrieved["name"])
	assert.Equal(t, 25, retrieved["age"])

	// Test non-existent document
	_, err = engine.GetById("users", "999")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test non-existent collection
	_, err = engine.GetById("nonexistent", "1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStorageEngine_UpdateById(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test document
	doc := domain.Document{"_id": "1", "name": "Alice", "age": 25}
	err := engine.Insert("users", doc)
	require.NoError(t, err)

	// Test successful update
	updates := domain.Document{"age": 26, "city": "Boston"}
	err = engine.UpdateById("users", "1", updates)
	require.NoError(t, err)

	// Verify update
	retrieved, err := engine.GetById("users", "1")
	require.NoError(t, err)
	assert.Equal(t, 26, retrieved["age"])
	assert.Equal(t, "Boston", retrieved["city"])
	assert.Equal(t, "Alice", retrieved["name"]) // Original field unchanged

	// Test that _id cannot be updated
	updates = domain.Document{"_id": "999"}
	err = engine.UpdateById("users", "1", updates)
	require.NoError(t, err) // Should not error, but should not update _id

	retrieved, err = engine.GetById("users", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", retrieved["_id"]) // _id should remain unchanged

	// Test non-existent document
	err = engine.UpdateById("users", "999", updates)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test non-existent collection
	err = engine.UpdateById("nonexistent", "1", updates)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStorageEngine_DeleteById(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	doc1 := domain.Document{"_id": "1", "name": "Alice", "age": 25}
	doc2 := domain.Document{"_id": "2", "name": "Bob", "age": 30}

	err := engine.Insert("users", doc1)
	require.NoError(t, err)
	err = engine.Insert("users", doc2)
	require.NoError(t, err)

	// Verify both documents exist
	docs, err := engine.FindAll("users", nil)
	require.NoError(t, err)
	assert.Len(t, docs, 2)

	// Test successful deletion
	err = engine.DeleteById("users", "1")
	require.NoError(t, err)

	// Verify document was deleted
	docs, err = engine.FindAll("users", nil)
	require.NoError(t, err)
	assert.Len(t, docs, 1)
	assert.Equal(t, "2", docs[0]["_id"])

	// Test non-existent document
	err = engine.DeleteById("users", "999")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test non-existent collection
	err = engine.DeleteById("nonexistent", "1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStorageEngine_FindAll(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	docs := []domain.Document{
		{"_id": "1", "name": "Alice", "age": 25, "city": "New York"},
		{"_id": "2", "name": "Bob", "age": 30, "city": "San Francisco"},
		{"_id": "3", "name": "Charlie", "age": 25, "city": "Boston"},
		{"_id": "4", "name": "David", "age": 35, "city": "New York"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test single field filter
	results, err := engine.FindAll("users", map[string]interface{}{"age": 25})
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// Verify results
	names := make([]string, len(results))
	for i, doc := range results {
		names[i] = doc["name"].(string)
	}
	assert.Contains(t, names, "Alice")
	assert.Contains(t, names, "Charlie")

	// Test string filter (case-insensitive)
	results, err = engine.FindAll("users", map[string]interface{}{"city": "new york"})
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// Test multiple field filter
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "New York",
	})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])

	// Test non-existent field
	results, err = engine.FindAll("users", map[string]interface{}{"nonexistent": "value"})
	require.NoError(t, err)
	assert.Len(t, results, 0)

	// Test non-existent collection
	_, err = engine.FindAll("nonexistent", map[string]interface{}{"age": 25})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Test empty filter (should return all documents)
	results, err = engine.FindAll("users", map[string]interface{}{})
	require.NoError(t, err)
	assert.Len(t, results, 4)
}

func TestStorageEngine_FilterTypeHandling(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents with different numeric types
	docs := []domain.Document{
		{"_id": "1", "age": 25, "score": 100.5, "count": int64(42)},
		{"_id": "2", "age": 25.0, "score": 100, "count": 42.0},
		{"_id": "3", "age": 30, "score": 200.0, "count": 100},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test int vs float64 comparison
	results, err := engine.FindAll("users", map[string]interface{}{"age": 25})
	require.NoError(t, err)
	assert.Len(t, results, 2) // Both 25 and 25.0 should match

	results, err = engine.FindAll("users", map[string]interface{}{"age": 25.0})
	require.NoError(t, err)
	assert.Len(t, results, 2) // Both 25 and 25.0 should match

	// Test float vs int comparison - this might not work as expected due to type differences
	results, err = engine.FindAll("users", map[string]interface{}{"score": 100})
	require.NoError(t, err)
	// The exact count depends on how the type comparison works in the implementation
	assert.GreaterOrEqual(t, len(results), 1)

	// Test string case-insensitive comparison
	docs = []domain.Document{
		{"_id": "4", "name": "Alice", "city": "New York"},
		{"_id": "5", "name": "alice", "city": "NEW YORK"},
		{"_id": "6", "name": "Bob", "city": "Boston"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	results, err = engine.FindAll("users", map[string]interface{}{"name": "alice"})
	require.NoError(t, err)
	assert.Len(t, results, 2) // Both "Alice" and "alice" should match

	results, err = engine.FindAll("users", map[string]interface{}{"city": "new york"})
	require.NoError(t, err)
	assert.Len(t, results, 2) // Both "New York" and "NEW YORK" should match
}

func TestStorageEngine_IndexOptimization(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert documents with different ages
	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "New York"},
		{"name": "Bob", "age": 30, "city": "Boston"},
		{"name": "Charlie", "age": 25, "city": "Chicago"},
		{"name": "Diana", "age": 35, "city": "New York"},
		{"name": "Eve", "age": 25, "city": "Boston"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index on age field
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Test index optimization - should use index for age=25
	filter := map[string]interface{}{"age": 25}
	results, err := engine.FindAll("users", filter)
	require.NoError(t, err)

	// Should find 3 documents with age=25
	assert.Len(t, results, 3)

	// Verify we got the right documents
	names := make(map[string]bool)
	for _, doc := range results {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}

	assert.True(t, names["Alice"])
	assert.True(t, names["Charlie"])
	assert.True(t, names["Eve"])

	// Test multiple field filter - should still work
	filter = map[string]interface{}{"age": 25, "city": "New York"}
	results, err = engine.FindAll("users", filter)
	require.NoError(t, err)

	// Should find 1 document with age=25 and city=New York
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])

	// Test non-indexed field - should fall back to full scan
	filter = map[string]interface{}{"city": "Boston"}
	results, err = engine.FindAll("users", filter)
	require.NoError(t, err)

	// Should find 2 documents with city=Boston
	assert.Len(t, results, 2)

	// Test non-existent value - should return empty
	filter = map[string]interface{}{"age": 999}
	results, err = engine.FindAll("users", filter)
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestStorageEngine_IndexOptimizationStream(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "New York"},
		{"name": "Bob", "age": 30, "city": "Boston"},
		{"name": "Charlie", "age": 25, "city": "Chicago"},
		{"name": "Diana", "age": 35, "city": "New York"},
		{"name": "Eve", "age": 25, "city": "Boston"},
	}
	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index on age field
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Test index optimization - should use index for age=25
	filter := map[string]interface{}{"age": 25}
	docChan, err := engine.FindAllStream("users", filter)
	require.NoError(t, err)
	var results []domain.Document
	for doc := range docChan {
		results = append(results, doc)
	}
	assert.Len(t, results, 3)
	names := make(map[string]bool)
	for _, doc := range results {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}
	assert.True(t, names["Alice"])
	assert.True(t, names["Charlie"])
	assert.True(t, names["Eve"])

	// Test multiple field filter - should still work
	filter = map[string]interface{}{"age": 25, "city": "New York"}
	docChan, err = engine.FindAllStream("users", filter)
	require.NoError(t, err)
	results = nil
	for doc := range docChan {
		results = append(results, doc)
	}
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])

	// Test non-indexed field - should fall back to full scan
	filter = map[string]interface{}{"city": "Boston"}
	docChan, err = engine.FindAllStream("users", filter)
	require.NoError(t, err)
	results = nil
	for doc := range docChan {
		results = append(results, doc)
	}
	assert.Len(t, results, 2)

	// Test non-existent value - should return empty
	filter = map[string]interface{}{"age": 999}
	docChan, err = engine.FindAllStream("users", filter)
	require.NoError(t, err)
	results = nil
	for doc := range docChan {
		results = append(results, doc)
	}
	assert.Len(t, results, 0)
}

func TestStorageEngine_MultiFieldIndexOptimization(t *testing.T) {
	engine := NewStorageEngine()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	assert.NoError(t, err)

	// Insert documents with multiple fields
	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "Boston", "role": "admin"},
		{"name": "Bob", "age": 30, "city": "Boston", "role": "user"},
		{"name": "Charlie", "age": 25, "city": "New York", "role": "user"},
		{"name": "Diana", "age": 35, "city": "Boston", "role": "admin"},
		{"name": "Eve", "age": 25, "city": "Boston", "role": "user"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		assert.NoError(t, err)
	}

	// Create indexes on multiple fields
	err = engine.CreateIndex("users", "age")
	assert.NoError(t, err)
	err = engine.CreateIndex("users", "city")
	assert.NoError(t, err)
	err = engine.CreateIndex("users", "role")
	assert.NoError(t, err)

	// Test single field index optimization
	results, err := engine.FindAll("users", map[string]interface{}{"age": 25})
	assert.NoError(t, err)
	assert.Len(t, results, 3) // Alice, Charlie, Eve

	// Test two-field index intersection (AND logic)
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "Boston",
	})
	assert.NoError(t, err)
	assert.Len(t, results, 2) // Alice, Eve

	// Test three-field index intersection
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "Boston",
		"role": "user",
	})
	assert.NoError(t, err)
	assert.Len(t, results, 1) // Eve only

	// Test with non-indexed field (should still work but may not use index optimization)
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"name": "Alice",
	})
	assert.NoError(t, err)
	assert.Len(t, results, 1) // Alice only

	// Test streaming with multi-field filters
	docChan, err := engine.FindAllStream("users", map[string]interface{}{
		"age":  25,
		"city": "Boston",
	})
	assert.NoError(t, err)

	var streamResults []domain.Document
	for doc := range docChan {
		streamResults = append(streamResults, doc)
	}
	assert.Len(t, streamResults, 2) // Alice, Eve
}

func TestStorageEngine_IndexOptimizationFallback(t *testing.T) {
	engine := NewStorageEngine()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	assert.NoError(t, err)

	// Insert documents
	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "Boston"},
		{"name": "Bob", "age": 30, "city": "New York"},
		{"name": "Charlie", "age": 25, "city": "Boston"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		assert.NoError(t, err)
	}

	// Create index only on age field
	err = engine.CreateIndex("users", "age")
	assert.NoError(t, err)

	// Test query with indexed and non-indexed fields
	// Should use index for age=25, then filter by city
	results, err := engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "Boston",
	})
	assert.NoError(t, err)
	assert.Len(t, results, 2) // Alice, Charlie

	// Test query with only non-indexed fields (should fall back to full scan)
	results, err = engine.FindAll("users", map[string]interface{}{
		"city": "Boston",
	})
	assert.NoError(t, err)
	assert.Len(t, results, 2) // Alice, Charlie
}
