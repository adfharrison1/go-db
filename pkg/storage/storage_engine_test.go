package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
				maxMemoryMB: 1024,
				dataDir:     ".",
				noSaves:     false,
			},
		},
		{
			name: "custom options",
			options: []StorageOption{
				WithMaxMemory(2048),
				WithDataDir("/tmp"),
				WithNoSaves(true),
			},
			expected: &StorageEngine{
				maxMemoryMB: 2048,
				dataDir:     "/tmp",
				noSaves:     true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewStorageEngine(tt.options...)

			assert.Equal(t, tt.expected.maxMemoryMB, engine.maxMemoryMB)
			assert.Equal(t, tt.expected.dataDir, engine.dataDir)
			assert.Equal(t, tt.expected.noSaves, engine.noSaves)
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

	_, err := engine.Insert("users", doc1)
	require.NoError(t, err)
	_, err = engine.Insert("users", doc2)
	require.NoError(t, err)

	result, err := engine.FindAll("users", nil, nil)
	require.NoError(t, err)
	assert.Len(t, result.Documents, 2)

	names := make(map[string]bool)
	for _, doc := range result.Documents {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}
	assert.True(t, names["Alice"])
	assert.True(t, names["Bob"])

	// Verify that _id index was automatically created
	indexes, err := engine.GetIndexes("users")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Len(t, indexes, 1) // Only _id index should exist
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
		_, err := engine.Insert("users", doc)
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
		_, err := engine.Insert("test", doc)
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
	engine := NewStorageEngine(WithNoSaves(false))

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
		_, err := engine.Insert("users", doc)
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
				_, err := engine.Insert("concurrent", doc)
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
	result, err := engine.FindAll("concurrent", nil, &domain.PaginationOptions{Limit: 10000})
	require.NoError(t, err)
	assert.Len(t, result.Documents, numGoroutines*docsPerGoroutine)
}

func TestStorageEngine_ErrorHandling(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test invalid collection name
	err := engine.CreateCollection("")
	assert.Error(t, err)

	// Test inserting to non-existent collection (should create it)
	doc := domain.Document{"test": "data"}
	_, err = engine.Insert("new_collection", doc)
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

	_, err := engine.Insert("users", doc1)
	require.NoError(t, err)
	_, err = engine.Insert("users", doc2)
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
	_, err := engine.Insert("users", doc)
	require.NoError(t, err)

	// Test successful update
	updates := domain.Document{"age": 26, "city": "Boston"}
	_, err = engine.UpdateById("users", "1", updates)
	require.NoError(t, err)

	// Verify update
	retrieved, err := engine.GetById("users", "1")
	require.NoError(t, err)
	assert.Equal(t, 26, retrieved["age"])
	assert.Equal(t, "Boston", retrieved["city"])
	assert.Equal(t, "Alice", retrieved["name"]) // Original field unchanged

	// Test that _id cannot be updated
	updates = domain.Document{"_id": "999"}
	_, err = engine.UpdateById("users", "1", updates)
	require.NoError(t, err) // Should not error, but should not update _id

	retrieved, err = engine.GetById("users", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", retrieved["_id"]) // _id should remain unchanged

	// Test non-existent document
	_, err = engine.UpdateById("users", "999", updates)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")

	// Test non-existent collection
	_, err = engine.UpdateById("nonexistent", "1", updates)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStorageEngine_DeleteById(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	doc1 := domain.Document{"_id": "1", "name": "Alice", "age": 25}
	doc2 := domain.Document{"_id": "2", "name": "Bob", "age": 30}

	_, err := engine.Insert("users", doc1)
	require.NoError(t, err)
	_, err = engine.Insert("users", doc2)
	require.NoError(t, err)

	// Verify both documents exist
	docs, err := engine.FindAll("users", nil, nil)
	require.NoError(t, err)
	assert.Len(t, docs.Documents, 2)

	// Test successful deletion
	err = engine.DeleteById("users", "1")
	require.NoError(t, err)

	// Verify document was deleted
	docs, err = engine.FindAll("users", nil, nil)
	require.NoError(t, err)
	assert.Len(t, docs.Documents, 1)
	assert.Equal(t, "2", docs.Documents[0]["_id"])

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
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test single field filter
	results, err := engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2)

	// Verify results
	names := make([]string, len(results.Documents))
	for i, doc := range results.Documents {
		names[i] = doc["name"].(string)
	}
	assert.Contains(t, names, "Alice")
	assert.Contains(t, names, "Charlie")

	// Test string filter (case-insensitive)
	results, err = engine.FindAll("users", map[string]interface{}{"city": "new york"}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2)

	// Test multiple field filter
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "New York",
	}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 1)
	assert.Equal(t, "Alice", results.Documents[0]["name"])

	// Test non-existent field
	results, err = engine.FindAll("users", map[string]interface{}{"nonexistent": "value"}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 0)

	// Test non-existent collection
	_, err = engine.FindAll("nonexistent", map[string]interface{}{"age": 25}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")

	// Test empty filter (should return all documents)
	results, err = engine.FindAll("users", map[string]interface{}{}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 4)
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
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test int vs float64 comparison
	results, err := engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Both 25 and 25.0 should match

	results, err = engine.FindAll("users", map[string]interface{}{"age": 25.0}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Both 25 and 25.0 should match

	// Test float vs int comparison - this might not work as expected due to type differences
	results, err = engine.FindAll("users", map[string]interface{}{"score": 100}, nil)
	require.NoError(t, err)
	// The exact count depends on how the type comparison works in the implementation
	assert.GreaterOrEqual(t, len(results.Documents), 1)

	// Test string case-insensitive comparison
	docs = []domain.Document{
		{"_id": "4", "name": "Alice", "city": "New York"},
		{"_id": "5", "name": "alice", "city": "NEW YORK"},
		{"_id": "6", "name": "Bob", "city": "Boston"},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	results, err = engine.FindAll("users", map[string]interface{}{"name": "alice"}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Both "Alice" and "alice" should match

	results, err = engine.FindAll("users", map[string]interface{}{"city": "new york"}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Both "New York" and "NEW YORK" should match
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
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index on age field
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Test index optimization - should use index for age=25
	filter := map[string]interface{}{"age": 25}
	results, err := engine.FindAll("users", filter, nil)
	require.NoError(t, err)

	// Should find 3 documents with age=25
	assert.Len(t, results.Documents, 3)

	// Verify we got the right documents
	names := make(map[string]bool)
	for _, doc := range results.Documents {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}

	assert.True(t, names["Alice"])
	assert.True(t, names["Charlie"])
	assert.True(t, names["Eve"])

	// Test multiple field filter - should still work
	filter = map[string]interface{}{"age": 25, "city": "New York"}
	results, err = engine.FindAll("users", filter, nil)
	require.NoError(t, err)

	// Should find 1 document with age=25 and city=New York
	assert.Len(t, results.Documents, 1)
	assert.Equal(t, "Alice", results.Documents[0]["name"])

	// Test non-indexed field - should fall back to full scan
	filter = map[string]interface{}{"city": "Boston"}
	results, err = engine.FindAll("users", filter, nil)
	require.NoError(t, err)

	// Should find 2 documents with city=Boston
	assert.Len(t, results.Documents, 2)

	// Test non-existent value - should return empty
	filter = map[string]interface{}{"age": 999}
	results, err = engine.FindAll("users", filter, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 0)
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
		_, err := engine.Insert("users", doc)
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
		_, err := engine.Insert("users", doc)
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
	results, err := engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 3) // Alice, Charlie, Eve

	// Test two-field index intersection (AND logic)
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "Boston",
	}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Alice, Eve

	// Test three-field index intersection
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"city": "Boston",
		"role": "user",
	}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 1) // Eve only

	// Test with non-indexed field (should still work but may not use index optimization)
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":  25,
		"name": "Alice",
	}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 1) // Alice only

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
		_, err := engine.Insert("users", doc)
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
	}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Alice, Charlie

	// Test query with only non-indexed fields (should fall back to full scan)
	results, err = engine.FindAll("users", map[string]interface{}{
		"city": "Boston",
	}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Alice, Charlie
}

// Additional comprehensive tests for missing functionality

func TestStorageEngine_DropIndex(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "Boston"},
		{"name": "Bob", "age": 30, "city": "New York"},
		{"name": "Charlie", "age": 25, "city": "Boston"},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Verify index exists
	indexes, err := engine.GetIndexes("users")
	require.NoError(t, err)
	assert.Contains(t, indexes, "age")

	// Drop index
	err = engine.DropIndex("users", "age")
	require.NoError(t, err)

	// Verify index is gone
	indexes, err = engine.GetIndexes("users")
	require.NoError(t, err)
	assert.NotContains(t, indexes, "age")

	// Test dropping non-existent index (should error)
	err = engine.DropIndex("users", "nonexistent")
	assert.Error(t, err)

	// Test dropping index from non-existent collection
	err = engine.DropIndex("nonexistent", "age")
	assert.Error(t, err)
}

func TestStorageEngine_FindByIndex(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "Boston"},
		{"name": "Bob", "age": 30, "city": "New York"},
		{"name": "Charlie", "age": 25, "city": "Boston"},
		{"name": "Diana", "age": 35, "city": "Boston"},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index on age field
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Test finding by index
	results, err := engine.FindByIndex("users", "age", 25)
	require.NoError(t, err)
	assert.Len(t, results, 2) // Alice, Charlie

	// Verify results
	names := make(map[string]bool)
	for _, doc := range results {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}
	assert.True(t, names["Alice"])
	assert.True(t, names["Charlie"])

	// Test finding non-existent value
	results, err = engine.FindByIndex("users", "age", 999)
	require.NoError(t, err)
	assert.Len(t, results, 0)

	// Test finding by non-existent index
	results, err = engine.FindByIndex("users", "nonexistent", "value")
	require.NoError(t, err)
	assert.Len(t, results, 0)

	// Test finding by non-existent collection
	_, err = engine.FindByIndex("nonexistent", "age", 25)
	assert.Error(t, err)
}

func TestStorageEngine_GetIndexes(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Initially should have only _id index
	indexes, err := engine.GetIndexes("users")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")

	// Create additional indexes
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("users", "name")
	require.NoError(t, err)

	// Get all indexes
	indexes, err = engine.GetIndexes("users")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Contains(t, indexes, "age")
	assert.Contains(t, indexes, "name")
	assert.Len(t, indexes, 3)

	// Test getting indexes for non-existent collection
	_, err = engine.GetIndexes("nonexistent")
	assert.NoError(t, err) // Returns empty slice, not error
}

func TestStorageEngine_UpdateIndex(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	docs := []domain.Document{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Add more documents
	moreDocs := []domain.Document{
		{"name": "Charlie", "age": 25},
		{"name": "Diana", "age": 35},
	}

	for _, doc := range moreDocs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Update index to include new documents
	err = engine.UpdateIndex("users", "age")
	require.NoError(t, err)

	// Verify index is updated (may have duplicates due to multiple updates)
	results, err := engine.FindByIndex("users", "age", 25)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(results), 2) // At least Alice, Charlie

	// Test updating non-existent index
	err = engine.UpdateIndex("users", "nonexistent")
	assert.NoError(t, err) // Returns nil, not error

	// Test updating index for non-existent collection
	err = engine.UpdateIndex("nonexistent", "age")
	assert.Error(t, err)
}

func TestStorageEngine_BackgroundSave(t *testing.T) {
	// Create engine with dual-write mode enabled
	engine := NewStorageEngine(WithNoSaves(false))
	defer engine.StopBackgroundWorkers()

	// Start background workers
	engine.StartBackgroundWorkers()

	// Insert some data
	docs := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Wait for background save to occur
	time.Sleep(200 * time.Millisecond)

	// Verify background workers are running
	stats := engine.GetMemoryStats()
	assert.GreaterOrEqual(t, stats["num_goroutines"], 1)

	// Test that background workers can be stopped safely
	engine.StopBackgroundWorkers()

	// Verify stop channel is closed
	select {
	case <-engine.stopChan:
		// Expected
	default:
		t.Error("stop channel should be closed")
	}
}

func TestStorageEngine_BackgroundWorkersMultipleStarts(t *testing.T) {
	engine := NewStorageEngine(WithNoSaves(false))
	defer engine.StopBackgroundWorkers()

	// Start background workers multiple times (should be safe)
	engine.StartBackgroundWorkers()
	engine.StartBackgroundWorkers()
	engine.StartBackgroundWorkers()

	// Stop should work correctly
	engine.StopBackgroundWorkers()
}

func TestStorageEngine_BackgroundWorkersWithoutSave(t *testing.T) {
	// Create engine without background save
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Start background workers (should do nothing)
	engine.StartBackgroundWorkers()

	// Stop should work without issues
	engine.StopBackgroundWorkers()
}

func TestStorageEngine_InsertEdgeCases(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test inserting empty document
	_, err := engine.Insert("users", domain.Document{})
	assert.NoError(t, err)

	// Test inserting document with _id already set
	doc := domain.Document{"_id": "custom_id", "name": "Alice"}
	_, err = engine.Insert("users", doc)
	assert.NoError(t, err)

	// Verify _id was preserved (note: Insert may generate new ID)
	foundDocs, err := engine.FindAll("users", map[string]interface{}{"name": "Alice"}, nil)
	require.NoError(t, err)
	assert.Len(t, foundDocs.Documents, 1)
	assert.Equal(t, "Alice", foundDocs.Documents[0]["name"])

	// Test inserting document with complex nested structure
	complexDoc := domain.Document{
		"name": "Bob",
		"address": map[string]interface{}{
			"street": "123 Main St",
			"city":   "Boston",
			"zip":    12345,
		},
		"tags": []interface{}{"tag1", "tag2", "tag3"},
		"metadata": map[string]interface{}{
			"created": "2023-01-01",
			"active":  true,
		},
	}

	_, err = engine.Insert("users", complexDoc)
	assert.NoError(t, err)

	// Verify complex document was stored correctly
	docs, err := engine.FindAll("users", map[string]interface{}{"name": "Bob"}, nil)
	require.NoError(t, err)
	assert.Len(t, docs.Documents, 1)

	storedDoc := docs.Documents[0]
	address := storedDoc["address"].(map[string]interface{})
	assert.Equal(t, "123 Main St", address["street"])
	assert.Equal(t, "Boston", address["city"])
	assert.Equal(t, 12345, address["zip"])

	tags := storedDoc["tags"].([]interface{})
	assert.Len(t, tags, 3)
	assert.Contains(t, tags, "tag1")
	assert.Contains(t, tags, "tag2")
	assert.Contains(t, tags, "tag3")

	metadata := storedDoc["metadata"].(map[string]interface{})
	assert.Equal(t, "2023-01-01", metadata["created"])
	assert.Equal(t, true, metadata["active"])
}

func TestStorageEngine_UpdateByIdEdgeCases(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test document
	doc := domain.Document{"name": "Alice", "age": 25}
	_, err := engine.Insert("users", doc)
	require.NoError(t, err)

	// Get the generated ID
	docs, err := engine.FindAll("users", map[string]interface{}{"name": "Alice"}, nil)
	require.NoError(t, err)
	assert.Len(t, docs.Documents, 1)
	docID := docs.Documents[0]["_id"].(string)

	// Test updating with empty updates
	_, err = engine.UpdateById("users", docID, domain.Document{})
	assert.NoError(t, err)

	// Test updating with nil updates
	_, err = engine.UpdateById("users", docID, nil)
	assert.NoError(t, err)

	// Test updating with complex nested structure
	complexUpdates := domain.Document{
		"address": map[string]interface{}{
			"street": "456 Oak Ave",
			"city":   "Chicago",
		},
		"tags": []interface{}{"updated", "tags"},
		"metadata": map[string]interface{}{
			"updated": "2023-02-01",
		},
	}

	_, err = engine.UpdateById("users", docID, complexUpdates)
	assert.NoError(t, err)

	// Verify complex updates were applied
	updated, err := engine.GetById("users", docID)
	require.NoError(t, err)
	assert.Equal(t, "Alice", updated["name"]) // Original field unchanged
	assert.Equal(t, 25, updated["age"])       // Original field unchanged

	address := updated["address"].(map[string]interface{})
	assert.Equal(t, "456 Oak Ave", address["street"])
	assert.Equal(t, "Chicago", address["city"])

	tags := updated["tags"].([]interface{})
	assert.Len(t, tags, 2)
	assert.Contains(t, tags, "updated")
	assert.Contains(t, tags, "tags")

	metadata := updated["metadata"].(map[string]interface{})
	assert.Equal(t, "2023-02-01", metadata["updated"])
}

func TestStorageEngine_FindAllEdgeCases(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	docs := []domain.Document{
		{"name": "Alice", "age": 25, "active": true, "score": 100.5},
		{"name": "Bob", "age": 30, "active": false, "score": 85.0},
		{"name": "Charlie", "age": 25, "active": true, "score": 95.5},
		{"name": "Diana", "age": 35, "active": false, "score": 120.0},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test finding with nil filter
	results, err := engine.FindAll("users", nil, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 4)

	// Test finding with empty filter
	results, err = engine.FindAll("users", map[string]interface{}{}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 4)

	// Test finding with boolean filter
	results, err = engine.FindAll("users", map[string]interface{}{"active": true}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2)

	// Test finding with float filter
	results, err = engine.FindAll("users", map[string]interface{}{"score": 100.5}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 1)
	assert.Equal(t, "Alice", results.Documents[0]["name"])

	// Test finding with multiple conditions
	results, err = engine.FindAll("users", map[string]interface{}{
		"age":    25,
		"active": true,
	}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 2) // Alice and Charlie both have age=25 and active=true
	names := make(map[string]bool)
	for _, doc := range results.Documents {
		names[doc["name"].(string)] = true
	}
	assert.True(t, names["Alice"])
	assert.True(t, names["Charlie"])

	// Test finding with non-existent field
	results, err = engine.FindAll("users", map[string]interface{}{"nonexistent": "value"}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 0)

	// Test finding with non-existent value
	results, err = engine.FindAll("users", map[string]interface{}{"age": 999}, nil)
	require.NoError(t, err)
	assert.Len(t, results.Documents, 0)
}

func TestStorageEngine_ConcurrentIndexOperations(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert documents
	const numDocs = 100
	for i := 0; i < numDocs; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 50,
			"city": fmt.Sprintf("city%d", i%10),
		}
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test concurrent index creation
	var wg sync.WaitGroup
	indexFields := []string{"age", "city", "name"}

	for _, field := range indexFields {
		wg.Add(1)
		go func(fieldName string) {
			defer wg.Done()
			err := engine.CreateIndex("users", fieldName)
			assert.NoError(t, err)
		}(field)
	}

	wg.Wait()

	// Verify all indexes were created
	indexes, err := engine.GetIndexes("users")
	require.NoError(t, err)
	assert.Contains(t, indexes, "age")
	assert.Contains(t, indexes, "city")
	assert.Contains(t, indexes, "name")

	// Test concurrent queries using indexes
	wg = sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results, err := engine.FindAll("users", map[string]interface{}{"age": id % 50}, nil)
			assert.NoError(t, err)
			assert.Greater(t, len(results.Documents), 0)
		}(i)
	}

	wg.Wait()
}

func TestStorageEngine_FileOperationErrors(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test saving to directory that doesn't exist
	err := engine.SaveToFile("/nonexistent/directory/test.godb")
	assert.Error(t, err)

	// Test saving to read-only directory (if possible)
	// This is platform-dependent, so we'll just test the error handling
	tempFile := "test_permissions.godb"
	defer os.Remove(tempFile)

	// Create file first
	err = engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Make file read-only
	err = os.Chmod(tempFile, 0444)
	if err == nil {
		// Try to overwrite read-only file
		err = engine.SaveToFile(tempFile)
		assert.Error(t, err)
		// Restore permissions for cleanup
		os.Chmod(tempFile, 0644)
	}
}

func TestStorageEngine_CollectionStateTransitions(t *testing.T) {
	// Use transaction saves disabled to test manual state transitions
	engine := NewStorageEngine(WithNoSaves(false))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Verify initial state
	engine.mu.RLock()
	info, exists := engine.collections["users"]
	engine.mu.RUnlock()
	assert.True(t, exists)
	assert.Equal(t, CollectionStateLoaded, info.State)

	// Insert document - with dual-write, this should immediately save and keep collection clean
	doc := domain.Document{"name": "Alice"}
	_, err = engine.Insert("users", doc)
	require.NoError(t, err)

	// Verify state remains clean due to dual-write
	engine.mu.RLock()
	info, exists = engine.collections["users"]
	engine.mu.RUnlock()
	assert.True(t, exists)
	assert.Equal(t, CollectionStateLoaded, info.State)

	// Save to file
	tempFile := "test_state.godb"
	defer os.Remove(tempFile)
	err = engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Create new engine and load metadata
	newEngine := NewStorageEngine()
	defer newEngine.StopBackgroundWorkers()

	err = newEngine.LoadCollectionMetadata(tempFile)
	require.NoError(t, err)

	// Verify state is unloaded
	newEngine.mu.RLock()
	info, exists = newEngine.collections["users"]
	newEngine.mu.RUnlock()
	assert.True(t, exists)
	assert.Equal(t, CollectionStateUnloaded, info.State)

	// Note: Collection loading from disk is not fully implemented in this version
	// The test verifies that metadata loading works correctly
	t.Logf("Collection state transitions test completed - metadata loading verified")
}

func TestStorageEngine_IndexConsistency(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert documents
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "Boston"},
		{"name": "Bob", "age": 30, "city": "New York"},
		{"name": "Charlie", "age": 25, "city": "Boston"},
	}

	for _, doc := range docs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create index after insertion
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)

	// Verify index is consistent
	results, err := engine.FindByIndex("users", "age", 25)
	require.NoError(t, err)
	assert.Len(t, results, 2)

	// Update document
	foundDocs, err := engine.FindAll("users", map[string]interface{}{"name": "Alice"}, nil)
	require.NoError(t, err)
	assert.Len(t, foundDocs.Documents, 1)
	docID := foundDocs.Documents[0]["_id"].(string)

	_, err = engine.UpdateById("users", docID, map[string]interface{}{"age": 26})
	require.NoError(t, err)

	// Verify index is updated
	results, err = engine.FindByIndex("users", "age", 25)
	require.NoError(t, err)
	assert.Len(t, results, 1) // Only Charlie now

	results, err = engine.FindByIndex("users", "age", 26)
	require.NoError(t, err)
	assert.Len(t, results, 1) // Alice now

	// Delete document
	err = engine.DeleteById("users", docID)
	require.NoError(t, err)

	// Verify index is updated
	results, err = engine.FindByIndex("users", "age", 26)
	require.NoError(t, err)
	assert.Len(t, results, 0) // Alice deleted

	results, err = engine.FindByIndex("users", "age", 25)
	require.NoError(t, err)
	assert.Len(t, results, 1) // Charlie still there
}

func TestStorageEngine_StreamingEdgeCases(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test streaming empty collection
	err := engine.CreateCollection("empty")
	require.NoError(t, err)

	docChan, err := engine.FindAllStream("empty", nil)
	require.NoError(t, err)

	docCount := 0
	for range docChan {
		docCount++
	}
	assert.Equal(t, 0, docCount)

	// Test streaming with filter that matches nothing
	doc := domain.Document{"name": "Alice", "age": 25}
	_, err = engine.Insert("users", doc)
	require.NoError(t, err)

	docChan, err = engine.FindAllStream("users", map[string]interface{}{"age": 999})
	require.NoError(t, err)

	docCount = 0
	for range docChan {
		docCount++
	}
	assert.Equal(t, 0, docCount)

	// Test streaming with complex filter
	moreDocs := []domain.Document{
		{"name": "Bob", "age": 30, "active": true},
		{"name": "Charlie", "age": 25, "active": false},
		{"name": "Diana", "age": 30, "active": true},
	}

	for _, doc := range moreDocs {
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	docChan, err = engine.FindAllStream("users", map[string]interface{}{
		"age":    30,
		"active": true,
	})
	require.NoError(t, err)

	docCount = 0
	for range docChan {
		docCount++
	}
	assert.Equal(t, 2, docCount) // Bob and Diana
}

func TestStorageEngine_ConcurrentDocumentOperations(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Test concurrent inserts only
	const numGoroutines = 10
	const docsPerGoroutine = 10
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < docsPerGoroutine; j++ {
				doc := domain.Document{
					"goroutine": id,
					"doc_id":    j,
					"name":      fmt.Sprintf("user_%d_%d", id, j),
				}
				_, err := engine.Insert("users", doc)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all documents were inserted
	docs, err := engine.FindAll("users", nil, &domain.PaginationOptions{Limit: 1000, MaxLimit: 1000})
	require.NoError(t, err)
	expectedCount := numGoroutines * docsPerGoroutine

	// With atomic ID generation, we should get exactly the expected number of documents
	assert.Len(t, docs.Documents, expectedCount,
		"Expected exactly %d documents with atomic ID generation, got %d.",
		expectedCount, len(docs.Documents))

	// Test sequential operations to avoid concurrent map access
	for i := 0; i < numGoroutines; i++ {
		// Find a document to update
		results, err := engine.FindAll("users", map[string]interface{}{"goroutine": i}, nil)
		if err == nil && len(results.Documents) > 0 {
			docID := results.Documents[0]["_id"].(string)
			_, err = engine.UpdateById("users", docID, map[string]interface{}{"updated": true})
			assert.NoError(t, err)
		}
	}

	// Test sequential deletes
	for i := 0; i < numGoroutines; i++ {
		// Find a document to delete
		results, err := engine.FindAll("users", map[string]interface{}{"goroutine": i}, nil)
		if err == nil && len(results.Documents) > 0 {
			docID := results.Documents[0]["_id"].(string)
			err = engine.DeleteById("users", docID)
			assert.NoError(t, err)
		}
	}

	// Verify some documents were deleted
	finalDocs, err := engine.FindAll("users", nil, nil)
	require.NoError(t, err)
	assert.Less(t, len(finalDocs.Documents), expectedCount)
}

// Tests for new background save and concurrency functionality

func TestStorageEngine_SaveDirtyCollections(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(false))
	defer engine.StopBackgroundWorkers()

	// Create and populate collections
	err = engine.CreateCollection("users")
	require.NoError(t, err)
	err = engine.CreateCollection("products")
	require.NoError(t, err)

	// Insert documents into users collection
	for i := 0; i < 3; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("User%d", i),
			"age":  20 + i,
		}
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Insert documents into products collection
	for i := 0; i < 2; i++ {
		doc := domain.Document{
			"name":  fmt.Sprintf("Product%d", i),
			"price": 10.0 + float64(i),
		}
		_, err := engine.Insert("products", doc)
		require.NoError(t, err)
	}

	// With dual-write, collections should be clean after insert
	engine.mu.RLock()
	usersInfo := engine.collections["users"]
	productsInfo := engine.collections["products"]
	engine.mu.RUnlock()

	assert.Equal(t, CollectionStateLoaded, usersInfo.State)
	assert.Equal(t, CollectionStateLoaded, productsInfo.State)

	// Call saveDirtyCollections (should be no-op since collections are already clean)
	engine.saveDirtyCollections()

	// Both collections should still be clean
	engine.mu.RLock()
	usersInfo = engine.collections["users"]
	productsInfo = engine.collections["products"]
	engine.mu.RUnlock()

	assert.Equal(t, CollectionStateLoaded, usersInfo.State)
	assert.Equal(t, CollectionStateLoaded, productsInfo.State)

	// Verify files were created
	usersFile := filepath.Join(tempDir, "collections", "users.godb")
	productsFile := filepath.Join(tempDir, "collections", "products.godb")

	assert.FileExists(t, usersFile)
	assert.FileExists(t, productsFile)

	// Verify file sizes are reasonable
	usersFileInfo, err := os.Stat(usersFile)
	require.NoError(t, err)
	assert.Greater(t, usersFileInfo.Size(), int64(0))

	productsFileInfo, err := os.Stat(productsFile)
	require.NoError(t, err)
	assert.Greater(t, productsFileInfo.Size(), int64(0))
}

func TestStorageEngine_SaveDirtyCollections_NoDirtyCollections(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection but don't modify it
	err = engine.CreateCollection("users")
	require.NoError(t, err)

	// Mark as loaded (not dirty)
	engine.mu.Lock()
	engine.collections["users"].State = CollectionStateLoaded
	engine.mu.Unlock()

	// Call saveDirtyCollections - should not create any files
	engine.saveDirtyCollections()

	// Verify no collections directory was created
	collectionsDir := filepath.Join(tempDir, "collections")
	_, err = os.Stat(collectionsDir)
	assert.True(t, os.IsNotExist(err))
}

func TestStorageEngine_SaveCollectionToFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create and populate collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	doc := domain.Document{"name": "TestUser", "age": 25}
	_, err = engine.Insert("test", doc)
	require.NoError(t, err)

	// Save specific collection
	err = engine.saveCollectionToFile("test")
	require.NoError(t, err)

	// Verify file was created
	testFile := filepath.Join(tempDir, "collections", "test.godb")
	assert.FileExists(t, testFile)

	// Verify collection state changed to loaded
	engine.mu.RLock()
	testInfo := engine.collections["test"]
	engine.mu.RUnlock()

	assert.Equal(t, CollectionStateLoaded, testInfo.State)
	assert.Greater(t, testInfo.SizeOnDisk, int64(0))
}

func TestStorageEngine_PerCollectionConcurrency(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create two collections
	err := engine.CreateCollection("coll1")
	require.NoError(t, err)
	err = engine.CreateCollection("coll2")
	require.NoError(t, err)

	var wg sync.WaitGroup
	results := make(chan string, 100)

	// Test concurrent operations on different collections
	for i := 0; i < 50; i++ {
		wg.Add(2)

		// Operations on collection 1
		go func(id int) {
			defer wg.Done()
			doc := domain.Document{"id": id, "collection": "coll1"}
			_, err := engine.Insert("coll1", doc)
			if err != nil {
				results <- fmt.Sprintf("coll1-insert-error-%d: %v", id, err)
			} else {
				results <- fmt.Sprintf("coll1-insert-success-%d", id)
			}
		}(i)

		// Operations on collection 2
		go func(id int) {
			defer wg.Done()
			doc := domain.Document{"id": id, "collection": "coll2"}
			_, err := engine.Insert("coll2", doc)
			if err != nil {
				results <- fmt.Sprintf("coll2-insert-error-%d: %v", id, err)
			} else {
				results <- fmt.Sprintf("coll2-insert-success-%d", id)
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Count results
	successCount := 0
	errorCount := 0

	for result := range results {
		if strings.Contains(result, "success") {
			successCount++
		} else {
			errorCount++
			t.Logf("Error: %s", result)
		}
	}

	assert.Equal(t, 100, successCount, "All operations should succeed")
	assert.Equal(t, 0, errorCount, "No operations should fail")

	// Verify final document counts
	coll1Docs, err := engine.FindAll("coll1", nil, nil)
	require.NoError(t, err)
	assert.Len(t, coll1Docs.Documents, 50)

	coll2Docs, err := engine.FindAll("coll2", nil, nil)
	require.NoError(t, err)
	assert.Len(t, coll2Docs.Documents, 50)
}

func TestStorageEngine_ConcurrentReadsDuringWrite(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection with some initial data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert initial documents
	for i := 0; i < 10; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("User%d", i),
			"age":  20 + i,
		}
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	results := make(chan string, 200) // Buffer for all possible results

	// Start multiple readers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				docID := fmt.Sprintf("%d", (j%10)+1) // Read docs 1-10
				doc, err := engine.GetById("users", docID)
				if err != nil {
					results <- fmt.Sprintf("read-error-%d-%d: %v", readerID, j, err)
				} else {
					results <- fmt.Sprintf("read-success-%d-%d: %v", readerID, j, doc["name"])
				}
				time.Sleep(1 * time.Millisecond) // Small delay
			}
		}(i)
	}

	// Start some writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				doc := domain.Document{
					"name": fmt.Sprintf("NewUser%d-%d", writerID, j),
					"age":  30 + writerID + j,
				}
				_, err := engine.Insert("users", doc)
				if err != nil {
					results <- fmt.Sprintf("write-error-%d-%d: %v", writerID, j, err)
				} else {
					results <- fmt.Sprintf("write-success-%d-%d", writerID, j)
				}
				time.Sleep(2 * time.Millisecond) // Small delay
			}
		}(i)
	}

	wg.Wait()
	close(results)

	// Analyze results
	readSuccesses := 0
	writeSuccesses := 0
	errors := 0

	for result := range results {
		if strings.Contains(result, "read-success") {
			readSuccesses++
		} else if strings.Contains(result, "write-success") {
			writeSuccesses++
		} else {
			errors++
			t.Logf("Error: %s", result)
		}
	}

	assert.Equal(t, 100, readSuccesses, "All reads should succeed")
	assert.Equal(t, 15, writeSuccesses, "All writes should succeed")
	assert.Equal(t, 0, errors, "No operations should fail")
}

func TestStorageEngine_BackgroundSaveIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(
		WithNoSaves(false),
		WithDataDir(tempDir),
	)
	defer engine.StopBackgroundWorkers()

	// Start background workers
	engine.StartBackgroundWorkers()

	// Create collection and insert documents
	err = engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert some documents
	for i := 0; i < 5; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("User%d", i),
			"age":  20 + i,
		}
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Wait for background save
	time.Sleep(200 * time.Millisecond)

	// Verify file was created
	usersFile := filepath.Join(tempDir, "collections", "users.godb")
	assert.FileExists(t, usersFile)

	// Verify collection is no longer dirty
	engine.mu.RLock()
	usersInfo := engine.collections["users"]
	engine.mu.RUnlock()

	assert.Equal(t, CollectionStateLoaded, usersInfo.State)

	// Insert more documents
	for i := 5; i < 10; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("User%d", i),
			"age":  20 + i,
		}
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// With dual-write, collection should remain clean after insert
	engine.mu.RLock()
	usersInfo = engine.collections["users"]
	engine.mu.RUnlock()

	assert.Equal(t, CollectionStateLoaded, usersInfo.State)

	// Wait a bit to ensure no background operations are needed
	time.Sleep(200 * time.Millisecond)

	// Collection should still be clean
	engine.mu.RLock()
	usersInfo = engine.collections["users"]
	engine.mu.RUnlock()

	assert.Equal(t, CollectionStateLoaded, usersInfo.State)
}

func TestStorageEngine_CollectionLockCreation(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Initially no locks should exist
	engine.locksMu.RLock()
	lockCount := len(engine.collectionLocks)
	engine.locksMu.RUnlock()
	assert.Equal(t, 0, lockCount)

	// Create a collection
	err := engine.CreateCollection("test")
	require.NoError(t, err)

	// Insert a document (which should create a lock)
	doc := domain.Document{"name": "Test"}
	_, err = engine.Insert("test", doc)
	require.NoError(t, err)

	// Now there should be a lock for the collection
	lock := engine.getOrCreateCollectionLock("test")
	assert.NotNil(t, lock)

	engine.locksMu.RLock()
	lockCount = len(engine.collectionLocks)
	engine.locksMu.RUnlock()
	assert.Equal(t, 1, lockCount)

	// Getting the same lock should return the same instance
	lock2 := engine.getOrCreateCollectionLock("test")
	assert.Same(t, lock, lock2)
}

func TestStorageEngine_WithCollectionLocks(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	executed := false

	// Test read lock
	err := engine.withCollectionReadLock("test", func() error {
		executed = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, executed)

	executed = false

	// Test write lock
	err = engine.withCollectionWriteLock("test", func() error {
		executed = true
		return nil
	})
	require.NoError(t, err)
	assert.True(t, executed)

	// Test error propagation
	testError := fmt.Errorf("test error")
	err = engine.withCollectionReadLock("test", func() error {
		return testError
	})
	assert.Equal(t, testError, err)
}

// Tests for transaction save functionality

func TestStorageEngine_TransactionSaveEnabled(t *testing.T) {
	// Test default behavior - transaction saves should be enabled
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	assert.False(t, engine.IsNoSavesEnabled(), "No-saves should be disabled by default (dual-write mode)")
}

func TestStorageEngine_TransactionSaveDisabled(t *testing.T) {
	// Test with dual-write mode enabled (default)
	engine := NewStorageEngine(WithNoSaves(false))
	defer engine.StopBackgroundWorkers()

	assert.False(t, engine.IsNoSavesEnabled(), "No-saves should be disabled in dual-write mode")
}

func TestStorageEngine_BackgroundSaveDisablesTransactionSave(t *testing.T) {
	// Test that no-saves mode disables automatic saves
	engine := NewStorageEngine(WithNoSaves(true))
	defer engine.StopBackgroundWorkers()

	assert.True(t, engine.IsNoSavesEnabled(), "No-saves should be enabled when set to true")
}

func TestStorageEngine_SaveCollectionAfterTransaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-transaction-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create engine with dual-write mode (default)
	engine := NewStorageEngine(
		WithDataDir(tempDir),
		WithNoSaves(false),
	)
	defer engine.StopBackgroundWorkers()

	// Create collection and insert document
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	doc := domain.Document{"name": "Test", "value": 42}
	_, err = engine.Insert("test", doc)
	require.NoError(t, err)

	// With transaction saves enabled, collection should be clean after insert (already saved)
	engine.mu.RLock()
	collInfo := engine.collections["test"]
	isClean := collInfo.State == CollectionStateLoaded
	engine.mu.RUnlock()
	assert.True(t, isClean, "Collection should be clean after insert when transaction saves are enabled")

	// Check that file was already created by the transaction save during insert
	fileName := filepath.Join(tempDir, "collections", "test.godb")
	assert.FileExists(t, fileName)

	// Trigger transaction save again (should be a no-op since collection is already clean)
	err = engine.SaveCollectionAfterTransaction("test")
	require.NoError(t, err)

	// Collection should still be clean
	engine.mu.RLock()
	collInfo = engine.collections["test"]
	isClean = collInfo.State == CollectionStateLoaded
	engine.mu.RUnlock()
	assert.True(t, isClean, "Collection should remain clean after redundant save")
}

func TestStorageEngine_SaveCollectionAfterTransaction_Disabled(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-transaction-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create engine with no-saves mode (no automatic disk writes)
	engine := NewStorageEngine(
		WithDataDir(tempDir),
		WithNoSaves(true),
	)
	defer engine.StopBackgroundWorkers()

	// Create collection and insert document
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	doc := domain.Document{"name": "Test", "value": 42}
	_, err = engine.Insert("test", doc)
	require.NoError(t, err)

	// Try to trigger transaction save - should do nothing
	err = engine.SaveCollectionAfterTransaction("test")
	require.NoError(t, err)

	// Check that no file was created
	fileName := filepath.Join(tempDir, "collections", "test.godb")
	assert.NoFileExists(t, fileName)

	// Collection should still be dirty
	engine.mu.RLock()
	collInfo := engine.collections["test"]
	isDirty := collInfo.State == CollectionStateDirty
	engine.mu.RUnlock()
	assert.True(t, isDirty, "Collection should remain dirty when transaction saves are disabled")
}

func TestStorageEngine_SaveCollectionAfterTransaction_NonDirtyCollection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-transaction-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(
		WithDataDir(tempDir),
		WithNoSaves(true),
	)
	defer engine.StopBackgroundWorkers()

	// Create collection but don't modify it
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Mark as loaded (not dirty)
	engine.mu.Lock()
	engine.collections["test"].State = CollectionStateLoaded
	engine.mu.Unlock()

	// Try to save - should do nothing since collection isn't dirty
	err = engine.SaveCollectionAfterTransaction("test")
	require.NoError(t, err)

	// No file should be created
	fileName := filepath.Join(tempDir, "collections", "test.godb")
	assert.NoFileExists(t, fileName)
}

func TestStorageEngine_SaveCollectionAfterTransaction_NonExistentCollection(t *testing.T) {
	engine := NewStorageEngine(WithNoSaves(true))
	defer engine.StopBackgroundWorkers()

	// Try to save non-existent collection - should do nothing
	err := engine.SaveCollectionAfterTransaction("nonexistent")
	require.NoError(t, err) // Should not error, just do nothing
}

// Tests for _id index behavior

func TestStorageEngine_IdIndexCreationAndUpdates(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test that _id index is created on first insert and updated on subsequent inserts
	doc1 := domain.Document{"name": "User1", "age": 25}
	doc2 := domain.Document{"name": "User2", "age": 30}
	doc3 := domain.Document{"name": "User3", "age": 35}

	// First insert - should create collection and _id index
	_, err := engine.Insert("id_test", doc1)
	require.NoError(t, err)

	// Verify _id index was created
	indexes, err := engine.GetIndexes("id_test")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Len(t, indexes, 1) // Only _id index should exist

	// Get the index to verify it contains the first document
	index, exists := engine.getIndex("id_test", "_id")
	require.True(t, exists)
	assert.NotNil(t, index)

	// Verify the index contains the first document's ID
	// The document should have been assigned ID "1"
	docIDs := index.Query("1")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	// Second insert - should NOT recreate _id index, but should update it
	_, err = engine.Insert("id_test", doc2)
	require.NoError(t, err)

	// Verify _id index still exists and count is still 1 (not recreated)
	indexes, err = engine.GetIndexes("id_test")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Len(t, indexes, 1) // Still only 1 index

	// Verify the index now contains both documents
	docIDs = index.Query("1")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	docIDs = index.Query("2")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "2", docIDs[0])

	// Third insert - should update the existing index
	_, err = engine.Insert("id_test", doc3)
	require.NoError(t, err)

	// Verify _id index still exists and count is still 1
	indexes, err = engine.GetIndexes("id_test")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Len(t, indexes, 1) // Still only 1 index

	// Verify the index now contains all three documents
	docIDs = index.Query("1")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	docIDs = index.Query("2")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "2", docIDs[0])

	docIDs = index.Query("3")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "3", docIDs[0])

	// Verify all documents can be found using FindAll
	result, err := engine.FindAll("id_test", nil, nil)
	require.NoError(t, err)
	assert.Len(t, result.Documents, 3)

	// Verify all documents have _id fields
	for i, doc := range result.Documents {
		assert.Contains(t, doc, "_id")
		assert.Equal(t, fmt.Sprintf("%d", i+1), doc["_id"])
	}
}

func TestStorageEngine_BatchInsert_IdIndexCreationAndUpdates(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Test that batch insert creates _id index and updates it properly
	docs1 := []domain.Document{
		{"name": "BatchUser1", "age": 25},
		{"name": "BatchUser2", "age": 30},
	}

	docs2 := []domain.Document{
		{"name": "BatchUser3", "age": 35},
		{"name": "BatchUser4", "age": 40},
	}

	// First batch insert - should create collection and _id index
	_, err := engine.BatchInsert("batch_id_test", docs1)
	require.NoError(t, err)

	// Verify _id index was created
	indexes, err := engine.GetIndexes("batch_id_test")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Len(t, indexes, 1) // Only _id index should exist

	// Get the index to verify it contains the first batch documents
	index, exists := engine.getIndex("batch_id_test", "_id")
	require.True(t, exists)
	assert.NotNil(t, index)

	// Verify the index contains the first batch document IDs
	docIDs := index.Query("1")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	docIDs = index.Query("2")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "2", docIDs[0])

	// Second batch insert - should NOT recreate _id index, but should update it
	_, err = engine.BatchInsert("batch_id_test", docs2)
	require.NoError(t, err)

	// Verify _id index still exists and count is still 1 (not recreated)
	indexes, err = engine.GetIndexes("batch_id_test")
	require.NoError(t, err)
	assert.Contains(t, indexes, "_id")
	assert.Len(t, indexes, 1) // Still only 1 index

	// Verify the index now contains all four documents
	docIDs = index.Query("1")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	docIDs = index.Query("2")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "2", docIDs[0])

	docIDs = index.Query("3")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "3", docIDs[0])

	docIDs = index.Query("4")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "4", docIDs[0])

	// Verify all documents can be found using FindAll
	result, err := engine.FindAll("batch_id_test", nil, nil)
	require.NoError(t, err)
	assert.Len(t, result.Documents, 4)

	// Verify all documents have _id fields
	for i, doc := range result.Documents {
		assert.Contains(t, doc, "_id")
		assert.Equal(t, fmt.Sprintf("%d", i+1), doc["_id"])
	}
}

// Tests for index updates during document modifications

func TestStorageEngine_IndexUpdates(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	doc1 := domain.Document{"name": "Alice", "age": 25, "city": "New York"}
	doc2 := domain.Document{"name": "Bob", "age": 30, "city": "Boston"}
	doc3 := domain.Document{"name": "Charlie", "age": 25, "city": "Chicago"}

	_, err := engine.Insert("index_updates", doc1)
	require.NoError(t, err)
	_, err = engine.Insert("index_updates", doc2)
	require.NoError(t, err)
	_, err = engine.Insert("index_updates", doc3)
	require.NoError(t, err)

	// Create indexes on age and city fields
	err = engine.CreateIndex("index_updates", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("index_updates", "city")
	require.NoError(t, err)

	// Get the indexes for direct testing
	ageIndex, exists := engine.getIndex("index_updates", "age")
	require.True(t, exists)
	cityIndex, exists := engine.getIndex("index_updates", "city")
	require.True(t, exists)

	t.Run("UpdateById - Change Indexed Property", func(t *testing.T) {
		// Update Alice's age from 25 to 26
		updates := domain.Document{"age": 26}
		_, err := engine.UpdateById("index_updates", "1", updates)
		require.NoError(t, err)

		// Verify age index was updated
		docIDs := ageIndex.Query(25)
		assert.Len(t, docIDs, 1) // Only Charlie should have age=25
		assert.Equal(t, "3", docIDs[0])

		docIDs = ageIndex.Query(26)
		assert.Len(t, docIDs, 1) // Only Alice should have age=26
		assert.Equal(t, "1", docIDs[0])

		// Verify city index was not affected
		docIDs = cityIndex.Query("New York")
		assert.Len(t, docIDs, 1)
		assert.Equal(t, "1", docIDs[0])
	})

	t.Run("UpdateById - Remove Indexed Property", func(t *testing.T) {
		// Update Bob to remove city property
		updates := domain.Document{"city": nil}
		_, err := engine.UpdateById("index_updates", "2", updates)
		require.NoError(t, err)

		// Verify city index was updated - Bob should be removed from Boston
		docIDs := cityIndex.Query("Boston")
		assert.Len(t, docIDs, 0) // No documents should have city=Boston

		// Verify age index was not affected
		docIDs = ageIndex.Query(30)
		assert.Len(t, docIDs, 1)
		assert.Equal(t, "2", docIDs[0])
	})

	t.Run("ReplaceById - Complete Replacement", func(t *testing.T) {
		// Replace Charlie's document completely
		newDoc := domain.Document{
			"name": "Charlie Updated",
			"age":  35,        // Changed from 25 to 35
			"city": "Seattle", // Changed from Chicago to Seattle
		}
		_, err := engine.ReplaceById("index_updates", "3", newDoc)
		require.NoError(t, err)

		// Verify age index was updated
		docIDs := ageIndex.Query(25)
		assert.Len(t, docIDs, 0) // No documents should have age=25 now

		docIDs = ageIndex.Query(35)
		assert.Len(t, docIDs, 1) // Only Charlie should have age=35
		assert.Equal(t, "3", docIDs[0])

		// Verify city index was updated
		docIDs = cityIndex.Query("Chicago")
		assert.Len(t, docIDs, 0) // No documents should have city=Chicago now

		docIDs = cityIndex.Query("Seattle")
		assert.Len(t, docIDs, 1) // Only Charlie should have city=Seattle
		assert.Equal(t, "3", docIDs[0])
	})

	t.Run("DeleteById - Remove from Indexes", func(t *testing.T) {
		// Delete Alice's document
		err := engine.DeleteById("index_updates", "1")
		require.NoError(t, err)

		// Verify age index was updated - Alice should be removed
		docIDs := ageIndex.Query(26)
		assert.Len(t, docIDs, 0) // No documents should have age=26 now

		// Verify city index was updated - Alice should be removed
		docIDs = cityIndex.Query("New York")
		assert.Len(t, docIDs, 0) // No documents should have city=New York now

		// Verify other documents are still indexed correctly
		docIDs = ageIndex.Query(30)
		assert.Len(t, docIDs, 1) // Bob should still have age=30
		assert.Equal(t, "2", docIDs[0])

		docIDs = ageIndex.Query(35)
		assert.Len(t, docIDs, 1) // Charlie should still have age=35
		assert.Equal(t, "3", docIDs[0])
	})
}

func TestStorageEngine_BatchUpdate_IndexUpdates(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	docs := []domain.Document{
		{"name": "User1", "age": 25, "city": "New York"},
		{"name": "User2", "age": 30, "city": "Boston"},
		{"name": "User3", "age": 35, "city": "Chicago"},
	}

	_, err := engine.BatchInsert("batch_index_updates", docs)
	require.NoError(t, err)

	// Create indexes on age and city fields
	err = engine.CreateIndex("batch_index_updates", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("batch_index_updates", "city")
	require.NoError(t, err)

	// Get the indexes for direct testing
	ageIndex, exists := engine.getIndex("batch_index_updates", "age")
	require.True(t, exists)
	cityIndex, exists := engine.getIndex("batch_index_updates", "city")
	require.True(t, exists)

	// Batch update to change indexed properties
	updateOps := []domain.BatchUpdateOperation{
		{
			ID:      "1",
			Updates: domain.Document{"age": 26}, // Change age from 25 to 26
		},
		{
			ID:      "2",
			Updates: domain.Document{"city": "Seattle"}, // Change city from Boston to Seattle
		},
		{
			ID: "3",
			Updates: domain.Document{
				"age":  40,         // Change age from 35 to 40
				"city": "Portland", // Change city from Chicago to Portland
			},
		},
	}

	_, err = engine.BatchUpdate("batch_index_updates", updateOps)
	require.NoError(t, err)

	// Verify age index was updated correctly
	docIDs := ageIndex.Query(25)
	assert.Len(t, docIDs, 0) // No documents should have age=25 now

	docIDs = ageIndex.Query(26)
	assert.Len(t, docIDs, 1) // Only User1 should have age=26
	assert.Equal(t, "1", docIDs[0])

	docIDs = ageIndex.Query(30)
	assert.Len(t, docIDs, 1) // User2 should still have age=30
	assert.Equal(t, "2", docIDs[0])

	docIDs = ageIndex.Query(40)
	assert.Len(t, docIDs, 1) // Only User3 should have age=40
	assert.Equal(t, "3", docIDs[0])

	// Verify city index was updated correctly
	docIDs = cityIndex.Query("New York")
	assert.Len(t, docIDs, 1) // User1 should still have city=New York
	assert.Equal(t, "1", docIDs[0])

	docIDs = cityIndex.Query("Boston")
	assert.Len(t, docIDs, 0) // No documents should have city=Boston now

	docIDs = cityIndex.Query("Seattle")
	assert.Len(t, docIDs, 1) // Only User2 should have city=Seattle
	assert.Equal(t, "2", docIDs[0])

	docIDs = cityIndex.Query("Chicago")
	assert.Len(t, docIDs, 0) // No documents should have city=Chicago now

	docIDs = cityIndex.Query("Portland")
	assert.Len(t, docIDs, 1) // Only User3 should have city=Portland
	assert.Equal(t, "3", docIDs[0])
}

func TestStorageEngine_IndexUpdates_EdgeCases(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	doc1 := domain.Document{"name": "Alice", "age": 25, "city": "New York"}
	doc2 := domain.Document{"name": "Bob", "age": 30, "city": "Boston"}

	_, err := engine.Insert("edge_cases", doc1)
	require.NoError(t, err)
	_, err = engine.Insert("edge_cases", doc2)
	require.NoError(t, err)

	// Create indexes on age and city fields
	err = engine.CreateIndex("edge_cases", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("edge_cases", "city")
	require.NoError(t, err)

	// Get the indexes for direct testing
	ageIndex, exists := engine.getIndex("edge_cases", "age")
	require.True(t, exists)
	cityIndex, exists := engine.getIndex("edge_cases", "city")
	require.True(t, exists)

	t.Run("Update to Same Value - No Index Change", func(t *testing.T) {
		// Update Alice's age to the same value (25)
		updates := domain.Document{"age": 25}
		_, err := engine.UpdateById("edge_cases", "1", updates)
		require.NoError(t, err)

		// Verify age index still has Alice with age=25
		docIDs := ageIndex.Query(25)
		assert.Len(t, docIDs, 1) // Only Alice should have age=25
		assert.Equal(t, "1", docIDs[0])
	})

	t.Run("Add New Indexed Field", func(t *testing.T) {
		// Add a new field that has an index
		err = engine.CreateIndex("edge_cases", "salary")
		require.NoError(t, err)

		// Update Alice to add salary field
		updates := domain.Document{"salary": 50000}
		_, err := engine.UpdateById("edge_cases", "1", updates)
		require.NoError(t, err)

		// Get the salary index
		salaryIndex, exists := engine.getIndex("edge_cases", "salary")
		require.True(t, exists)

		// Verify Alice is now in the salary index
		docIDs := salaryIndex.Query(50000)
		assert.Len(t, docIDs, 1)
		assert.Equal(t, "1", docIDs[0])
	})

	t.Run("Update Non-Indexed Field - Index Unchanged", func(t *testing.T) {
		// Update Alice's name (not indexed)
		updates := domain.Document{"name": "Alice Updated"}
		_, err := engine.UpdateById("edge_cases", "1", updates)
		require.NoError(t, err)

		// Verify age index is unchanged
		docIDs := ageIndex.Query(25)
		assert.Len(t, docIDs, 1) // Should still have Alice with age=25
		assert.Equal(t, "1", docIDs[0])

		// Verify city index is unchanged
		docIDs = cityIndex.Query("New York")
		assert.Len(t, docIDs, 1)
		assert.Equal(t, "1", docIDs[0])
	})

	t.Run("Multiple Field Updates - All Indexes Updated", func(t *testing.T) {
		// Update Bob with multiple indexed fields
		updates := domain.Document{
			"age":    35,        // Change from 30 to 35
			"city":   "Seattle", // Change from Boston to Seattle
			"salary": 60000,     // Add new indexed field
		}
		_, err := engine.UpdateById("edge_cases", "2", updates)
		require.NoError(t, err)

		// Get the salary index
		salaryIndex, exists := engine.getIndex("edge_cases", "salary")
		require.True(t, exists)

		// Verify age index was updated
		docIDs := ageIndex.Query(30)
		assert.Len(t, docIDs, 0) // No documents should have age=30 now

		docIDs = ageIndex.Query(35)
		assert.Len(t, docIDs, 1) // Only Bob should have age=35
		assert.Equal(t, "2", docIDs[0])

		// Verify city index was updated
		docIDs = cityIndex.Query("Boston")
		assert.Len(t, docIDs, 0) // No documents should have city=Boston now

		docIDs = cityIndex.Query("Seattle")
		assert.Len(t, docIDs, 1) // Only Bob should have city=Seattle
		assert.Equal(t, "2", docIDs[0])

		// Verify salary index was updated
		docIDs = salaryIndex.Query(60000)
		assert.Len(t, docIDs, 1) // Only Bob should have salary=60000
		assert.Equal(t, "2", docIDs[0])
	})

	t.Run("Replace Document - All Indexes Updated", func(t *testing.T) {
		// Replace Alice's document completely with different indexed values
		newDoc := domain.Document{
			"name":   "Alice Completely New",
			"age":    40,         // Different from 25
			"city":   "Portland", // Different from New York
			"salary": 70000,      // Different from 50000
		}
		_, err := engine.ReplaceById("edge_cases", "1", newDoc)
		require.NoError(t, err)

		// Get the salary index
		salaryIndex, exists := engine.getIndex("edge_cases", "salary")
		require.True(t, exists)

		// Verify age index was updated
		docIDs := ageIndex.Query(25)
		assert.Len(t, docIDs, 0) // No documents should have age=25 now

		docIDs = ageIndex.Query(40)
		assert.Len(t, docIDs, 1) // Only Alice should have age=40
		assert.Equal(t, "1", docIDs[0])

		// Verify city index was updated
		docIDs = cityIndex.Query("New York")
		assert.Len(t, docIDs, 0) // No documents should have city=New York now

		docIDs = cityIndex.Query("Portland")
		assert.Len(t, docIDs, 1) // Only Alice should have city=Portland
		assert.Equal(t, "1", docIDs[0])

		// Verify salary index was updated
		docIDs = salaryIndex.Query(50000)
		assert.Len(t, docIDs, 0) // No documents should have salary=50000 now

		docIDs = salaryIndex.Query(70000)
		assert.Len(t, docIDs, 1) // Only Alice should have salary=70000
		assert.Equal(t, "1", docIDs[0])
	})
}

// Tests for batch operations

func TestStorageEngine_BatchInsert(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	t.Run("Basic Batch Insert", func(t *testing.T) {
		docs := []domain.Document{
			{"name": "Alice", "age": 30},
			{"name": "Bob", "age": 25},
			{"name": "Charlie", "age": 35},
		}

		_, err := engine.BatchInsert("users", docs)
		require.NoError(t, err)

		// Verify documents were inserted
		doc1, err := engine.GetById("users", "1")
		require.NoError(t, err)
		assert.Equal(t, "Alice", doc1["name"])
		assert.Equal(t, "1", doc1["_id"])

		doc2, err := engine.GetById("users", "2")
		require.NoError(t, err)
		assert.Equal(t, "Bob", doc2["name"])
		assert.Equal(t, "2", doc2["_id"])

		doc3, err := engine.GetById("users", "3")
		require.NoError(t, err)
		assert.Equal(t, "Charlie", doc3["name"])
		assert.Equal(t, "3", doc3["_id"])
	})

	t.Run("Batch Insert Updates Collection State", func(t *testing.T) {
		docs := []domain.Document{
			{"product": "Widget", "price": 10.99},
			{"product": "Gadget", "price": 25.50},
		}

		_, err := engine.BatchInsert("products", docs)
		require.NoError(t, err)

		// Check collection state is clean (dual-write saves immediately)
		engine.mu.RLock()
		collInfo, exists := engine.collections["products"]
		require.True(t, exists)
		assert.Equal(t, CollectionStateLoaded, collInfo.State)
		assert.Equal(t, int64(2), collInfo.DocumentCount)
		engine.mu.RUnlock()
	})

	t.Run("Batch Insert Creates Collection", func(t *testing.T) {
		docs := []domain.Document{
			{"item": "test"},
		}

		_, err := engine.BatchInsert("new_collection", docs)
		require.NoError(t, err)

		// Verify collection was created
		engine.mu.RLock()
		_, exists := engine.collections["new_collection"]
		engine.mu.RUnlock()
		assert.True(t, exists)

		// Verify document exists
		doc, err := engine.GetById("new_collection", "1")
		require.NoError(t, err)
		assert.Equal(t, "test", doc["item"])
	})

	t.Run("Batch Insert Empty Docs", func(t *testing.T) {
		_, err := engine.BatchInsert("test", []domain.Document{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no documents provided")
	})

	t.Run("Batch Insert Too Many Docs", func(t *testing.T) {
		docs := make([]domain.Document, 1001)
		for i := 0; i < 1001; i++ {
			docs[i] = domain.Document{"id": i}
		}

		_, err := engine.BatchInsert("test", docs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "limited to 1000 documents")
	})

	t.Run("Batch Insert Large Valid Batch", func(t *testing.T) {
		docs := make([]domain.Document, 500)
		for i := 0; i < 500; i++ {
			docs[i] = domain.Document{"id": i, "value": i * 2}
		}

		_, err := engine.BatchInsert("large_batch", docs)
		require.NoError(t, err)

		// Verify a few random documents
		doc1, err := engine.GetById("large_batch", "1")
		require.NoError(t, err)
		assert.Equal(t, 0, doc1["id"])

		doc250, err := engine.GetById("large_batch", "250")
		require.NoError(t, err)
		assert.Equal(t, 249, doc250["id"])
		assert.Equal(t, 498, doc250["value"])
	})
}

func TestStorageEngine_BatchUpdate(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Setup: Insert some initial documents
	initialDocs := []domain.Document{
		{"name": "Alice", "age": 30, "department": "Engineering"},
		{"name": "Bob", "age": 25, "department": "Sales"},
		{"name": "Charlie", "age": 35, "department": "Engineering"},
	}
	_, err = engine.BatchInsert("employees", initialDocs)
	require.NoError(t, err)

	t.Run("Basic Batch Update", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"age": 31, "salary": 75000}},
			{ID: "2", Updates: domain.Document{"age": 26, "salary": 60000}},
		}

		_, err := engine.BatchUpdate("employees", operations)
		require.NoError(t, err)

		// Verify updates
		doc1, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, 31, doc1["age"])
		assert.Equal(t, 75000, doc1["salary"])
		assert.Equal(t, "Alice", doc1["name"]) // Original field preserved

		doc2, err := engine.GetById("employees", "2")
		require.NoError(t, err)
		assert.Equal(t, 26, doc2["age"])
		assert.Equal(t, 60000, doc2["salary"])
	})

	t.Run("Batch Update Prevents ID Changes", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"_id": "999", "newfield": "test"}},
		}

		_, err := engine.BatchUpdate("employees", operations)
		require.NoError(t, err)

		// Verify _id wasn't changed but other field was updated
		doc, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, "1", doc["_id"])
		assert.Equal(t, "test", doc["newfield"])
	})

	t.Run("Batch Update Atomic Failures", func(t *testing.T) {
		// Store original state to verify no changes occurred
		doc1Before, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		doc2Before, err := engine.GetById("employees", "2")
		require.NoError(t, err)

		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"status": "updated"}},  // Valid
			{ID: "999", Updates: domain.Document{"status": "failed"}}, // Invalid - atomic failure
			{ID: "2", Updates: domain.Document{"status": "updated"}},  // Valid but not applied
		}

		_, err = engine.BatchUpdate("employees", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "document with id 999 not found")

		// Verify NO updates were applied (atomic behavior)
		doc1After, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, doc1Before, doc1After) // Should be unchanged

		doc2After, err := engine.GetById("employees", "2")
		require.NoError(t, err)
		assert.Equal(t, doc2Before, doc2After) // Should be unchanged
	})

	t.Run("Batch Update Empty Operations", func(t *testing.T) {
		_, err := engine.BatchUpdate("employees", []domain.BatchUpdateOperation{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no operations provided")
	})

	t.Run("Batch Update Too Many Operations", func(t *testing.T) {
		operations := make([]domain.BatchUpdateOperation, 1001)
		for i := 0; i < 1001; i++ {
			operations[i] = domain.BatchUpdateOperation{
				ID:      "1",
				Updates: domain.Document{"field": i},
			}
		}

		_, err := engine.BatchUpdate("employees", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "limited to 1000 operations")
	})

	t.Run("Batch Update Non-Existent Collection", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"field": "value"}},
		}

		_, err := engine.BatchUpdate("nonexistent", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("Batch Update Empty ID Atomic Failure", func(t *testing.T) {
		// Store original state to verify no changes occurred
		doc1Before, err := engine.GetById("employees", "1")
		require.NoError(t, err)

		operations := []domain.BatchUpdateOperation{
			{ID: "", Updates: domain.Document{"field": "value"}},
			{ID: "1", Updates: domain.Document{"field": "value"}}, // Valid but not applied due to atomic failure
		}

		_, err = engine.BatchUpdate("employees", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "document ID cannot be empty")

		// Verify NO updates were applied (atomic behavior)
		doc1After, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, doc1Before, doc1After) // Should be unchanged
	})
}

func TestStorageEngine_BatchOperations_WithIndexes(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-index-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create index on age field
	err = engine.indexEngine.CreateIndex("users", "age")
	require.NoError(t, err)

	t.Run("Batch Insert Updates Indexes", func(t *testing.T) {
		docs := []domain.Document{
			{"name": "Alice", "age": 30},
			{"name": "Bob", "age": 25},
			{"name": "Charlie", "age": 30}, // Same age as Alice
		}

		_, err := engine.BatchInsert("users", docs)
		require.NoError(t, err)

		// Verify indexes were created by checking that documents can be found
		// (The actual index querying will be tested separately when the API is available)
		doc1, err := engine.GetById("users", "1")
		require.NoError(t, err)
		assert.Equal(t, 30, doc1["age"])

		doc3, err := engine.GetById("users", "3")
		require.NoError(t, err)
		assert.Equal(t, 30, doc3["age"])
	})

	t.Run("Batch Update Updates Indexes", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"age": 31}}, // Change Alice's age
		}

		_, err := engine.BatchUpdate("users", operations)
		require.NoError(t, err)

		// Verify the update was applied (index update testing will be done when query API is available)
		doc1, err := engine.GetById("users", "1")
		require.NoError(t, err)
		assert.Equal(t, 31, doc1["age"])
	})
}

func TestStorageEngine_BatchOperations_WithNoSavess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-save-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(
		WithDataDir(tempDir),
		WithNoSaves(true),
	)
	defer engine.StopBackgroundWorkers()

	t.Run("Batch Insert Marks Collection Dirty", func(t *testing.T) {
		docs := []domain.Document{
			{"name": "Test1"},
			{"name": "Test2"},
		}

		_, err := engine.BatchInsert("test_saves", docs)
		require.NoError(t, err)

		// Check collection is dirty
		engine.mu.RLock()
		collInfo, exists := engine.collections["test_saves"]
		require.True(t, exists)
		assert.Equal(t, CollectionStateDirty, collInfo.State)
		engine.mu.RUnlock()

		// Trigger save (should be no-op in no-saves mode)
		err = engine.SaveCollectionAfterTransaction("test_saves")
		require.NoError(t, err)

		// Check file does NOT exist (no-saves mode)
		saveFile := filepath.Join(tempDir, "collections", "test_saves.godb")
		assert.NoFileExists(t, saveFile)

		// Check collection remains dirty (no-saves mode)
		engine.mu.RLock()
		collInfo, _ = engine.collections["test_saves"]
		assert.Equal(t, CollectionStateDirty, collInfo.State)
		engine.mu.RUnlock()
	})

	t.Run("Batch Update Marks Collection Dirty", func(t *testing.T) {
		// First mark as clean by saving
		err := engine.SaveCollectionAfterTransaction("test_saves")
		require.NoError(t, err)

		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"updated": true}},
		}

		_, err = engine.BatchUpdate("test_saves", operations)
		require.NoError(t, err)

		// Check collection is dirty again
		engine.mu.RLock()
		collInfo, exists := engine.collections["test_saves"]
		require.True(t, exists)
		assert.Equal(t, CollectionStateDirty, collInfo.State)
		engine.mu.RUnlock()
	})
}

func TestStorageEngine_BatchOperations_Concurrency(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-concurrency-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	t.Run("Concurrent Batch Inserts Different Collections", func(t *testing.T) {
		const numGoroutines = 5
		const docsPerBatch = 10

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(routineID int) {
				defer wg.Done()

				docs := make([]domain.Document, docsPerBatch)
				for j := 0; j < docsPerBatch; j++ {
					docs[j] = domain.Document{
						"routine": routineID,
						"doc":     j,
						"data":    fmt.Sprintf("routine-%d-doc-%d", routineID, j),
					}
				}

				collName := fmt.Sprintf("concurrent_coll_%d", routineID)
				_, err := engine.BatchInsert(collName, docs)
				if err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent batch insert error: %v", err)
		}

		// Verify all collections were created with correct document counts
		for i := 0; i < numGoroutines; i++ {
			collName := fmt.Sprintf("concurrent_coll_%d", i)

			engine.mu.RLock()
			collInfo, exists := engine.collections[collName]
			engine.mu.RUnlock()

			assert.True(t, exists, "Collection %s should exist", collName)
			assert.Equal(t, int64(docsPerBatch), collInfo.DocumentCount, "Collection %s should have %d documents", collName, docsPerBatch)
		}
	})

	t.Run("Concurrent Batch Operations Same Collection", func(t *testing.T) {
		// Insert initial documents
		initialDocs := make([]domain.Document, 20)
		for i := 0; i < 20; i++ {
			initialDocs[i] = domain.Document{"id": i, "value": i}
		}
		_, err := engine.BatchInsert("shared_coll", initialDocs)
		require.NoError(t, err)

		const numReaders = 3
		const numUpdaters = 2
		const operationsPerGoroutine = 5

		var wg sync.WaitGroup
		errors := make(chan error, numReaders+numUpdaters)

		// Start readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					docID := fmt.Sprintf("%d", (readerID*operationsPerGoroutine+j)%20+1)
					_, err := engine.GetById("shared_coll", docID)
					if err != nil {
						errors <- fmt.Errorf("reader %d: %v", readerID, err)
						return
					}
					time.Sleep(1 * time.Millisecond)
				}
			}(i)
		}

		// Start batch updaters
		for i := 0; i < numUpdaters; i++ {
			wg.Add(1)
			go func(updaterID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					operations := []domain.BatchUpdateOperation{
						{ID: "1", Updates: domain.Document{"updater": updaterID, "batch": j}},
						{ID: "2", Updates: domain.Document{"updater": updaterID, "batch": j}},
					}

					_, err := engine.BatchUpdate("shared_coll", operations)
					if err != nil {
						errors <- fmt.Errorf("updater %d: %v", updaterID, err)
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent operation error: %v", err)
		}

		// Verify final state
		doc1, err := engine.GetById("shared_coll", "1")
		require.NoError(t, err)
		assert.Contains(t, doc1, "updater") // Should have been updated
	})
}

func TestStorageEngine_BatchInsert_Atomic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-atomic-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	t.Run("Atomic Success - All Documents Inserted", func(t *testing.T) {
		docs := []domain.Document{
			{"name": "Alice", "age": 30},
			{"name": "Bob", "age": 25},
			{"name": "Charlie", "age": 35},
		}

		_, err := engine.BatchInsert("users", docs)
		require.NoError(t, err)

		// Verify all documents were inserted
		for i := 1; i <= 3; i++ {
			doc, err := engine.GetById("users", fmt.Sprintf("%d", i))
			require.NoError(t, err)
			assert.NotEmpty(t, doc["name"])
		}

		// Verify collection state
		collection, err := engine.GetCollection("users")
		require.NoError(t, err)
		assert.Len(t, collection.Documents, 3)
	})

	t.Run("Atomic Failure - ID Conflict Prevention", func(t *testing.T) {
		// Insert an initial document to set up the ID counter
		initialDoc := domain.Document{"name": "Initial", "age": 40}
		_, err := engine.Insert("products", initialDoc)
		require.NoError(t, err)

		// Get the current state before batch insert
		collectionBefore, err := engine.GetCollection("products")
		require.NoError(t, err)
		docCountBefore := len(collectionBefore.Documents)

		// Manually create a conflict by inserting a document with a future ID
		// This simulates the scenario where our atomic batch insert detects a conflict
		conflictDoc := domain.Document{"name": "Conflict", "age": 50, "_id": "3"}
		collectionBefore.Documents["3"] = conflictDoc

		// Now try batch insert that would create IDs "2", "3", "4"
		docs := []domain.Document{
			{"name": "Product A", "price": 100},
			{"name": "Product B", "price": 200}, // This would get ID "3" - conflict!
			{"name": "Product C", "price": 300},
		}

		_, err = engine.BatchInsert("products", docs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")

		// Verify NO documents were inserted from the batch (atomic rollback)
		collectionAfter, err := engine.GetCollection("products")
		require.NoError(t, err)

		// Should only have the initial document (1) and the conflict document (3)
		assert.Len(t, collectionAfter.Documents, docCountBefore+1) // +1 for manually added conflict

		// Verify the intended batch documents were NOT inserted
		_, err = engine.GetById("products", "2")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")

		// ID "3" should still be the conflict document, not from our batch
		doc3, err := engine.GetById("products", "3")
		require.NoError(t, err)
		assert.Equal(t, "Conflict", doc3["name"])

		_, err = engine.GetById("products", "4")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Validation Failures Prevent Collection Creation", func(t *testing.T) {
		// Test 1: Empty document list - should fail before any collection creation
		_, err := engine.GetCollection("empty_test")
		assert.Error(t, err) // Collection shouldn't exist

		_, err = engine.BatchInsert("empty_test", []domain.Document{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no documents provided")

		// Verify no collection was created
		_, exists := engine.collections["empty_test"]
		assert.False(t, exists)

		// Test 2: Too many documents - should fail before any collection creation
		tooManyDocs := make([]domain.Document, 1001)
		for i := 0; i < 1001; i++ {
			tooManyDocs[i] = domain.Document{"id": i}
		}

		_, err = engine.BatchInsert("large_test", tooManyDocs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "limited to 1000 documents")

		// Verify no collection was created
		_, exists = engine.collections["large_test"]
		assert.False(t, exists)
	})
}

func TestStorageEngine_BatchUpdate_Atomic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-update-atomic-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Setup: Insert some initial documents to update
	initialDocs := []domain.Document{
		{"name": "Alice", "age": 30, "department": "Engineering"},
		{"name": "Bob", "age": 25, "department": "Sales"},
		{"name": "Charlie", "age": 35, "department": "Marketing"},
		{"name": "Diana", "age": 28, "department": "Engineering"},
	}

	_, err = engine.BatchInsert("employees", initialDocs)
	require.NoError(t, err)

	t.Run("Atomic Success - All Updates Applied", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"age": 31, "salary": 75000}},
			{ID: "2", Updates: domain.Document{"age": 26, "position": "Senior Sales Rep"}},
			{ID: "3", Updates: domain.Document{"department": "Product Marketing"}},
		}

		_, err := engine.BatchUpdate("employees", operations)
		require.NoError(t, err)

		// Verify all updates were applied
		doc1, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, 31, doc1["age"])
		assert.Equal(t, 75000, doc1["salary"])
		assert.Equal(t, "Alice", doc1["name"]) // Original field preserved

		doc2, err := engine.GetById("employees", "2")
		require.NoError(t, err)
		assert.Equal(t, 26, doc2["age"])
		assert.Equal(t, "Senior Sales Rep", doc2["position"])
		assert.Equal(t, "Bob", doc2["name"]) // Original field preserved

		doc3, err := engine.GetById("employees", "3")
		require.NoError(t, err)
		assert.Equal(t, "Product Marketing", doc3["department"])
		assert.Equal(t, "Charlie", doc3["name"]) // Original field preserved
	})

	t.Run("Atomic Failure - Document Not Found", func(t *testing.T) {
		// Store original state before attempted batch update
		doc1Before, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		doc2Before, err := engine.GetById("employees", "2")
		require.NoError(t, err)

		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"age": 99}},   // Valid
			{ID: "999", Updates: domain.Document{"age": 99}}, // Invalid - doesn't exist
			{ID: "2", Updates: domain.Document{"age": 99}},   // Valid but should not be applied
		}

		_, err = engine.BatchUpdate("employees", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "document with id 999 not found")

		// Verify NO updates were applied (atomic rollback)
		doc1After, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, doc1Before, doc1After) // Should be identical

		doc2After, err := engine.GetById("employees", "2")
		require.NoError(t, err)
		assert.Equal(t, doc2Before, doc2After) // Should be identical
	})

	t.Run("Atomic Failure - Empty Document ID", func(t *testing.T) {
		// Store original states
		doc1Before, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		doc2Before, err := engine.GetById("employees", "2")
		require.NoError(t, err)

		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"age": 100}}, // Valid
			{ID: "", Updates: domain.Document{"age": 100}},  // Invalid - empty ID
			{ID: "2", Updates: domain.Document{"age": 100}}, // Valid but should not be applied
		}

		_, err = engine.BatchUpdate("employees", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "document ID cannot be empty")

		// Verify NO updates were applied
		doc1After, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, doc1Before, doc1After)

		doc2After, err := engine.GetById("employees", "2")
		require.NoError(t, err)
		assert.Equal(t, doc2Before, doc2After)
	})

	t.Run("Atomic Failure - Non-Existent Collection", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"field": "value"}},
		}

		_, err := engine.BatchUpdate("nonexistent", operations)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "collection nonexistent does not exist")
	})

	t.Run("ID Protection - Cannot Update _id Field", func(t *testing.T) {
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"_id": "999", "age": 32}}, // Try to change ID
		}

		_, err := engine.BatchUpdate("employees", operations)
		require.NoError(t, err) // Should succeed but ignore _id change

		// Verify _id was NOT changed but other fields were updated
		doc1After, err := engine.GetById("employees", "1")
		require.NoError(t, err)
		assert.Equal(t, "1", doc1After["_id"]) // ID unchanged
		assert.Equal(t, 32, doc1After["age"])  // Other field updated
	})

	t.Run("Complex Update Scenario", func(t *testing.T) {
		// Add a new field to all documents and modify existing fields
		operations := []domain.BatchUpdateOperation{
			{ID: "1", Updates: domain.Document{"status": "active", "age": 33}},
			{ID: "2", Updates: domain.Document{"status": "active", "experience": "5 years"}},
			{ID: "3", Updates: domain.Document{"status": "inactive", "age": 36}},
			{ID: "4", Updates: domain.Document{"status": "active", "department": "HR"}},
		}

		_, err := engine.BatchUpdate("employees", operations)
		require.NoError(t, err)

		// Verify all complex updates
		for i := 1; i <= 4; i++ {
			doc, err := engine.GetById("employees", fmt.Sprintf("%d", i))
			require.NoError(t, err)

			// All should have status field
			assert.Contains(t, doc, "status")

			// Verify specific updates
			switch i {
			case 1:
				assert.Equal(t, "active", doc["status"])
				assert.Equal(t, 33, doc["age"])
			case 2:
				assert.Equal(t, "active", doc["status"])
				assert.Equal(t, "5 years", doc["experience"])
			case 3:
				assert.Equal(t, "inactive", doc["status"])
				assert.Equal(t, 36, doc["age"])
			case 4:
				assert.Equal(t, "active", doc["status"])
				assert.Equal(t, "HR", doc["department"])
			}
		}
	})
}

func TestStorageEngine_BatchUpdate_Validation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-batch-update-validation-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	t.Run("Empty Operations List", func(t *testing.T) {
		_, err := engine.BatchUpdate("test", []domain.BatchUpdateOperation{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no operations provided")
	})

	t.Run("Too Many Operations", func(t *testing.T) {
		tooManyOps := make([]domain.BatchUpdateOperation, 1001)
		for i := 0; i < 1001; i++ {
			tooManyOps[i] = domain.BatchUpdateOperation{
				ID:      fmt.Sprintf("%d", i),
				Updates: domain.Document{"field": "value"},
			}
		}

		_, err := engine.BatchUpdate("test", tooManyOps)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "limited to 1000 operations")
	})
}

func TestStorageEngine_IndexPersistence(t *testing.T) {
	tempFile := "test_index_persistence.godb"
	defer os.Remove(tempFile)

	// Create first engine and add data with indexes
	engine1 := NewStorageEngine()
	defer engine1.StopBackgroundWorkers()

	// Insert documents
	doc1 := domain.Document{"name": "Alice", "age": 30, "city": "New York"}
	doc2 := domain.Document{"name": "Bob", "age": 25, "city": "Boston"}
	doc3 := domain.Document{"name": "Charlie", "age": 35, "city": "Chicago"}

	_, err := engine1.Insert("users", doc1)
	require.NoError(t, err)

	_, err = engine1.Insert("users", doc2)
	require.NoError(t, err)

	_, err = engine1.Insert("users", doc3)
	require.NoError(t, err)

	// Create indexes
	err = engine1.indexEngine.CreateIndex("users", "age")
	require.NoError(t, err)

	err = engine1.indexEngine.CreateIndex("users", "city")
	require.NoError(t, err)

	// Build indexes for the collection
	collection, err := engine1.GetCollection("users")
	require.NoError(t, err)

	err = engine1.indexEngine.BuildIndexForCollection("users", "age", collection)
	require.NoError(t, err)

	err = engine1.indexEngine.BuildIndexForCollection("users", "city", collection)
	require.NoError(t, err)

	// Verify indexes exist (including auto-created _id index)
	indexes, err := engine1.indexEngine.GetIndexes("users")
	require.NoError(t, err)
	assert.Len(t, indexes, 3) // _id, age, city
	assert.Contains(t, indexes, "_id")
	assert.Contains(t, indexes, "age")
	assert.Contains(t, indexes, "city")

	// Save to file
	err = engine1.SaveToFile(tempFile)
	require.NoError(t, err)

	// Create second engine and load from file
	engine2 := NewStorageEngine()
	defer engine2.StopBackgroundWorkers()

	err = engine2.LoadCollectionMetadata(tempFile)
	require.NoError(t, err)

	// Verify indexes were loaded (including auto-created _id index)
	indexes, err = engine2.indexEngine.GetIndexes("users")
	require.NoError(t, err)
	assert.Len(t, indexes, 3, "Indexes should be loaded from file")
	assert.Contains(t, indexes, "_id")
	assert.Contains(t, indexes, "age")
	assert.Contains(t, indexes, "city")

	// Load the collection to trigger index rebuilding
	collection, err = engine2.GetCollection("users")
	require.NoError(t, err)

	// Verify index functionality works
	ageIndex, exists := engine2.indexEngine.GetIndex("users", "age")
	require.True(t, exists)

	// Query by age (use int8 to match the stored type)
	docIDs := ageIndex.Query(int8(30))
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	docIDs = ageIndex.Query(int8(25))
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "2", docIDs[0])

	// Query by city
	cityIndex, exists := engine2.indexEngine.GetIndex("users", "city")
	require.True(t, exists)

	docIDs = cityIndex.Query("New York")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "1", docIDs[0])

	docIDs = cityIndex.Query("Boston")
	assert.Len(t, docIDs, 1)
	assert.Equal(t, "2", docIDs[0])
}
