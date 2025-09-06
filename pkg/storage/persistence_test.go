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

func TestStorageEngine_SaveToFile(t *testing.T) {
	tempFile := "test_save.godb"
	defer os.Remove(tempFile)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test data
	docs := []domain.Document{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "San Francisco"},
		{"name": "Charlie", "age": 35, "city": "Chicago"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Save to file
	err := engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Verify file exists and has content
	fileInfo, err := os.Stat(tempFile)
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(0))

	// Verify file has correct extension
	assert.Contains(t, tempFile, FileExtension)
}

func TestStorageEngine_LoadCollectionMetadata(t *testing.T) {
	tempFile := "test_load.godb"
	defer os.Remove(tempFile)

	// Create engine and save data
	engine1 := NewStorageEngine()
	defer engine1.StopBackgroundWorkers()

	// Insert test data
	docs := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	for _, doc := range docs {
		err := engine1.Insert("users", doc)
		require.NoError(t, err)
	}

	// Save to file
	err := engine1.SaveToFile(tempFile)
	require.NoError(t, err)

	// Create new engine and load metadata
	engine2 := NewStorageEngine()
	defer engine2.StopBackgroundWorkers()

	err = engine2.LoadCollectionMetadata(tempFile)
	require.NoError(t, err)

	// Verify collection metadata was loaded
	engine2.mu.RLock()
	info, exists := engine2.collections["users"]
	engine2.mu.RUnlock()

	assert.True(t, exists)
	assert.Equal(t, "users", info.Name)
	assert.Equal(t, int64(2), info.DocumentCount)
	assert.Equal(t, CollectionStateUnloaded, info.State)
}

func TestStorageEngine_LoadCollectionMetadata_FileNotExists(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Try to load from non-existent file
	err := engine.LoadCollectionMetadata("nonexistent.godb")
	assert.NoError(t, err) // Should not error, just return empty state
}

func TestStorageEngine_LoadCollectionMetadata_InvalidFile(t *testing.T) {
	tempFile := "test_invalid.godb"
	defer os.Remove(tempFile)

	// Create invalid file
	err := os.WriteFile(tempFile, []byte("invalid data"), 0644)
	require.NoError(t, err)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Try to load invalid file
	err = engine.LoadCollectionMetadata(tempFile)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid file header")
}

func TestStorageEngine_LoadCollectionMetadata_EmptyFile(t *testing.T) {
	tempFile := "test_empty.godb"
	defer os.Remove(tempFile)

	// Create empty file
	file, err := os.Create(tempFile)
	require.NoError(t, err)
	file.Close()

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Try to load empty file
	err = engine.LoadCollectionMetadata(tempFile)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read header")
}

func TestStorageEngine_SaveToFile_EmptyCollections(t *testing.T) {
	tempFile := "test_empty_collections.godb"
	defer os.Remove(tempFile)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Save empty engine
	err := engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Verify file was created
	fileInfo, err := os.Stat(tempFile)
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(0))
}

func TestStorageEngine_SaveToFile_MultipleCollections(t *testing.T) {
	tempFile := "test_multiple_collections.godb"
	defer os.Remove(tempFile)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert data into multiple collections
	users := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	products := []domain.Document{
		{"name": "Laptop", "price": 999.99},
		{"name": "Mouse", "price": 29.99},
	}

	for _, doc := range users {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	for _, doc := range products {
		err := engine.Insert("products", doc)
		require.NoError(t, err)
	}

	// Save to file
	err := engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Load metadata in new engine
	newEngine := NewStorageEngine()
	defer newEngine.StopBackgroundWorkers()

	err = newEngine.LoadCollectionMetadata(tempFile)
	require.NoError(t, err)

	// Verify both collections were loaded
	newEngine.mu.RLock()
	usersInfo, usersExists := newEngine.collections["users"]
	productsInfo, productsExists := newEngine.collections["products"]
	newEngine.mu.RUnlock()

	assert.True(t, usersExists)
	assert.Equal(t, "users", usersInfo.Name)
	assert.Equal(t, int64(2), usersInfo.DocumentCount)

	assert.True(t, productsExists)
	assert.Equal(t, "products", productsInfo.Name)
	assert.Equal(t, int64(2), productsInfo.DocumentCount)
}

func TestStorageEngine_SaveToFile_PermissionError(t *testing.T) {
	// Try to save to a directory that doesn't exist
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert some data
	doc := domain.Document{"test": "data"}
	err := engine.Insert("test", doc)
	require.NoError(t, err)

	// Try to save to non-existent directory
	err = engine.SaveToFile("/nonexistent/directory/test.godb")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create file")
}

func TestStorageEngine_LoadCollectionFromDisk(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	tempFile := filepath.Join(tempDir, "test_load_collection.godb")

	// Create engine and save data
	engine1 := NewStorageEngine(WithDataDir(tempDir))
	defer engine1.StopBackgroundWorkers()

	// Insert test data
	docs := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	for _, doc := range docs {
		err = engine1.Insert("users", doc)
		require.NoError(t, err)
	}

	// Save to file
	err = engine1.SaveToFile(tempFile)
	require.NoError(t, err)

	// Test loading collection from disk - should fail because SaveToFile creates a monolithic file
	// but loadCollectionFromDisk expects per-collection files in tempDir/collections/users.godb
	_, err = engine1.loadCollectionFromDisk("users")
	assert.Error(t, err, "loadCollectionFromDisk should fail when trying to load from collections/users.godb after SaveToFile creates a monolithic file")
}

func TestStorageEngine_SaveToFile_ConcurrentAccess(t *testing.T) {
	tempFile := "test_concurrent_save.godb"
	defer os.Remove(tempFile)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert data concurrently
	const numGoroutines = 5
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
					"data":      "concurrent test",
				}
				err := engine.Insert("concurrent", doc)
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Save to file (should be thread-safe)
	err := engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Verify file was created
	fileInfo, err := os.Stat(tempFile)
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(0))
}

func TestStorageEngine_FileFormatCompatibility(t *testing.T) {
	tempFile := "test_compatibility.godb"
	defer os.Remove(tempFile)

	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test data
	doc := domain.Document{"name": "Test", "value": 42}
	err := engine.Insert("test", doc)
	require.NoError(t, err)

	// Save to file
	err = engine.SaveToFile(tempFile)
	require.NoError(t, err)

	// Read file and verify format
	file, err := os.Open(tempFile)
	require.NoError(t, err)
	defer file.Close()

	// Read and validate header
	header, err := ReadHeader(file)
	require.NoError(t, err)
	assert.Equal(t, MagicBytes, string(header.Magic[:]))
	assert.EqualValues(t, FormatVersion, header.Version)

	// Verify file has compressed data after header
	fileInfo, err := file.Stat()
	require.NoError(t, err)
	assert.Greater(t, fileInfo.Size(), int64(8)) // Header size
}

// Tests for new persistence functionality

func TestStorageEngine_SaveDirtyCollections_FileOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-persistence-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create multiple collections with different data
	collections := []string{"users", "products", "orders"}

	for _, collName := range collections {
		err = engine.CreateCollection(collName)
		require.NoError(t, err)

		// Insert different amounts of data per collection
		docCount := (len(collName) % 3) + 1 // 1-3 documents per collection
		for i := 0; i < docCount; i++ {
			doc := domain.Document{
				"collection": collName,
				"id":         i,
				"data":       fmt.Sprintf("test-data-%s-%d", collName, i),
			}
			err := engine.Insert(collName, doc)
			require.NoError(t, err)
		}
	}

	// All collections should be dirty
	for _, collName := range collections {
		engine.mu.RLock()
		collInfo := engine.collections[collName]
		engine.mu.RUnlock()
		assert.Equal(t, CollectionStateDirty, collInfo.State)
	}

	// Save all dirty collections
	engine.saveDirtyCollections()

	// Verify all files were created with correct format
	collectionsDir := filepath.Join(tempDir, "collections")
	assert.DirExists(t, collectionsDir)

	for _, collName := range collections {
		fileName := filepath.Join(collectionsDir, collName+".godb")
		assert.FileExists(t, fileName)

		// Verify file has proper header
		file, err := os.Open(fileName)
		require.NoError(t, err)

		header, err := ReadHeader(file)
		require.NoError(t, err)
		assert.Equal(t, uint8(FormatVersion), header.Version)

		file.Close()

		// Verify collection state was updated
		engine.mu.RLock()
		collInfo := engine.collections[collName]
		engine.mu.RUnlock()
		assert.Equal(t, CollectionStateLoaded, collInfo.State)
		assert.Greater(t, collInfo.SizeOnDisk, int64(0))
	}
}

func TestStorageEngine_SaveCollectionToFile_DataIntegrity(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-persistence-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection with complex documents
	err = engine.CreateCollection("complex")
	require.NoError(t, err)

	// Insert documents with various data types
	testDocs := []domain.Document{
		{
			"string_field":  "test string",
			"int_field":     42,
			"float_field":   3.14159,
			"bool_field":    true,
			"array_field":   []interface{}{1, 2, "three", true},
			"nested_object": map[string]interface{}{"key": "value", "num": 123},
		},
		{
			"unicode":    "こんにちは世界",
			"empty_str":  "",
			"zero_int":   0,
			"false_bool": false,
			"null_field": nil,
		},
		{
			"large_string": strings.Repeat("x", 1000),
			"timestamp":    time.Now().Unix(),
		},
	}

	for _, doc := range testDocs {
		err := engine.Insert("complex", doc)
		require.NoError(t, err)
	}

	// Save the collection
	err = engine.saveCollectionToFile("complex")
	require.NoError(t, err)

	// Verify file was created
	fileName := filepath.Join(tempDir, "collections", "complex.godb")
	assert.FileExists(t, fileName)

	// Create a new engine and try to load the collection
	newEngine := NewStorageEngine(WithDataDir(tempDir))
	defer newEngine.StopBackgroundWorkers()

	// Manually load the collection to verify data integrity
	loadedCollection, err := newEngine.loadCollectionFromDisk("complex")
	require.NoError(t, err)

	// Verify all documents were saved and loaded correctly
	assert.Len(t, loadedCollection.Documents, 3)

	// Check that documents contain expected data
	for docID, doc := range loadedCollection.Documents {
		assert.NotEmpty(t, docID)
		assert.NotEmpty(t, doc)

		// Every document should have an _id field
		assert.Contains(t, doc, "_id")
	}
}

func TestStorageEngine_SaveDirtyCollections_EmptyCollections(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-persistence-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create an empty collection
	err = engine.CreateCollection("empty")
	require.NoError(t, err)

	// Mark it as dirty manually
	engine.mu.Lock()
	engine.collections["empty"].State = CollectionStateDirty
	engine.mu.Unlock()

	// Save dirty collections
	engine.saveDirtyCollections()

	// Verify file was created even for empty collection
	fileName := filepath.Join(tempDir, "collections", "empty.godb")
	assert.FileExists(t, fileName)

	// Verify collection state was updated
	engine.mu.RLock()
	emptyInfo := engine.collections["empty"]
	engine.mu.RUnlock()
	assert.Equal(t, CollectionStateLoaded, emptyInfo.State)
}

func TestStorageEngine_SaveCollectionToFile_ErrorHandling(t *testing.T) {
	engine := NewStorageEngine(WithDataDir("/invalid/path/that/does/not/exist"))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err := engine.CreateCollection("test")
	require.NoError(t, err)

	// Insert a document
	doc := domain.Document{"name": "Test"}
	err = engine.Insert("test", doc)
	require.NoError(t, err)

	// Try to save to invalid path - should handle error gracefully
	err = engine.saveCollectionToFile("test")
	assert.Error(t, err)
	// Could fail on directory creation or file creation
	assert.True(t, strings.Contains(err.Error(), "failed to create collections directory") ||
		strings.Contains(err.Error(), "failed to create file"))
}

func TestStorageEngine_SaveDirtyCollections_ConcurrentAccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-persistence-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("concurrent")
	require.NoError(t, err)

	var wg sync.WaitGroup
	results := make(chan string, 100)

	// Start multiple goroutines inserting data
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				doc := domain.Document{
					"goroutine": id,
					"iteration": j,
					"data":      fmt.Sprintf("data-%d-%d", id, j),
				}
				err := engine.Insert("concurrent", doc)
				if err != nil {
					results <- fmt.Sprintf("insert-error-%d-%d: %v", id, j, err)
				} else {
					results <- fmt.Sprintf("insert-success-%d-%d", id, j)
				}
			}
		}(i)
	}

	// Start a goroutine that saves collections
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let some inserts happen first
		engine.saveDirtyCollections()
		results <- "save-completed"
	}()

	wg.Wait()
	close(results)

	// Count results
	insertSuccesses := 0
	saveCompleted := false
	errors := 0

	for result := range results {
		if strings.Contains(result, "insert-success") {
			insertSuccesses++
		} else if result == "save-completed" {
			saveCompleted = true
		} else {
			errors++
			t.Logf("Error: %s", result)
		}
	}

	assert.Equal(t, 50, insertSuccesses, "All inserts should succeed")
	assert.True(t, saveCompleted, "Save should complete")
	assert.Equal(t, 0, errors, "No errors should occur")

	// Verify file was created
	fileName := filepath.Join(tempDir, "collections", "concurrent.godb")
	assert.FileExists(t, fileName)
}

func TestStorageEngine_LoadCollectionFromDisk_Integration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-persistence-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create first engine and save data
	engine1 := NewStorageEngine(WithDataDir(tempDir))
	defer engine1.StopBackgroundWorkers()

	err = engine1.CreateCollection("shared")
	require.NoError(t, err)

	originalDocs := []domain.Document{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "San Francisco"},
		{"name": "Charlie", "age": 35, "city": "Chicago"},
	}

	for _, doc := range originalDocs {
		err := engine1.Insert("shared", doc)
		require.NoError(t, err)
	}

	// Save the collection
	err = engine1.saveCollectionToFile("shared")
	require.NoError(t, err)

	// Create second engine and load the data
	engine2 := NewStorageEngine(WithDataDir(tempDir))
	defer engine2.StopBackgroundWorkers()

	loadedCollection, err := engine2.loadCollectionFromDisk("shared")
	require.NoError(t, err)

	// Verify loaded data matches original
	assert.Len(t, loadedCollection.Documents, 3)

	// Check that all original data is present
	foundNames := make(map[string]bool)
	for _, doc := range loadedCollection.Documents {
		if name, ok := doc["name"].(string); ok {
			foundNames[name] = true
		}
	}

	assert.True(t, foundNames["Alice"])
	assert.True(t, foundNames["Bob"])
	assert.True(t, foundNames["Charlie"])
}
