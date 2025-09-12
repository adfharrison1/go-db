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
		_, err := engine.Insert("users", doc)
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
		_, err := engine1.Insert("users", doc)
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
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	for _, doc := range products {
		_, err := engine.Insert("products", doc)
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
	_, err := engine.Insert("test", doc)
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

	// Create engine and save data (disable transaction saves to test monolithic vs per-collection loading)
	engine1 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine1.StopBackgroundWorkers()

	// Insert test data
	docs := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	for _, doc := range docs {
		_, err = engine1.Insert("users", doc)
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
				_, err := engine.Insert("concurrent", doc)
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
	_, err := engine.Insert("test", doc)
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

	engine := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
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
			_, err := engine.Insert(collName, doc)
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
		_, err := engine.Insert("complex", doc)
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
	// Disable transaction saves to test manual save error handling
	engine := NewStorageEngine(WithDataDir("/invalid/path/that/does/not/exist"), WithNoSaves(true))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err := engine.CreateCollection("test")
	require.NoError(t, err)

	// Insert a document
	doc := domain.Document{"name": "Test"}
	_, err = engine.Insert("test", doc)
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
				_, err := engine.Insert("concurrent", doc)
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
		_, err := engine1.Insert("shared", doc)
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

// Test ID counter restoration when loading collections from disk
func TestStorageEngine_IDCounterRestoration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-id-restoration-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Phase 1: Create collection with documents and save to disk
	engine1 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine1.StopBackgroundWorkers()

	// Insert documents with sequential IDs
	for i := 1; i <= 5; i++ {
		doc := domain.Document{"name": fmt.Sprintf("User %d", i), "value": i * 10}
		_, err := engine1.Insert("users", doc)
		require.NoError(t, err)
	}

	// Verify IDs are sequential: "1", "2", "3", "4", "5"
	for i := 1; i <= 5; i++ {
		doc, err := engine1.GetById("users", fmt.Sprintf("%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("User %d", i), doc["name"])
	}

	// Save to disk
	engine1.saveDirtyCollections()

	// Phase 2: Create new engine instance and load collection from disk
	engine2 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine2.StopBackgroundWorkers()

	// Since we're using per-collection saves, manually add collection info
	// to simulate how the engine would know about collections in a real scenario
	engine2.mu.Lock()
	engine2.collections["users"] = &CollectionInfo{
		Name:          "users",
		DocumentCount: 5,
		State:         CollectionStateUnloaded,
		LastModified:  time.Now(),
	}
	engine2.mu.Unlock()

	// Access the collection to trigger loading from disk
	collection, err := engine2.GetCollection("users")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 5)

	// Phase 3: Insert new document - should get ID "6", not "1"
	newDoc := domain.Document{"name": "User 6", "value": 60}
	_, err = engine2.Insert("users", newDoc)
	require.NoError(t, err)

	// Verify the new document got ID "6"
	doc6, err := engine2.GetById("users", "6")
	require.NoError(t, err)
	assert.Equal(t, "User 6", doc6["name"])
	assert.Equal(t, 60, doc6["value"])

	// Verify original documents are still intact
	for i := 1; i <= 5; i++ {
		doc, err := engine2.GetById("users", fmt.Sprintf("%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("User %d", i), doc["name"])
	}

	// Insert another document - should get ID "7"
	anotherDoc := domain.Document{"name": "User 7", "value": 70}
	_, err = engine2.Insert("users", anotherDoc)
	require.NoError(t, err)

	doc7, err := engine2.GetById("users", "7")
	require.NoError(t, err)
	assert.Equal(t, "User 7", doc7["name"])
}

// Test ID counter restoration with non-sequential IDs
func TestStorageEngine_IDCounterRestoration_NonSequential(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-id-nonseq-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Phase 1: Simulate a collection that had some documents deleted
	engine1 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine1.StopBackgroundWorkers()

	// Create collection manually and add documents with gaps
	err = engine1.CreateCollection("products")
	require.NoError(t, err)

	collection, err := engine1.GetCollection("products")
	require.NoError(t, err)

	// Manually add documents with non-sequential IDs (simulating deleted documents)
	collection.Documents["1"] = domain.Document{"name": "Product 1", "_id": "1"}
	collection.Documents["3"] = domain.Document{"name": "Product 3", "_id": "3"}
	collection.Documents["7"] = domain.Document{"name": "Product 7", "_id": "7"}
	collection.Documents["15"] = domain.Document{"name": "Product 15", "_id": "15"}

	// Mark collection as dirty and save
	if _, collectionInfo, found := engine1.cache.Get("products"); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount = 4
	}

	engine1.saveDirtyCollections()

	// Phase 2: Load in new engine - should restore counter to highest ID (15)
	engine2 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine2.StopBackgroundWorkers()

	// Manually add collection info for per-collection loading
	engine2.mu.Lock()
	engine2.collections["products"] = &CollectionInfo{
		Name:          "products",
		DocumentCount: 4,
		State:         CollectionStateUnloaded,
		LastModified:  time.Now(),
	}
	engine2.mu.Unlock()

	// Trigger loading from disk
	_, err = engine2.GetCollection("products")
	require.NoError(t, err)

	// Insert new document - should get ID "16", not "1"
	newDoc := domain.Document{"name": "Product 16"}
	_, err = engine2.Insert("products", newDoc)
	require.NoError(t, err)

	doc16, err := engine2.GetById("products", "16")
	require.NoError(t, err)
	assert.Equal(t, "Product 16", doc16["name"])
	assert.Equal(t, "16", doc16["_id"])
}

// Test ID counter restoration with empty collection
func TestStorageEngine_IDCounterRestoration_EmptyCollection(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-id-empty-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Phase 1: Create empty collection and save
	engine1 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine1.StopBackgroundWorkers()

	err = engine1.CreateCollection("empty")
	require.NoError(t, err)

	// Add a document, then delete it to create an empty collection that gets saved
	doc := domain.Document{"temp": "temp"}
	_, err = engine1.Insert("empty", doc)
	require.NoError(t, err)
	err = engine1.DeleteById("empty", "1")
	require.NoError(t, err)

	engine1.saveDirtyCollections()

	// Phase 2: Load in new engine
	engine2 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine2.StopBackgroundWorkers()

	// Manually add collection info for per-collection loading
	engine2.mu.Lock()
	engine2.collections["empty"] = &CollectionInfo{
		Name:          "empty",
		DocumentCount: 0,
		State:         CollectionStateUnloaded,
		LastModified:  time.Now(),
	}
	engine2.mu.Unlock()

	// Trigger loading from disk
	collection, err := engine2.GetCollection("empty")
	require.NoError(t, err)
	assert.Empty(t, collection.Documents)

	// Insert first document - should get ID "1" since collection is truly empty
	firstDoc := domain.Document{"name": "First Document"}
	_, err = engine2.Insert("empty", firstDoc)
	require.NoError(t, err)

	doc1, err := engine2.GetById("empty", "1")
	require.NoError(t, err)
	assert.Equal(t, "First Document", doc1["name"])
	assert.Equal(t, "1", doc1["_id"])
}

// Test ID counter restoration with non-numeric IDs (edge case)
func TestStorageEngine_IDCounterRestoration_NonNumericIDs(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-id-nonnumeric-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Phase 1: Create collection with mixed numeric and non-numeric IDs
	engine1 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine1.StopBackgroundWorkers()

	err = engine1.CreateCollection("mixed")
	require.NoError(t, err)

	collection, err := engine1.GetCollection("mixed")
	require.NoError(t, err)

	// Add documents with mixed ID types
	collection.Documents["5"] = domain.Document{"name": "Numeric 5", "_id": "5"}
	collection.Documents["abc"] = domain.Document{"name": "String abc", "_id": "abc"}
	collection.Documents["10"] = domain.Document{"name": "Numeric 10", "_id": "10"}
	collection.Documents["xyz"] = domain.Document{"name": "String xyz", "_id": "xyz"}

	// Mark as dirty and save
	if _, collectionInfo, found := engine1.cache.Get("mixed"); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount = 4
	}

	engine1.saveDirtyCollections()

	// Phase 2: Load in new engine - should restore counter to highest numeric ID (10)
	engine2 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine2.StopBackgroundWorkers()

	// Manually add collection info for per-collection loading
	engine2.mu.Lock()
	engine2.collections["mixed"] = &CollectionInfo{
		Name:          "mixed",
		DocumentCount: 4,
		State:         CollectionStateUnloaded,
		LastModified:  time.Now(),
	}
	engine2.mu.Unlock()

	// Trigger loading from disk
	_, err = engine2.GetCollection("mixed")
	require.NoError(t, err)

	// Insert new document - should get ID "11" (ignoring non-numeric IDs)
	newDoc := domain.Document{"name": "Numeric 11"}
	_, err = engine2.Insert("mixed", newDoc)
	require.NoError(t, err)

	doc11, err := engine2.GetById("mixed", "11")
	require.NoError(t, err)
	assert.Equal(t, "Numeric 11", doc11["name"])
	assert.Equal(t, "11", doc11["_id"])

	// Verify all original documents are still there
	doc5, err := engine2.GetById("mixed", "5")
	require.NoError(t, err)
	assert.Equal(t, "Numeric 5", doc5["name"])

	docAbc, err := engine2.GetById("mixed", "abc")
	require.NoError(t, err)
	assert.Equal(t, "String abc", docAbc["name"])
}

// Test ID counter restoration with batch operations
func TestStorageEngine_IDCounterRestoration_BatchOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-test-id-batch-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Phase 1: Create collection with batch insert and save
	engine1 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine1.StopBackgroundWorkers()

	docs := make([]domain.Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = domain.Document{"name": fmt.Sprintf("Batch Doc %d", i+1), "value": (i + 1) * 5}
	}

	_, err = engine1.BatchInsert("batch_test", docs)
	require.NoError(t, err)

	// Save to disk
	engine1.saveDirtyCollections()

	// Phase 2: Load in new engine and continue with batch operations
	engine2 := NewStorageEngine(WithDataDir(tempDir), WithNoSaves(true))
	defer engine2.StopBackgroundWorkers()

	// Manually add collection info for per-collection loading
	engine2.mu.Lock()
	engine2.collections["batch_test"] = &CollectionInfo{
		Name:          "batch_test",
		DocumentCount: 10,
		State:         CollectionStateUnloaded,
		LastModified:  time.Now(),
	}
	engine2.mu.Unlock()

	// Trigger loading from disk
	collection, err := engine2.GetCollection("batch_test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 10)

	// Batch insert more documents - should continue from ID "11"
	moreDocs := make([]domain.Document, 5)
	for i := 0; i < 5; i++ {
		moreDocs[i] = domain.Document{"name": fmt.Sprintf("Second Batch Doc %d", i+1), "value": (i + 11) * 5}
	}

	_, err = engine2.BatchInsert("batch_test", moreDocs)
	require.NoError(t, err)

	// Verify new documents got IDs "11" through "15"
	for i := 11; i <= 15; i++ {
		doc, err := engine2.GetById("batch_test", fmt.Sprintf("%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("Second Batch Doc %d", i-10), doc["name"])
		assert.Equal(t, fmt.Sprintf("%d", i), doc["_id"])
	}

	// Verify original documents are still intact
	for i := 1; i <= 10; i++ {
		doc, err := engine2.GetById("batch_test", fmt.Sprintf("%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("Batch Doc %d", i), doc["name"])
	}
}
