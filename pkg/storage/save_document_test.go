package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveDocumentToDisk_CorruptedFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-save-document-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Create a corrupted collection file (too small for LZ4 decompression)
	collectionsDir := filepath.Join(tempDir, "collections")
	err = os.MkdirAll(collectionsDir, 0755)
	require.NoError(t, err)

	corruptedFile := filepath.Join(collectionsDir, "test.godb")
	err = os.WriteFile(corruptedFile, []byte("corrupt"), 0644)
	require.NoError(t, err)

	// Insert a document - this should trigger saveDocumentToDisk
	// which will try to load the corrupted file and fall back to starting fresh
	doc := domain.Document{"name": "Test Document", "value": 42}
	resultDoc, err := engine.Insert("test", doc)
	require.NoError(t, err)
	assert.NotEmpty(t, resultDoc["_id"])

	// Verify the document was inserted successfully
	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 1)

	// Find the document by its _id
	docID := resultDoc["_id"].(string)
	assert.Equal(t, doc, collection.Documents[docID])
}

func TestSaveDocumentToDisk_EmptyFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-save-document-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Create an empty collection file
	collectionsDir := filepath.Join(tempDir, "collections")
	err = os.MkdirAll(collectionsDir, 0755)
	require.NoError(t, err)

	emptyFile := filepath.Join(collectionsDir, "test.godb")
	file, err := os.Create(emptyFile)
	require.NoError(t, err)
	file.Close()

	// Insert a document - this should trigger saveDocumentToDisk
	// which will try to load the empty file and fall back to starting fresh
	doc := domain.Document{"name": "Test Document", "value": 42}
	resultDoc, err := engine.Insert("test", doc)
	require.NoError(t, err)
	assert.NotEmpty(t, resultDoc["_id"])

	// Verify the document was inserted successfully
	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 1)

	// Find the document by its _id
	docID := resultDoc["_id"].(string)
	assert.Equal(t, doc, collection.Documents[docID])
}

func TestSaveDocumentToDisk_InvalidHeader(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-save-document-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Create a file with invalid header
	collectionsDir := filepath.Join(tempDir, "collections")
	err = os.MkdirAll(collectionsDir, 0755)
	require.NoError(t, err)

	invalidFile := filepath.Join(collectionsDir, "test.godb")
	err = os.WriteFile(invalidFile, []byte("INVALID_HEADER_DATA"), 0644)
	require.NoError(t, err)

	// Insert a document - this should trigger saveDocumentToDisk
	// which will try to load the invalid file and fall back to starting fresh
	doc := domain.Document{"name": "Test Document", "value": 42}
	resultDoc, err := engine.Insert("test", doc)
	require.NoError(t, err)
	assert.NotEmpty(t, resultDoc["_id"])

	// Verify the document was inserted successfully
	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 1)

	// Find the document by its _id
	docID := resultDoc["_id"].(string)
	assert.Equal(t, doc, collection.Documents[docID])
}

func TestSaveDocumentToDisk_ConcurrentAccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-save-document-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Create a small file that might cause LZ4 decompression issues
	collectionsDir := filepath.Join(tempDir, "collections")
	err = os.MkdirAll(collectionsDir, 0755)
	require.NoError(t, err)

	smallFile := filepath.Join(collectionsDir, "test.godb")
	err = os.WriteFile(smallFile, []byte("small"), 0644)
	require.NoError(t, err)

	// Insert multiple documents concurrently
	// This should trigger multiple saveDocumentToDisk calls that might encounter
	// the small file and fall back to starting fresh
	var docIDs []string
	for i := 0; i < 5; i++ {
		doc := domain.Document{
			"name":  "Test Document",
			"value": i,
			"id":    i,
		}
		resultDoc, err := engine.Insert("test", doc)
		require.NoError(t, err)
		docID := resultDoc["_id"].(string)
		docIDs = append(docIDs, docID)
	}

	// Verify all documents were inserted successfully
	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 5)

	// Verify each document exists
	for _, docID := range docIDs {
		_, exists := collection.Documents[docID]
		assert.True(t, exists, "Document %s should exist", docID)
	}
}

func TestSaveDocumentToDisk_NoExistingFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-save-document-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Don't create any existing file - this should work normally
	doc := domain.Document{"name": "Test Document", "value": 42}
	resultDoc, err := engine.Insert("test", doc)
	require.NoError(t, err)
	assert.NotEmpty(t, resultDoc["_id"])

	// Verify the document was inserted successfully
	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 1)

	// Find the document by its _id
	docID := resultDoc["_id"].(string)
	assert.Equal(t, doc, collection.Documents[docID])
}

func TestSaveDocumentToDisk_ValidExistingFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-save-document-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Create collection
	err = engine.CreateCollection("test")
	require.NoError(t, err)

	// Insert a document first to create a valid file
	doc1 := domain.Document{"name": "First Document", "value": 1}
	resultDoc1, err := engine.Insert("test", doc1)
	require.NoError(t, err)
	docID1 := resultDoc1["_id"].(string)

	// Insert another document - this should load the existing file and merge
	doc2 := domain.Document{"name": "Second Document", "value": 2}
	resultDoc2, err := engine.Insert("test", doc2)
	require.NoError(t, err)
	docID2 := resultDoc2["_id"].(string)

	// Verify both documents exist
	collection, err := engine.GetCollection("test")
	require.NoError(t, err)
	assert.Len(t, collection.Documents, 2)
	assert.Equal(t, doc1, collection.Documents[docID1])
	assert.Equal(t, doc2, collection.Documents[docID2])
}
