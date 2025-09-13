package v2

import (
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

func TestNewStorageEngine(t *testing.T) {
	// Test basic creation
	engine := NewStorageEngine()
	if engine == nil {
		t.Fatal("Expected engine to be created")
	}

	// Test with options
	engine = NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
		WithMaxMemory(512),
		WithDurabilityLevel(DurabilityFull),
		WithCheckpointInterval(10*time.Second),
	)

	if engine.walDir != "/tmp/test-wal" {
		t.Errorf("Expected WAL dir to be /tmp/test-wal, got %s", engine.walDir)
	}

	if engine.dataDir != "/tmp/test-data" {
		t.Errorf("Expected data dir to be /tmp/test-data, got %s", engine.dataDir)
	}

	if engine.maxMemoryMB != 512 {
		t.Errorf("Expected max memory to be 512, got %d", engine.maxMemoryMB)
	}

	if engine.durabilityLevel != DurabilityFull {
		t.Errorf("Expected durability level to be DurabilityFull, got %d", engine.durabilityLevel)
	}
}

func TestStorageEngine_Insert(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Test document insertion
	doc := domain.Document{
		"_id":   "test-1",
		"name":  "Test Document",
		"value": 42,
	}

	result, err := engine.Insert("test_collection", doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if result["_id"] != "test-1" {
		t.Errorf("Expected document ID to be test-1, got %v", result["_id"])
	}

	// Verify document can be retrieved
	retrieved, err := engine.GetById("test_collection", "test-1")
	if err != nil {
		t.Fatalf("Failed to retrieve document: %v", err)
	}

	if retrieved["name"] != "Test Document" {
		t.Errorf("Expected name to be 'Test Document', got %v", retrieved["name"])
	}
}

func TestStorageEngine_BatchInsert(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	docs := []domain.Document{
		{"_id": "batch-1", "name": "Document 1"},
		{"_id": "batch-2", "name": "Document 2"},
		{"_id": "batch-3", "name": "Document 3"},
	}

	results, err := engine.BatchInsert("test_collection", docs)
	if err != nil {
		t.Fatalf("Failed to batch insert documents: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("Expected 3 documents, got %d", len(results))
	}

	// Verify all documents can be retrieved
	for i, doc := range results {
		retrieved, err := engine.GetById("test_collection", doc["_id"].(string))
		if err != nil {
			t.Fatalf("Failed to retrieve document %d: %v", i, err)
		}

		if retrieved["name"] != doc["name"] {
			t.Errorf("Expected name to be %v, got %v", doc["name"], retrieved["name"])
		}
	}
}

func TestStorageEngine_UpdateById(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Insert initial document
	doc := domain.Document{
		"_id":   "update-test",
		"name":  "Original Name",
		"value": 100,
	}

	_, err := engine.Insert("test_collection", doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Update document
	updates := domain.Document{
		"name":  "Updated Name",
		"value": 200,
	}

	updated, err := engine.UpdateById("test_collection", "update-test", updates)
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	if updated["name"] != "Updated Name" {
		t.Errorf("Expected name to be 'Updated Name', got %v", updated["name"])
	}

	if updated["value"] != 200 {
		t.Errorf("Expected value to be 200, got %v", updated["value"])
	}

	// Verify update persisted
	retrieved, err := engine.GetById("test_collection", "update-test")
	if err != nil {
		t.Fatalf("Failed to retrieve updated document: %v", err)
	}

	if retrieved["name"] != "Updated Name" {
		t.Errorf("Expected retrieved name to be 'Updated Name', got %v", retrieved["name"])
	}
}

func TestStorageEngine_DeleteById(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Insert document
	doc := domain.Document{
		"_id":  "delete-test",
		"name": "To Be Deleted",
	}

	_, err := engine.Insert("test_collection", doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Delete document
	err = engine.DeleteById("test_collection", "delete-test")
	if err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	// Verify document is deleted
	_, err = engine.GetById("test_collection", "delete-test")
	if err == nil {
		t.Error("Expected document to be deleted, but it still exists")
	}
}

func TestStorageEngine_FindAll(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Insert test documents
	docs := []domain.Document{
		{"_id": "find-1", "name": "Alice", "age": 30},
		{"_id": "find-2", "name": "Bob", "age": 25},
		{"_id": "find-3", "name": "Charlie", "age": 35},
	}

	for _, doc := range docs {
		_, err := engine.Insert("test_collection", doc)
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Test find all
	result, err := engine.FindAll("test_collection", map[string]interface{}{}, nil)
	if err != nil {
		t.Fatalf("Failed to find all documents: %v", err)
	}

	if result.Total != 3 {
		t.Errorf("Expected 3 documents, got %d", result.Total)
	}

	// Test find with filter
	result, err = engine.FindAll("test_collection", map[string]interface{}{"age": 30}, nil)
	if err != nil {
		t.Fatalf("Failed to find filtered documents: %v", err)
	}

	if result.Total != 1 {
		t.Errorf("Expected 1 document with age 30, got %d", result.Total)
	}

	if result.Documents[0]["name"] != "Alice" {
		t.Errorf("Expected Alice, got %v", result.Documents[0]["name"])
	}
}

func TestStorageEngine_GetMemoryStats(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	stats := engine.GetMemoryStats()
	if stats == nil {
		t.Fatal("Expected stats to be returned")
	}

	// Insert a document to generate some stats
	doc := domain.Document{"_id": "stats-test", "name": "Test"}
	_, err := engine.Insert("test_collection", doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	stats = engine.GetMemoryStats()
	if stats["collection_count"] == nil {
		t.Error("Expected collection_count in stats")
	}
}

func TestStorageEngine_CreateIndex(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Create collection first
	if err := engine.CreateCollection("test_collection"); err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert some test documents
	docs := []domain.Document{
		{"_id": "doc1", "name": "Alice", "age": 30},
		{"_id": "doc2", "name": "Bob", "age": 25},
		{"_id": "doc3", "name": "Charlie", "age": 35},
	}

	for _, doc := range docs {
		_, err := engine.Insert("test_collection", doc)
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Create index on name field
	if err := engine.CreateIndex("test_collection", "name"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Test finding documents by index
	results, err := engine.FindByIndex("test_collection", "name", "Alice")
	if err != nil {
		t.Fatalf("Failed to find by index: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0]["name"] != "Alice" {
		t.Errorf("Expected Alice, got %v", results[0]["name"])
	}
}

func TestStorageEngine_IndexUpdates(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Create collection and insert document
	doc := domain.Document{"_id": "doc1", "name": "Alice", "age": 30}
	_, err := engine.Insert("test_collection", doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Create index on name field
	if err := engine.CreateIndex("test_collection", "name"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Update document
	updates := domain.Document{"name": "Alice Updated"}
	_, err = engine.UpdateById("test_collection", "doc1", updates)
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// Test that index was updated
	results, err := engine.FindByIndex("test_collection", "name", "Alice Updated")
	if err != nil {
		t.Fatalf("Failed to find by index: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0]["name"] != "Alice Updated" {
		t.Errorf("Expected Alice Updated, got %v", results[0]["name"])
	}

	// Test that old value is no longer indexed
	oldResults, err := engine.FindByIndex("test_collection", "name", "Alice")
	if err != nil {
		t.Fatalf("Failed to find by index: %v", err)
	}

	if len(oldResults) != 0 {
		t.Errorf("Expected 0 results for old value, got %d", len(oldResults))
	}
}

func TestStorageEngine_IndexDeletion(t *testing.T) {
	engine := NewStorageEngine(
		WithWALDir("/tmp/test-wal"),
		WithDataDir("/tmp/test-data"),
	)

	// Create collection and insert document
	doc := domain.Document{"_id": "doc1", "name": "Alice", "age": 30}
	_, err := engine.Insert("test_collection", doc)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Create index on name field
	if err := engine.CreateIndex("test_collection", "name"); err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Delete document
	err = engine.DeleteById("test_collection", "doc1")
	if err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	// Test that document is no longer found by index
	results, err := engine.FindByIndex("test_collection", "name", "Alice")
	if err != nil {
		t.Fatalf("Failed to find by index: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results after deletion, got %d", len(results))
	}
}
