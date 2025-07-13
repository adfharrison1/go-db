package indexing_test

import (
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateIndex(t *testing.T) {
	engine := storage.NewStorageEngine()

	// Create a collection first
	err := engine.CreateCollection("test")
	require.NoError(t, err)

	// Insert some test documents
	docs := []domain.Document{
		{"name": "Alice", "age": 25, "city": "New York"},
		{"name": "Bob", "age": 30, "city": "Boston"},
		{"name": "Charlie", "age": 25, "city": "New York"},
		{"name": "David", "age": 35, "city": "Chicago"},
	}

	for _, doc := range docs {
		err := engine.Insert("test", doc)
		require.NoError(t, err)
	}

	// Test creating index on "name" field
	err = engine.CreateIndex("test", "name")
	assert.NoError(t, err)

	// Test creating index on "age" field
	err = engine.CreateIndex("test", "age")
	assert.NoError(t, err)

	// Test creating duplicate index (should fail)
	err = engine.CreateIndex("test", "name")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Test creating index on non-existent collection
	err = engine.CreateIndex("nonexistent", "name")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestIndexOptimization(t *testing.T) {
	engine := storage.NewStorageEngine()

	// Create collection and insert documents
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	users := []domain.Document{
		{"name": "Alice", "age": 25, "role": "admin"},
		{"name": "Bob", "age": 30, "role": "user"},
		{"name": "Charlie", "age": 25, "role": "user"},
		{"name": "David", "age": 35, "role": "admin"},
		{"name": "Eve", "age": 28, "role": "user"},
	}

	for _, user := range users {
		err := engine.Insert("users", user)
		require.NoError(t, err)
	}

	// Create index on "role" field
	err = engine.CreateIndex("users", "role")
	require.NoError(t, err)

	// Test query using indexed field
	results, err := engine.FindAll("users", map[string]interface{}{"role": "admin"}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 2)

	// Verify results contain admin users
	for _, result := range results.Documents {
		assert.Equal(t, "admin", result["role"])
	}

	// Test query using non-indexed field (should fall back to full scan)
	results, err = engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 2)

	// Test query with multiple conditions (one indexed, one not)
	results, err = engine.FindAll("users", map[string]interface{}{
		"role": "user",
		"age":  30,
	}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 1)
	assert.Equal(t, "Bob", results.Documents[0]["name"])
}

func TestIndexMaintenance(t *testing.T) {
	engine := storage.NewStorageEngine()

	// Create collection and index
	err := engine.CreateCollection("products")
	require.NoError(t, err)

	err = engine.CreateIndex("products", "category")
	require.NoError(t, err)

	// Insert initial documents
	products := []domain.Document{
		{"name": "Laptop", "category": "electronics", "price": 999},
		{"name": "Phone", "category": "electronics", "price": 599},
		{"name": "Book", "category": "books", "price": 19},
	}

	for _, product := range products {
		err := engine.Insert("products", product)
		require.NoError(t, err)
	}

	// Verify initial index state
	results, err := engine.FindAll("products", map[string]interface{}{"category": "electronics"}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 2)

	// Update a document
	err = engine.UpdateById("products", "1", domain.Document{"category": "computers"})
	assert.NoError(t, err)

	// Verify index is updated
	results, err = engine.FindAll("products", map[string]interface{}{"category": "electronics"}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 1)

	results, err = engine.FindAll("products", map[string]interface{}{"category": "computers"}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 1)

	// Delete a document
	err = engine.DeleteById("products", "2")
	assert.NoError(t, err)

	// Verify index is updated
	results, err = engine.FindAll("products", map[string]interface{}{"category": "electronics"}, nil)
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 0)
}

func TestAutomaticIdIndex(t *testing.T) {
	engine := storage.NewStorageEngine()

	// Create collection (should automatically create _id index)
	err := engine.CreateCollection("test")
	require.NoError(t, err)

	// Insert documents
	docs := []domain.Document{
		{"name": "Alice"},
		{"name": "Bob"},
		{"name": "Charlie"},
	}

	for _, doc := range docs {
		err := engine.Insert("test", doc)
		require.NoError(t, err)
	}

	// Test GetById operations (should use _id index)
	doc, err := engine.GetById("test", "1")
	assert.NoError(t, err)
	assert.Equal(t, "Alice", doc["name"])

	doc, err = engine.GetById("test", "2")
	assert.NoError(t, err)
	assert.Equal(t, "Bob", doc["name"])

	// Test UpdateById operations
	err = engine.UpdateById("test", "1", domain.Document{"name": "Alice Updated"})
	assert.NoError(t, err)

	doc, err = engine.GetById("test", "1")
	assert.NoError(t, err)
	assert.Equal(t, "Alice Updated", doc["name"])

	// Test DeleteById operations
	err = engine.DeleteById("test", "2")
	assert.NoError(t, err)

	_, err = engine.GetById("test", "2")
	assert.Error(t, err)
}

func TestIndexPerformance(t *testing.T) {
	engine := storage.NewStorageEngine()

	// Create collection with index
	err := engine.CreateCollection("large_collection")
	require.NoError(t, err)

	err = engine.CreateIndex("large_collection", "status")
	require.NoError(t, err)

	// Insert many documents
	for i := 0; i < 1000; i++ {
		status := "active"
		if i%10 == 0 {
			status = "inactive"
		}

		doc := domain.Document{
			"id":     i,
			"status": status,
			"data":   "some data",
		}
		err := engine.Insert("large_collection", doc)
		require.NoError(t, err)
	}

	// Test query performance with index
	results, err := engine.FindAll("large_collection", map[string]interface{}{"status": "inactive"}, &domain.PaginationOptions{Limit: 1000})
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 100) // 100 inactive documents (every 10th)

	// Test query performance without index (should still work)
	results, err = engine.FindAll("large_collection", map[string]interface{}{"data": "some data"}, &domain.PaginationOptions{Limit: 1000})
	assert.NoError(t, err)
	assert.Len(t, results.Documents, 1000)
}
