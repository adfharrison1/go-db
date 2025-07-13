package main

import (
	"fmt"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/storage"
)

func TestDebugInsertAndFind(t *testing.T) {
	// Create engine with higher cache capacity
	engine := storage.NewStorageEngine(storage.WithMaxMemory(10000)) // 10GB to get higher cache capacity

	// Create collection
	err := engine.CreateCollection("test")
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert multiple documents to see if the issue is consistent
	docs := []domain.Document{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
		{"name": "Charlie", "age": 35},
	}

	for i, doc := range docs {
		err := engine.Insert("test", doc)
		if err != nil {
			t.Fatalf("Failed to insert document %d: %v", i, err)
		}
	}

	// Debug: Check if collection exists in cache
	collection, err := engine.GetCollection("test")
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}

	fmt.Printf("Collection has %d documents\n", len(collection.Documents))
	for id, doc := range collection.Documents {
		fmt.Printf("Document %s: %+v\n", id, doc)
	}

	// Try to find documents with different filters
	fmt.Println("Testing FindAll with nil filter...")
	results1, err := engine.FindAll("test", nil, &domain.PaginationOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("Failed to find documents with nil filter: %v", err)
	}
	fmt.Printf("FindAll with nil filter returned %d documents\n", len(results1.Documents))

	fmt.Println("Testing FindAll with empty filter...")
	results2, err := engine.FindAll("test", map[string]interface{}{}, &domain.PaginationOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("Failed to find documents with empty filter: %v", err)
	}
	fmt.Printf("FindAll with empty filter returned %d documents\n", len(results2.Documents))

	fmt.Println("Testing FindAll with specific filter...")
	results3, err := engine.FindAll("test", map[string]interface{}{"name": "Alice"}, &domain.PaginationOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("Failed to find documents with specific filter: %v", err)
	}
	fmt.Printf("FindAll with specific filter returned %d documents\n", len(results3.Documents))

	// Test GetById to see if individual document retrieval works
	fmt.Println("Testing GetById...")
	doc1, err := engine.GetById("test", "1")
	if err != nil {
		t.Fatalf("Failed to get document by ID: %v", err)
	}
	fmt.Printf("GetById returned: %+v\n", doc1)

	if len(results1.Documents) == 0 {
		t.Error("FindAll returned 0 documents, expected 3")
	}
}
