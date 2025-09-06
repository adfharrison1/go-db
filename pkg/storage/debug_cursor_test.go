package storage

import (
	"fmt"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestDebugCursorPagination(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert 5 documents
	for i := 1; i <= 5; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
			"age":  i * 10,
		}
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Check what documents are in the collection
	collection, err := engine.GetCollection("users")
	require.NoError(t, err)

	fmt.Printf("Collection has %d documents:\n", len(collection.Documents))
	for id, doc := range collection.Documents {
		fmt.Printf("  ID: %s, Name: %s, Age: %v\n", id, doc["name"], doc["age"])
	}

	// Get first page
	options := &domain.PaginationOptions{
		Limit:    2,
		MaxLimit: 1000,
	}
	result, err := engine.FindAll("users", nil, options)
	require.NoError(t, err)

	fmt.Printf("First page result: %d documents\n", len(result.Documents))
	fmt.Printf("HasNext: %v, HasPrev: %v\n", result.HasNext, result.HasPrev)
	fmt.Printf("NextCursor: %s\n", result.NextCursor)

	for i, doc := range result.Documents {
		fmt.Printf("  Doc %d: ID=%s, Name=%s, Age=%v\n", i, doc["_id"], doc["name"], doc["age"])
	}

	// Try to decode the cursor
	if result.NextCursor != "" {
		cursor, err := domain.DecodeCursor(result.NextCursor)
		if err != nil {
			fmt.Printf("Error decoding cursor: %v\n", err)
		} else {
			fmt.Printf("Decoded cursor: ID=%s, Timestamp=%v\n", cursor.ID, cursor.Timestamp)
		}
	}

	// Get second page using cursor
	if result.NextCursor != "" {
		options2 := &domain.PaginationOptions{
			Limit:    2,
			MaxLimit: 1000,
			After:    result.NextCursor,
		}
		result2, err := engine.FindAll("users", nil, options2)
		require.NoError(t, err)

		fmt.Printf("Second page result: %d documents\n", len(result2.Documents))
		fmt.Printf("HasNext: %v, HasPrev: %v\n", result2.HasNext, result2.HasPrev)

		for i, doc := range result2.Documents {
			fmt.Printf("  Doc %d: ID=%s, Name=%s, Age=%v\n", i, doc["_id"], doc["name"], doc["age"])
		}
	}
}
