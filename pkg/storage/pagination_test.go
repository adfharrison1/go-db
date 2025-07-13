package storage

import (
	"fmt"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagination_OffsetBased(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert 10 documents
	for i := 1; i <= 10; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
			"age":  i * 10,
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	tests := []struct {
		name            string
		limit           int
		offset          int
		expectedCount   int
		expectedHasNext bool
		expectedHasPrev bool
	}{
		{
			name:            "first page",
			limit:           3,
			offset:          0,
			expectedCount:   3,
			expectedHasNext: true,
			expectedHasPrev: false,
		},
		{
			name:            "second page",
			limit:           3,
			offset:          3,
			expectedCount:   3,
			expectedHasNext: true,
			expectedHasPrev: true,
		},
		{
			name:            "last page",
			limit:           3,
			offset:          9,
			expectedCount:   1,
			expectedHasNext: false,
			expectedHasPrev: true,
		},
		{
			name:            "beyond end",
			limit:           3,
			offset:          15,
			expectedCount:   0,
			expectedHasNext: false,
			expectedHasPrev: false,
		},
		{
			name:            "large limit",
			limit:           100,
			offset:          0,
			expectedCount:   10,
			expectedHasNext: false,
			expectedHasPrev: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := &domain.PaginationOptions{
				Limit:    tt.limit,
				Offset:   tt.offset,
				MaxLimit: 1000, // Set a reasonable max limit
			}

			result, err := engine.FindAll("users", nil, options)
			require.NoError(t, err)

			assert.Equal(t, tt.expectedCount, len(result.Documents))
			assert.Equal(t, tt.expectedHasNext, result.HasNext)
			assert.Equal(t, tt.expectedHasPrev, result.HasPrev)
			assert.Equal(t, int64(10), result.Total)

			// Verify document ordering (should be sorted by ID)
			for i := 1; i < len(result.Documents); i++ {
				id1 := result.Documents[i-1]["_id"].(string)
				id2 := result.Documents[i]["_id"].(string)
				assert.Less(t, id1, id2, "Documents should be sorted by ID")
			}
		})
	}
}

func TestPagination_CursorBased(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert 10 documents
	for i := 1; i <= 10; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
			"age":  i * 10,
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Get first page
	options := &domain.PaginationOptions{
		Limit:    3,
		MaxLimit: 1000,
	}
	result, err := engine.FindAll("users", nil, options)
	require.NoError(t, err)

	assert.Equal(t, 3, len(result.Documents))
	assert.True(t, result.HasNext)
	assert.False(t, result.HasPrev)
	assert.NotEmpty(t, result.NextCursor)

	// Get second page using cursor
	options2 := &domain.PaginationOptions{
		Limit:    3,
		MaxLimit: 1000,
		After:    result.NextCursor,
	}
	result2, err := engine.FindAll("users", nil, options2)
	require.NoError(t, err)

	assert.Equal(t, 3, len(result2.Documents))
	assert.True(t, result2.HasNext)
	assert.True(t, result2.HasPrev)

	// Verify documents are different
	firstPageIDs := make(map[string]bool)
	for _, doc := range result.Documents {
		firstPageIDs[doc["_id"].(string)] = true
	}

	for _, doc := range result2.Documents {
		assert.False(t, firstPageIDs[doc["_id"].(string)], "Document should not be in first page")
	}
}

func TestPagination_WithFilter(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert documents with different ages
	for i := 1; i <= 20; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 5, // 0, 1, 2, 3, 4 repeating
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test pagination with filter
	filter := map[string]interface{}{"age": 2}
	options := &domain.PaginationOptions{
		Limit:    2,
		Offset:   0,
		MaxLimit: 1000,
	}

	result, err := engine.FindAll("users", filter, options)
	require.NoError(t, err)

	// Should have 4 documents with age=2 (2, 7, 12, 17)
	assert.Equal(t, 2, len(result.Documents))
	assert.True(t, result.HasNext)
	assert.False(t, result.HasPrev)
	assert.Equal(t, int64(4), result.Total)

	// Verify all returned documents match the filter
	for _, doc := range result.Documents {
		assert.Equal(t, 2, doc["age"])
	}
}

func TestPagination_Validation(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Test negative limit
	options := &domain.PaginationOptions{
		Limit: -1,
	}
	_, err = engine.FindAll("users", nil, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "limit cannot be negative")

	// Test negative offset
	options = &domain.PaginationOptions{
		Offset: -1,
	}
	_, err = engine.FindAll("users", nil, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "offset cannot be negative")

	// Test mixing cursor and offset pagination
	options = &domain.PaginationOptions{
		After:  "some-cursor",
		Offset: 10,
	}
	_, err = engine.FindAll("users", nil, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot mix cursor-based and offset-based pagination")
}

func TestPagination_Streaming(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create collection and insert test data
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert 10 documents
	for i := 1; i <= 10; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
			"age":  i * 10,
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test streaming - should return all documents (no pagination at storage level)
	docChan, err := engine.FindAllStream("users", nil)
	require.NoError(t, err)

	var docs []domain.Document
	for doc := range docChan {
		docs = append(docs, doc)
	}

	// Streaming should return all documents, pagination is handled by API layer
	assert.Equal(t, 10, len(docs))

	// Note: Streaming doesn't guarantee document ordering
	// Documents are returned in the order they're stored in the collection
}

func TestPagination_EmptyCollection(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	err := engine.CreateCollection("users")
	require.NoError(t, err)

	options := &domain.PaginationOptions{
		Limit:    10,
		Offset:   0,
		MaxLimit: 1000,
	}

	result, err := engine.FindAll("users", nil, options)
	require.NoError(t, err)

	assert.Equal(t, 0, len(result.Documents))
	assert.False(t, result.HasNext)
	assert.False(t, result.HasPrev)
	assert.Equal(t, int64(0), result.Total)
}

func TestPagination_MaxLimit(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert 10 documents
	for i := 1; i <= 10; i++ {
		doc := domain.Document{
			"name": fmt.Sprintf("user%d", i),
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test with limit exceeding max
	options := &domain.PaginationOptions{
		Limit:    2000, // Exceeds max of 1000
		MaxLimit: 1000,
	}

	_, err = engine.FindAll("users", nil, options)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "limit 2000 exceeds maximum 1000")
}
