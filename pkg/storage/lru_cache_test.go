package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
)

func TestNewLRUCache(t *testing.T) {
	cache := NewLRUCache(100)
	assert.NotNil(t, cache)
	assert.Equal(t, 100, cache.Capacity())
	assert.Equal(t, 0, cache.Len())
}

func TestLRUCache_GetAndPut(t *testing.T) {
	cache := NewLRUCache(10)

	// Create test collections
	collection1 := domain.NewCollection("users")
	collection1.Documents["1"] = domain.Document{"name": "Alice", "age": 30}

	collection2 := domain.NewCollection("posts")
	collection2.Documents["1"] = domain.Document{"title": "Hello", "content": "World"}

	// Put collections
	cache.Put("users", collection1, &CollectionInfo{Name: "users"})
	cache.Put("posts", collection2, &CollectionInfo{Name: "posts"})

	// Get collections
	col1, info1, found1 := cache.Get("users")
	assert.True(t, found1)
	assert.Equal(t, "users", col1.Name)
	assert.Equal(t, "users", info1.Name)

	col2, info2, found2 := cache.Get("posts")
	assert.True(t, found2)
	assert.Equal(t, "posts", col2.Name)
	assert.Equal(t, "posts", info2.Name)

	// Test non-existent key
	_, _, found3 := cache.Get("nonexistent")
	assert.False(t, found3)
}

func TestLRUCache_CapacityAndEviction(t *testing.T) {
	cache := NewLRUCache(2) // Small capacity to test eviction

	// Create test collections
	col1 := domain.NewCollection("collection1")
	col2 := domain.NewCollection("collection2")
	col3 := domain.NewCollection("collection3")

	// Add collections
	cache.Put("coll1", col1, &CollectionInfo{Name: "coll1"})
	cache.Put("coll2", col2, &CollectionInfo{Name: "coll2"})
	cache.Put("coll3", col3, &CollectionInfo{Name: "coll3"})

	// Should have evicted the first one (LRU)
	assert.Equal(t, 2, cache.Len())

	// First collection should be evicted
	_, _, found1 := cache.Get("coll1")
	assert.False(t, found1)

	// Last two should still be there
	_, _, found2 := cache.Get("coll2")
	assert.True(t, found2)

	_, _, found3 := cache.Get("coll3")
	assert.True(t, found3)
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	cache := NewLRUCache(10)

	// Create initial collection
	collection1 := domain.NewCollection("users")
	collection1.Documents["1"] = domain.Document{"name": "Alice"}

	// Put initial collection
	cache.Put("users", collection1, &CollectionInfo{Name: "users"})

	// Update with new collection
	collection2 := domain.NewCollection("users")
	collection2.Documents["1"] = domain.Document{"name": "Bob"}

	cache.Put("users", collection2, &CollectionInfo{Name: "users"})

	// Should have updated the existing entry
	assert.Equal(t, 1, cache.Len())

	// Get the updated collection
	col, info, found := cache.Get("users")
	assert.True(t, found)
	assert.Equal(t, "Bob", col.Documents["1"]["name"])
	assert.Equal(t, "users", info.Name)
}

func TestLRUCache_AccessCountAndTimestamps(t *testing.T) {
	cache := NewLRUCache(10)

	collection := domain.NewCollection("users")
	info := &CollectionInfo{Name: "users"}

	cache.Put("users", collection, info)

	// Initial access
	_, info1, _ := cache.Get("users")
	assert.Equal(t, int64(1), info1.AccessCount)
	assert.True(t, info1.LastAccessed.After(time.Now().Add(-time.Second)))

	// Second access
	_, info2, _ := cache.Get("users")
	assert.Equal(t, int64(2), info2.AccessCount)
	// Allow LastAccessed to be equal or after, due to time resolution
	assert.True(t, !info2.LastAccessed.Before(info1.LastAccessed))
}

func TestLRUCache_Remove(t *testing.T) {
	cache := NewLRUCache(10)

	collection := domain.NewCollection("users")
	cache.Put("users", collection, &CollectionInfo{Name: "users"})

	// Verify it exists
	_, _, found := cache.Get("users")
	assert.True(t, found)

	// Remove it
	cache.Remove("users")

	// Verify it's gone
	_, _, found = cache.Get("users")
	assert.False(t, found)
	assert.Equal(t, 0, cache.Len())
}

func TestLRUCache_RemoveNonExistent(t *testing.T) {
	cache := NewLRUCache(10)

	// Remove non-existent key should not panic
	cache.Remove("nonexistent")
	assert.Equal(t, 0, cache.Len())
}

func TestLRUCache_Concurrency(t *testing.T) {
	cache := NewLRUCache(100)
	var wg sync.WaitGroup

	// Start multiple goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			collection := domain.NewCollection("collection" + string(rune(id)))
			info := &CollectionInfo{Name: "collection" + string(rune(id))}

			// Put and get operations
			cache.Put("key"+string(rune(id)), collection, info)
			_, _, found := cache.Get("key" + string(rune(id)))
			assert.True(t, found)
		}(i)
	}

	wg.Wait()
	assert.Equal(t, 10, cache.Len())
}

func TestLRUCache_EdgeCases(t *testing.T) {
	// Test zero capacity
	cache := NewLRUCache(0)
	collection := domain.NewCollection("users")
	cache.Put("users", collection, &CollectionInfo{Name: "users"})

	// Should not store anything
	assert.Equal(t, 0, cache.Len())

	// Test negative capacity (should work as zero)
	cache = NewLRUCache(-1)
	cache.Put("users", collection, &CollectionInfo{Name: "users"})
	assert.Equal(t, 0, cache.Len())
}

func TestLRUCache_Performance(t *testing.T) {
	cache := NewLRUCache(1000)

	start := time.Now()

	// Put 1000 items
	for i := 0; i < 1000; i++ {
		collection := domain.NewCollection("collection" + string(rune(i)))
		cache.Put("key"+string(rune(i)), collection, &CollectionInfo{Name: "collection" + string(rune(i))})
	}

	putTime := time.Since(start)
	t.Logf("Put 1000 items: %v", putTime)

	start = time.Now()

	// Get 1000 items
	for i := 0; i < 1000; i++ {
		_, _, found := cache.Get("key" + string(rune(i)))
		assert.True(t, found)
	}

	getTime := time.Since(start)
	t.Logf("Get 1000 items: %v", getTime)

	// Performance should be reasonable
	assert.Less(t, putTime, 10*time.Millisecond)
	assert.Less(t, getTime, 10*time.Millisecond)
}
