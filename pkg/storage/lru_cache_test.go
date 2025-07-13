package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/data"
	"github.com/stretchr/testify/assert"
)

func TestNewLRUCache(t *testing.T) {
	cache := NewLRUCache(10)

	assert.Equal(t, 10, cache.Capacity())
	assert.Equal(t, 0, cache.Len())
	assert.Equal(t, 0, cache.CacheLen())
}

func TestLRUCache_GetAndPut(t *testing.T) {
	cache := NewLRUCache(3)

	// Test putting and getting items
	collection1 := data.NewCollection("test1")
	collection2 := data.NewCollection("test2")

	info1 := &CollectionInfo{Name: "test1", State: CollectionStateLoaded}
	info2 := &CollectionInfo{Name: "test2", State: CollectionStateLoaded}

	// Put items
	cache.Put("key1", collection1, info1)
	cache.Put("key2", collection2, info2)

	// Get items
	col1, info1Ret, found1 := cache.Get("key1")
	assert.True(t, found1)
	assert.Equal(t, collection1, col1)
	assert.Equal(t, info1, info1Ret)

	col2, info2Ret, found2 := cache.Get("key2")
	assert.True(t, found2)
	assert.Equal(t, collection2, col2)
	assert.Equal(t, info2, info2Ret)

	// Test getting non-existent key
	_, _, found3 := cache.Get("nonexistent")
	assert.False(t, found3)
}

func TestLRUCache_CapacityAndEviction(t *testing.T) {
	cache := NewLRUCache(2)

	// Add 3 items to a cache with capacity 2
	col1 := data.NewCollection("test1")
	col2 := data.NewCollection("test2")
	col3 := data.NewCollection("test3")

	info1 := &CollectionInfo{Name: "test1"}
	info2 := &CollectionInfo{Name: "test2"}
	info3 := &CollectionInfo{Name: "test3"}

	cache.Put("key1", col1, info1)
	cache.Put("key2", col2, info2)
	cache.Put("key3", col3, info3)

	// Verify capacity is maintained
	assert.Equal(t, 2, cache.Len())
	assert.Equal(t, 2, cache.CacheLen())

	// The oldest item (key1) should have been evicted
	_, _, found1 := cache.Get("key1")
	assert.False(t, found1)

	// The newer items should still be there
	_, _, found2 := cache.Get("key2")
	assert.True(t, found2)

	_, _, found3 := cache.Get("key3")
	assert.True(t, found3)
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	cache := NewLRUCache(3)

	collection1 := data.NewCollection("test1")
	collection2 := data.NewCollection("test2")

	info1 := &CollectionInfo{Name: "test1"}
	info2 := &CollectionInfo{Name: "test2"}

	// Put initial item
	cache.Put("key1", collection1, info1)

	// Update the same key
	cache.Put("key1", collection2, info2)

	// Verify the item was updated
	col, info, found := cache.Get("key1")
	assert.True(t, found)
	assert.Equal(t, collection2, col)
	assert.Equal(t, info2, info)

	// Verify only one item in cache
	assert.Equal(t, 1, cache.Len())
	assert.Equal(t, 1, cache.CacheLen())
}

func TestLRUCache_AccessCountAndTimestamps(t *testing.T) {
	cache := NewLRUCache(3)

	collection := data.NewCollection("test")
	info := &CollectionInfo{Name: "test"}

	cache.Put("key1", collection, info)

	// Get the item multiple times
	_, _, _ = cache.Get("key1")
	time.Sleep(1 * time.Millisecond) // Ensure time difference

	_, _, _ = cache.Get("key1")
	time.Sleep(1 * time.Millisecond)

	_, info3, _ := cache.Get("key1")
	// Verify access count increased
	assert.Equal(t, int64(3), info3.AccessCount)
	// Verify last accessed time is set (not zero)
	assert.False(t, info3.LastAccessed.IsZero())
}

func TestLRUCache_Remove(t *testing.T) {
	cache := NewLRUCache(3)

	collection := data.NewCollection("test")
	info := &CollectionInfo{Name: "test"}

	cache.Put("key1", collection, info)

	// Verify item exists
	_, _, found := cache.Get("key1")
	assert.True(t, found)

	// Remove item
	cache.Remove("key1")

	// Verify item no longer exists
	_, _, found = cache.Get("key1")
	assert.False(t, found)

	// Verify cache is empty
	assert.Equal(t, 0, cache.Len())
	assert.Equal(t, 0, cache.CacheLen())
}

func TestLRUCache_RemoveNonExistent(t *testing.T) {
	cache := NewLRUCache(3)

	// Remove non-existent key should not panic
	cache.Remove("nonexistent")

	// Verify cache is still empty
	assert.Equal(t, 0, cache.Len())
	assert.Equal(t, 0, cache.CacheLen())
}

func TestLRUCache_Concurrency(t *testing.T) {
	cache := NewLRUCache(100)

	const numGoroutines = 10
	const operationsPerGoroutine = 100

	var wg sync.WaitGroup

	// Test concurrent puts
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("key_%d_%d", id, j)
				collection := data.NewCollection(key)
				info := &CollectionInfo{Name: key}
				cache.Put(key, collection, info)
			}
		}(i)
	}

	wg.Wait()

	// Verify all items were added (some may have been evicted due to capacity)
	assert.LessOrEqual(t, cache.Len(), 100)
	assert.LessOrEqual(t, cache.CacheLen(), 100)
}

func TestLRUCache_EdgeCases(t *testing.T) {
	// Test zero capacity cache
	cache := NewLRUCache(0)

	collection := data.NewCollection("test")
	info := &CollectionInfo{Name: "test"}

	cache.Put("key1", collection, info)

	// Item should be immediately evicted
	_, _, found := cache.Get("key1")
	assert.False(t, found)

	// Test negative capacity (should work but not practical)
	cache2 := NewLRUCache(-1)
	cache2.Put("key1", collection, info)

	// Should not panic
	assert.NotNil(t, cache2)
}

func TestLRUCache_Performance(t *testing.T) {
	cache := NewLRUCache(1000)

	// Benchmark put operations
	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		collection := data.NewCollection(key)
		info := &CollectionInfo{Name: key}
		cache.Put(key, collection, info)
	}
	putTime := time.Since(start)

	// Benchmark get operations
	start = time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		cache.Get(key)
	}
	getTime := time.Since(start)

	// Verify reasonable performance (should be very fast)
	assert.Less(t, putTime, 100*time.Millisecond)
	assert.Less(t, getTime, 100*time.Millisecond)

	t.Logf("Put 1000 items: %v", putTime)
	t.Logf("Get 1000 items: %v", getTime)
}
