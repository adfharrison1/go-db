package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Performance thresholds - tests will fail if performance degrades below these
const (
	// Indexed queries should be at least 5x faster than non-indexed for large datasets
	IndexedSpeedupThreshold = 5.0

	// Memory usage should not exceed 100MB for 10K documents
	MaxMemoryMB = 100

	// Streaming should maintain at least 1M docs/sec throughput
	MinStreamingThroughput = 1000000

	// Index intersection should be at least 1.5x faster than single index for multi-field queries
	// (lower threshold because intersection returns fewer results, so it's naturally faster)
	IntersectionSpeedupThreshold = 1.5

	// Large dataset size (won't kill laptop but tests performance)
	LargeDatasetSize = 10000
)

// TestIndexedVsNonIndexedPerformance measures the performance improvement
// of indexed queries vs non-indexed queries
func TestIndexedVsNonIndexedPerformance(t *testing.T) {
	engine := NewStorageEngine()

	// Create collection and insert large dataset
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert large dataset with varied data
	for i := 0; i < LargeDatasetSize; i++ {
		doc := domain.Document{
			"id":    fmt.Sprintf("%d", i),
			"name":  fmt.Sprintf("user%d", i),
			"age":   i % 100,                     // 0-99
			"city":  fmt.Sprintf("city%d", i%50), // 50 cities
			"role":  fmt.Sprintf("role%d", i%10), // 10 roles
			"score": i % 1000,                    // 0-999
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create indexes on commonly queried fields
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("users", "city")
	require.NoError(t, err)
	err = engine.CreateIndex("users", "role")
	require.NoError(t, err)

	// Test single field queries
	t.Run("SingleField_IndexedVsNonIndexed", func(t *testing.T) {
		// Query that will use index
		start := time.Now()
		indexedResults, err := engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
		indexedDuration := time.Since(start)
		require.NoError(t, err)

		// Query that won't use index (no index on name)
		start = time.Now()
		nonIndexedResults, err := engine.FindAll("users", map[string]interface{}{"name": "user25"}, nil)
		nonIndexedDuration := time.Since(start)
		require.NoError(t, err)

		// Verify results are correct
		assert.Len(t, indexedResults, 100)  // Every 100th user has age 25
		assert.Len(t, nonIndexedResults, 1) // Only one user named "user25"

		// Performance assertion: indexed should be significantly faster
		speedup := float64(nonIndexedDuration) / float64(indexedDuration)
		t.Logf("Indexed query: %v, Non-indexed query: %v, Speedup: %.2fx",
			indexedDuration, nonIndexedDuration, speedup)

		assert.GreaterOrEqual(t, speedup, IndexedSpeedupThreshold,
			"Indexed queries should be at least %fx faster than non-indexed", IndexedSpeedupThreshold)
	})

	// Test multi-field queries
	t.Run("MultiField_IndexIntersection", func(t *testing.T) {
		// Query using multiple indexes (age AND city)
		start := time.Now()
		intersectionResults, err := engine.FindAll("users", map[string]interface{}{
			"age":  25,
			"city": "city25", // This should match users with id 25, 125, 225, etc.
		}, nil)
		intersectionDuration := time.Since(start)
		require.NoError(t, err)

		// Query using only one index (age only)
		start = time.Now()
		singleIndexResults, err := engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
		singleIndexDuration := time.Since(start)
		require.NoError(t, err)

		// Query without any index (name field)
		start = time.Now()
		nonIndexedResults, err := engine.FindAll("users", map[string]interface{}{"name": "user25"}, nil)
		nonIndexedDuration := time.Since(start)
		require.NoError(t, err)

		// Verify results
		// Users with age 25: id 25, 125, 225, 325, 425, 525, 625, 725, 825, 925, 1025, 1125, etc.
		// Users with city "city25": id 25, 75, 125, 175, 225, 275, etc.
		// Intersection: id 25, 125, 225, 325, 425, 525, 625, 725, 825, 925, 1025, 1125, etc.
		expectedIntersection := 100 // Every 100th user has age 25, and every 50th user has city25, so intersection is 100
		assert.Len(t, intersectionResults, expectedIntersection)
		assert.Len(t, singleIndexResults, 100) // 100 users with age 25
		assert.Len(t, nonIndexedResults, 1)    // Only one user named "user25"

		// Performance assertion: intersection should be much faster than non-indexed
		// (even though it might be slower than single index due to intersection overhead)
		speedupVsNonIndexed := float64(nonIndexedDuration) / float64(intersectionDuration)
		t.Logf("Intersection query: %v, Single index query: %v, Non-indexed query: %v",
			intersectionDuration, singleIndexDuration, nonIndexedDuration)
		t.Logf("Intersection vs Non-indexed speedup: %.2fx", speedupVsNonIndexed)

		assert.GreaterOrEqual(t, speedupVsNonIndexed, IndexedSpeedupThreshold,
			"Index intersection should be at least %fx faster than non-indexed", IndexedSpeedupThreshold)
	})
}

// TestMemoryUsageForLargeDatasets measures memory usage during operations
func TestMemoryUsageForLargeDatasets(t *testing.T) {
	engine := NewStorageEngine()

	// Get initial memory stats
	initialStats := engine.GetMemoryStats()
	initialAlloc := int(initialStats["alloc_mb"].(uint64))

	// Create collection and insert large dataset
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert documents in batches to monitor memory
	batchSize := 1000
	for batch := 0; batch < LargeDatasetSize/batchSize; batch++ {
		for i := 0; i < batchSize; i++ {
			docID := batch*batchSize + i
			doc := domain.Document{
				"id":   fmt.Sprintf("%d", docID),
				"name": fmt.Sprintf("user%d", docID),
				"age":  docID % 100,
				"city": fmt.Sprintf("city%d", docID%50),
				"data": fmt.Sprintf("data_%d_%s", docID, generateRandomString(100)),
			}
			err := engine.Insert("users", doc)
			require.NoError(t, err)
		}

		// Check memory usage after each batch
		stats := engine.GetMemoryStats()
		currentAlloc := int(stats["alloc_mb"].(uint64))
		t.Logf("Batch %d: Memory usage: %d MB", batch+1, currentAlloc)

		// Memory should not exceed threshold
		assert.LessOrEqual(t, currentAlloc, MaxMemoryMB,
			"Memory usage should not exceed %d MB", MaxMemoryMB)
	}

	// Create indexes and measure memory impact
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("users", "city")
	require.NoError(t, err)

	// Final memory check
	finalStats := engine.GetMemoryStats()
	finalAlloc := int(finalStats["alloc_mb"].(uint64))

	t.Logf("Initial memory: %d MB, Final memory: %d MB, Increase: %d MB",
		initialAlloc, finalAlloc, finalAlloc-initialAlloc)

	// Memory usage should be reasonable
	assert.LessOrEqual(t, finalAlloc, MaxMemoryMB,
		"Final memory usage should not exceed %d MB", MaxMemoryMB)
}

// TestStreamingPerformance measures streaming throughput
func TestStreamingPerformance(t *testing.T) {
	engine := NewStorageEngine()

	// Create collection and insert large dataset
	err := engine.CreateCollection("users")
	require.NoError(t, err)

	// Insert large dataset
	for i := 0; i < LargeDatasetSize; i++ {
		doc := domain.Document{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 100,
			"city": fmt.Sprintf("city%d", i%50),
		}
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test streaming performance
	t.Run("StreamingThroughput", func(t *testing.T) {
		start := time.Now()
		docChan, err := engine.FindAllStream("users", nil)
		require.NoError(t, err)

		docCount := 0
		for range docChan {
			docCount++
		}
		duration := time.Since(start)

		// Calculate throughput
		throughput := float64(docCount) / duration.Seconds()
		t.Logf("Streamed %d documents in %v (%.0f docs/sec)", docCount, duration, throughput)

		// Performance assertion
		assert.GreaterOrEqual(t, throughput, float64(MinStreamingThroughput),
			"Streaming throughput should be at least %.0f docs/sec", float64(MinStreamingThroughput))
		assert.Equal(t, LargeDatasetSize, docCount, "Should stream all documents")
	})

	// Test streaming with filters
	t.Run("FilteredStreamingThroughput", func(t *testing.T) {
		start := time.Now()
		docChan, err := engine.FindAllStream("users", map[string]interface{}{"age": 25})
		require.NoError(t, err)

		docCount := 0
		for range docChan {
			docCount++
		}
		duration := time.Since(start)

		// Calculate throughput
		throughput := float64(docCount) / duration.Seconds()
		t.Logf("Filtered stream: %d documents in %v (%.0f docs/sec)", docCount, duration, throughput)

		// Performance assertion (filtered should still be fast)
		assert.GreaterOrEqual(t, throughput, float64(MinStreamingThroughput)/100,
			"Filtered streaming throughput should be at least %.0f docs/sec", float64(MinStreamingThroughput)/100)
		assert.Equal(t, 100, docCount, "Should stream 100 documents with age 25")
	})
}

// Benchmark functions for performance regression testing
func BenchmarkIndexedQueries(b *testing.B) {
	engine := NewStorageEngine()

	// Setup: create collection with large dataset and indexes
	err := engine.CreateCollection("users")
	require.NoError(b, err)

	// Insert dataset
	for i := 0; i < LargeDatasetSize; i++ {
		doc := domain.Document{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 100,
			"city": fmt.Sprintf("city%d", i%50),
		}
		err := engine.Insert("users", doc)
		require.NoError(b, err)
	}

	// Create indexes
	err = engine.CreateIndex("users", "age")
	require.NoError(b, err)
	err = engine.CreateIndex("users", "city")
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("SingleIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := engine.FindAll("users", map[string]interface{}{"age": 25}, nil)
			require.NoError(b, err)
		}
	})

	b.Run("MultiIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := engine.FindAll("users", map[string]interface{}{
				"age":  25,
				"city": "city10",
			}, nil)
			require.NoError(b, err)
		}
	})

	b.Run("NonIndexed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := engine.FindAll("users", map[string]interface{}{"name": "user25"}, nil)
			require.NoError(b, err)
		}
	})
}

func BenchmarkStreaming(b *testing.B) {
	engine := NewStorageEngine()

	// Setup: create collection with large dataset
	err := engine.CreateCollection("users")
	require.NoError(b, err)

	// Insert dataset
	for i := 0; i < LargeDatasetSize; i++ {
		doc := domain.Document{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 100,
		}
		err := engine.Insert("users", doc)
		require.NoError(b, err)
	}

	b.ResetTimer()

	b.Run("StreamAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			docChan, err := engine.FindAllStream("users", nil)
			require.NoError(b, err)
			for range docChan {
				// Just consume the stream
			}
		}
	})

	b.Run("StreamFiltered", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			docChan, err := engine.FindAllStream("users", map[string]interface{}{"age": 25})
			require.NoError(b, err)
			for range docChan {
				// Just consume the stream
			}
		}
	})
}

// Helper function to generate random strings for realistic data
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[time.Now().UnixNano()%int64(len(charset))]
	}
	return string(b)
}
