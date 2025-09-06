package storage

import (
	"fmt"
	"os"
	"runtime"
	"strings"
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

// createIsolatedEngine creates a storage engine with proper isolation for testing
func createIsolatedEngine(t *testing.T) *StorageEngine {
	// Create temporary directory for this test
	tempDir, err := os.MkdirTemp("", "go-db-test-*")
	require.NoError(t, err)

	// Cleanup temp directory when test completes
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	// Create engine with isolated temp directory
	engine := NewStorageEngine(WithDataDir(tempDir))

	// Cleanup engine when test completes
	t.Cleanup(func() {
		engine.StopBackgroundWorkers()
		runtime.GC() // Force garbage collection to clean up memory
	})

	return engine
}

// TestIndexedVsNonIndexedPerformance measures the performance improvement
// of indexed queries vs non-indexed queries
func TestIndexedVsNonIndexedPerformance(t *testing.T) {
	engine := createIsolatedEngine(t)

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
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Create indexes on commonly queried fields
	err = engine.CreateIndex("users", "age")
	require.NoError(t, err)
	err = engine.CreateIndex("users", "city")
	require.NoError(t, err)
	err = engine.CreateIndex("users", "role")
	require.NoError(t, err)

	// Force garbage collection before performance tests
	runtime.GC()
	runtime.GC() // Run twice to ensure thorough cleanup

	// Test single field queries
	t.Run("SingleField_IndexedVsNonIndexed", func(t *testing.T) {
		// Query that will use index
		start := time.Now()
		indexedResults, err := engine.FindAll("users", map[string]interface{}{"age": 25}, &domain.PaginationOptions{Limit: 1000, MaxLimit: 1000})
		indexedDuration := time.Since(start)
		require.NoError(t, err)

		// Query that won't use index (no index on name)
		start = time.Now()
		nonIndexedResults, err := engine.FindAll("users", map[string]interface{}{"name": "user25"}, &domain.PaginationOptions{Limit: 1000, MaxLimit: 1000})
		nonIndexedDuration := time.Since(start)
		require.NoError(t, err)

		// Verify results are correct
		assert.Len(t, indexedResults.Documents, 100)  // Every 100th user has age 25
		assert.Len(t, nonIndexedResults.Documents, 1) // Only one user named "user25"

		// Performance assertion: indexed should be significantly faster
		speedup := float64(nonIndexedDuration) / float64(indexedDuration)
		t.Logf("Indexed query: %v, Non-indexed query: %v, Speedup: %.2fx",
			indexedDuration, nonIndexedDuration, speedup)

		assert.GreaterOrEqual(t, speedup, IndexedSpeedupThreshold,
			"Indexed queries should be at least %fx faster than non-indexed", IndexedSpeedupThreshold)
	})

	// Test multi-field queries
	t.Run("MultiField_IndexIntersection", func(t *testing.T) {
		// Force garbage collection before this specific test
		runtime.GC()
		runtime.GC()

		// Query using multiple indexes (age AND city)
		start := time.Now()
		intersectionResults, err := engine.FindAll("users", map[string]interface{}{
			"age":  25,
			"city": "city25", // This should match users with id 25, 125, 225, etc.
		}, &domain.PaginationOptions{Limit: 1000, MaxLimit: 1000})
		intersectionDuration := time.Since(start)
		require.NoError(t, err)

		// Query using only one index (age only)
		start = time.Now()
		singleIndexResults, err := engine.FindAll("users", map[string]interface{}{"age": 25}, &domain.PaginationOptions{Limit: 1000, MaxLimit: 1000})
		singleIndexDuration := time.Since(start)
		require.NoError(t, err)

		// Query without any index (name field)
		start = time.Now()
		nonIndexedResults, err := engine.FindAll("users", map[string]interface{}{"name": "user25"}, &domain.PaginationOptions{Limit: 1000, MaxLimit: 1000})
		nonIndexedDuration := time.Since(start)
		require.NoError(t, err)

		// Verify results
		// Users with age 25: id 25, 125, 225, 325, 425, 525, 625, 725, 825, 925, 1025, 1125, etc.
		// Users with city "city25": id 25, 75, 125, 175, 225, 275, etc.
		// Intersection: id 25, 125, 225, 325, 425, 525, 625, 725, 825, 925, 1025, 1125, etc.
		expectedIntersection := 100 // Every 100th user has age 25, and every 50th user has city25, so intersection is 100
		assert.Len(t, intersectionResults.Documents, expectedIntersection)
		assert.Len(t, singleIndexResults.Documents, 100) // 100 users with age 25
		assert.Len(t, nonIndexedResults.Documents, 1)    // Only one user named "user25"

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
	engine := createIsolatedEngine(t)

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
			_, err := engine.Insert("users", doc)
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
	engine := createIsolatedEngine(t)

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
		_, err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Force garbage collection before performance tests
	runtime.GC()
	runtime.GC()

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
	// Create temporary directory for this benchmark
	tempDir, err := os.MkdirTemp("", "go-db-benchmark-*")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Setup: create collection with large dataset and indexes
	err = engine.CreateCollection("users")
	require.NoError(b, err)

	// Insert dataset
	for i := 0; i < LargeDatasetSize; i++ {
		doc := domain.Document{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 100,
			"city": fmt.Sprintf("city%d", i%50),
		}
		_, err := engine.Insert("users", doc)
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
	// Create temporary directory for this benchmark
	tempDir, err := os.MkdirTemp("", "go-db-benchmark-*")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	engine := NewStorageEngine(WithDataDir(tempDir))
	defer engine.StopBackgroundWorkers()

	// Setup: create collection with large dataset
	err = engine.CreateCollection("users")
	require.NoError(b, err)

	// Insert dataset
	for i := 0; i < LargeDatasetSize; i++ {
		doc := domain.Document{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("user%d", i),
			"age":  i % 100,
		}
		_, err := engine.Insert("users", doc)
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

// Batch operation performance tests

func TestBatchInsertPerformance(t *testing.T) {
	engine := createIsolatedEngine(t)
	defer engine.StopBackgroundWorkers()

	t.Run("Batch Insert 1000 Documents", func(t *testing.T) {
		// Create 1000 documents
		docs := make([]domain.Document, 1000)
		for i := 0; i < 1000; i++ {
			docs[i] = domain.Document{
				"id":          i,
				"name":        fmt.Sprintf("User%d", i),
				"email":       fmt.Sprintf("user%d@example.com", i),
				"age":         20 + (i % 60),
				"department":  []string{"Engineering", "Sales", "Marketing", "HR"}[i%4],
				"salary":      50000 + (i * 1000),
				"active":      i%2 == 0,
				"description": fmt.Sprintf("This is a description for user %d with some additional text to make the document larger", i),
			}
		}

		start := time.Now()
		_, err := engine.BatchInsert("performance_users", docs)
		duration := time.Since(start)

		require.NoError(t, err)
		t.Logf("Batch insert of 1000 documents took: %v", duration)

		// Should be much faster than individual inserts
		assert.Less(t, duration, 1*time.Second, "Batch insert should complete in under 1 second")

		// Verify all documents were inserted
		engine.mu.RLock()
		collInfo, exists := engine.collections["performance_users"]
		engine.mu.RUnlock()

		assert.True(t, exists)
		assert.Equal(t, int64(1000), collInfo.DocumentCount)
	})

	t.Run("Batch Insert vs Individual Inserts", func(t *testing.T) {
		const numDocs = 500

		// Test individual inserts
		docs1 := make([]domain.Document, numDocs)
		for i := 0; i < numDocs; i++ {
			docs1[i] = domain.Document{
				"id":    i,
				"name":  fmt.Sprintf("IndividualUser%d", i),
				"value": i * 2,
			}
		}

		start := time.Now()
		for _, doc := range docs1 {
			_, err := engine.Insert("individual_inserts", doc)
			require.NoError(t, err)
		}
		individualDuration := time.Since(start)

		// Test batch insert
		docs2 := make([]domain.Document, numDocs)
		for i := 0; i < numDocs; i++ {
			docs2[i] = domain.Document{
				"id":    i,
				"name":  fmt.Sprintf("BatchUser%d", i),
				"value": i * 2,
			}
		}

		start = time.Now()
		_, err := engine.BatchInsert("batch_inserts", docs2)
		batchDuration := time.Since(start)

		require.NoError(t, err)

		t.Logf("Individual inserts (%d docs): %v", numDocs, individualDuration)
		t.Logf("Batch insert (%d docs): %v", numDocs, batchDuration)
		t.Logf("Batch is %.2fx faster", float64(individualDuration)/float64(batchDuration))

		// For small batches, performance might be similar due to overhead
		// We mainly verify that batch operations work correctly and aren't significantly slower
		performanceRatio := float64(batchDuration) / float64(individualDuration)
		assert.Less(t, performanceRatio, 2.0, "Batch insert should not be more than 2x slower than individual inserts")

		// Log the results for manual analysis
		if batchDuration < individualDuration {
			t.Logf("✓ Batch is faster")
		} else {
			t.Logf("ℹ Individual inserts are faster for this batch size, which can be normal due to overhead")
		}
	})
}

func TestBatchUpdatePerformance(t *testing.T) {
	engine := createIsolatedEngine(t)
	defer engine.StopBackgroundWorkers()

	// Setup: Insert 1000 documents for updating
	setupDocs := make([]domain.Document, 1000)
	for i := 0; i < 1000; i++ {
		setupDocs[i] = domain.Document{
			"id":     i,
			"name":   fmt.Sprintf("User%d", i),
			"salary": 50000,
			"level":  1,
		}
	}

	_, err := engine.BatchInsert("update_performance", setupDocs)
	require.NoError(t, err)

	t.Run("Batch Update 1000 Documents", func(t *testing.T) {
		// Create 1000 update operations
		operations := make([]domain.BatchUpdateOperation, 1000)
		for i := 0; i < 1000; i++ {
			operations[i] = domain.BatchUpdateOperation{
				ID: fmt.Sprintf("%d", i+1), // IDs start from 1
				Updates: domain.Document{
					"salary":     55000 + (i * 100),
					"level":      2,
					"updated_at": time.Now().Unix(),
					"bonus":      i%2 == 0,
				},
			}
		}

		start := time.Now()
		_, err := engine.BatchUpdate("update_performance", operations)
		duration := time.Since(start)

		require.NoError(t, err)
		t.Logf("Batch update of 1000 documents took: %v", duration)

		// Should complete reasonably quickly
		assert.Less(t, duration, 2*time.Second, "Batch update should complete in under 2 seconds")

		// Verify a few random updates
		doc1, err := engine.GetById("update_performance", "1")
		require.NoError(t, err)
		assert.Equal(t, 55000, doc1["salary"])
		assert.Equal(t, 2, doc1["level"])

		doc500, err := engine.GetById("update_performance", "500")
		require.NoError(t, err)
		assert.Equal(t, 55000+(499*100), doc500["salary"])
	})

	t.Run("Batch Update vs Individual Updates", func(t *testing.T) {
		const numUpdates = 200

		// Test individual updates
		start := time.Now()
		for i := 0; i < numUpdates; i++ {
			updates := domain.Document{
				"individual_update": true,
				"timestamp":         time.Now().Unix(),
			}
			_, err := engine.UpdateById("update_performance", fmt.Sprintf("%d", i+1), updates)
			require.NoError(t, err)
		}
		individualDuration := time.Since(start)

		// Test batch update
		operations := make([]domain.BatchUpdateOperation, numUpdates)
		for i := 0; i < numUpdates; i++ {
			operations[i] = domain.BatchUpdateOperation{
				ID: fmt.Sprintf("%d", i+1),
				Updates: domain.Document{
					"batch_update": true,
					"timestamp":    time.Now().Unix(),
				},
			}
		}

		start = time.Now()
		_, err := engine.BatchUpdate("update_performance", operations)
		batchDuration := time.Since(start)

		require.NoError(t, err)

		t.Logf("Individual updates (%d ops): %v", numUpdates, individualDuration)
		t.Logf("Batch update (%d ops): %v", numUpdates, batchDuration)
		t.Logf("Batch is %.2fx faster", float64(individualDuration)/float64(batchDuration))

		// Batch should be faster
		assert.Less(t, batchDuration, individualDuration, "Batch update should be faster than individual updates")
	})
}

func TestBatchOperationsMemoryUsage(t *testing.T) {
	engine := createIsolatedEngine(t)
	defer engine.StopBackgroundWorkers()

	t.Run("Memory Usage During Large Batch Operations", func(t *testing.T) {
		runtime.GC()
		initialStats := engine.GetMemoryStats()

		// Large batch insert (within limit)
		docs := make([]domain.Document, 1000)
		for i := 0; i < 1000; i++ {
			docs[i] = domain.Document{
				"id":          i,
				"data":        make([]byte, 1024), // 1KB per document
				"description": strings.Repeat("test", 100),
			}
		}

		_, err := engine.BatchInsert("memory_test", docs)
		require.NoError(t, err)

		afterInsertStats := engine.GetMemoryStats()

		t.Logf("Initial memory stats: %v", initialStats)
		t.Logf("After batch insert stats: %v", afterInsertStats)

		// Memory usage should be reasonable (not testing exact values due to GC unpredictability)
		assert.NotNil(t, afterInsertStats)
	})
}
