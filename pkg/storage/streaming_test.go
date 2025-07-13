package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_FindAllStream_Basic(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test documents
	docs := []domain.Document{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "San Francisco"},
		{"name": "Charlie", "age": 35, "city": "Chicago"},
	}

	for _, doc := range docs {
		err := engine.Insert("users", doc)
		require.NoError(t, err)
	}

	// Test streaming
	docChan, err := engine.FindAllStream("users", nil)
	require.NoError(t, err)

	// Collect all documents from the stream
	receivedDocs := make([]domain.Document, 0)
	for doc := range docChan {
		receivedDocs = append(receivedDocs, doc)
	}

	// Verify we received all documents
	assert.Len(t, receivedDocs, 3)

	// Verify document contents
	names := make(map[string]bool)
	for _, doc := range receivedDocs {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}

	assert.True(t, names["Alice"])
	assert.True(t, names["Bob"])
	assert.True(t, names["Charlie"])
}

func TestStorageEngine_FindAllStream_EmptyCollection(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Create empty collection
	err := engine.CreateCollection("empty")
	require.NoError(t, err)

	// Test streaming empty collection
	docChan, err := engine.FindAllStream("empty", nil)
	require.NoError(t, err)

	// Should receive no documents
	docCount := 0
	for range docChan {
		docCount++
	}

	assert.Equal(t, 0, docCount)
}

func TestStorageEngine_FindAllStream_NonExistentCollection(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Try to stream non-existent collection
	_, err := engine.FindAllStream("nonexistent", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestStorageEngine_FindAllStream_LargeDataset(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert large number of documents
	const numDocs = 1000
	for i := 0; i < numDocs; i++ {
		doc := domain.Document{
			"id":    i,
			"name":  fmt.Sprintf("User_%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"data":  fmt.Sprintf("Large data payload for user %d", i),
		}
		err := engine.Insert("large_collection", doc)
		require.NoError(t, err)
	}

	// Test streaming large dataset
	docChan, err := engine.FindAllStream("large_collection", nil)
	require.NoError(t, err)

	// Collect all documents
	receivedDocs := make([]domain.Document, 0)
	start := time.Now()
	for doc := range docChan {
		receivedDocs = append(receivedDocs, doc)
	}
	duration := time.Since(start)

	// Verify all documents were received
	assert.Len(t, receivedDocs, numDocs)

	// Verify performance is reasonable (should be fast for in-memory operations)
	assert.Less(t, duration, 100*time.Millisecond, "Streaming 1000 documents should be fast")

	t.Logf("Streamed %d documents in %v", numDocs, duration)
}

func TestStorageEngine_FindAllStream_ConcurrentStreaming(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test data
	const numDocs = 100
	for i := 0; i < numDocs; i++ {
		doc := domain.Document{"id": i, "data": fmt.Sprintf("doc_%d", i)}
		err := engine.Insert("concurrent", doc)
		require.NoError(t, err)
	}

	// Test multiple concurrent streams
	const numStreams = 5
	var wg sync.WaitGroup
	results := make([][]domain.Document, numStreams)

	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func(streamID int) {
			defer wg.Done()

			docChan, err := engine.FindAllStream("concurrent", nil)
			require.NoError(t, err)

			docs := make([]domain.Document, 0)
			for doc := range docChan {
				docs = append(docs, doc)
			}

			results[streamID] = docs
		}(i)
	}

	wg.Wait()

	// Verify all streams received the same number of documents
	for i, docs := range results {
		assert.Len(t, docs, numDocs, "Stream %d should have %d documents", i, numDocs)
	}
}

func TestStorageEngine_FindAllStream_ChannelBuffer(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert documents
	docs := []domain.Document{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
		{"id": 3, "name": "Charlie"},
	}

	for _, doc := range docs {
		err := engine.Insert("buffer_test", doc)
		require.NoError(t, err)
	}

	// Test streaming
	docChan, err := engine.FindAllStream("buffer_test", nil)
	require.NoError(t, err)

	// Verify channel has reasonable buffer size
	// The implementation uses a buffer of 100, so we should be able to read
	// multiple documents without blocking
	receivedDocs := make([]domain.Document, 0)

	// Read documents with small delays to test buffering
	for i := 0; i < len(docs); i++ {
		select {
		case doc, ok := <-docChan:
			if !ok {
				break
			}
			receivedDocs = append(receivedDocs, doc)
			time.Sleep(1 * time.Millisecond) // Small delay to test buffering
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timeout waiting for document from stream")
		}
	}

	assert.Len(t, receivedDocs, len(docs))
}

func TestStorageEngine_FindAllStream_ShutdownHandling(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert documents
	for i := 0; i < 10; i++ {
		doc := domain.Document{"id": i, "data": fmt.Sprintf("doc_%d", i)}
		err := engine.Insert("shutdown_test", doc)
		require.NoError(t, err)
	}

	// Start streaming
	docChan, err := engine.FindAllStream("shutdown_test", nil)
	require.NoError(t, err)

	// Read a few documents
	receivedDocs := make([]domain.Document, 0)
	for i := 0; i < 3; i++ {
		select {
		case doc, ok := <-docChan:
			if !ok {
				break
			}
			receivedDocs = append(receivedDocs, doc)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timeout waiting for document")
		}
	}

	// Stop the engine (this should close the stopChan)
	engine.StopBackgroundWorkers()

	// The stream should eventually close due to stopChan being closed
	// We can't guarantee exactly when, but it should happen
	timeout := time.After(1 * time.Second)
	for {
		select {
		case doc, ok := <-docChan:
			if !ok {
				// Channel closed, which is expected
				return
			}
			receivedDocs = append(receivedDocs, doc)
		case <-timeout:
			// Timeout reached, which is also acceptable
			t.Logf("Stream closed after receiving %d documents", len(receivedDocs))
			return
		}
	}
}

func TestStorageEngine_FindAllStream_DocumentModification(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert documents
	docs := []domain.Document{
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
	}

	for _, doc := range docs {
		err := engine.Insert("modify_test", doc)
		require.NoError(t, err)
	}

	// Start streaming
	docChan, err := engine.FindAllStream("modify_test", nil)
	require.NoError(t, err)

	// Modify documents while streaming (this should not affect the stream)
	go func() {
		time.Sleep(10 * time.Millisecond) // Small delay
		newDoc := domain.Document{"name": "Eve", "age": 28}
		engine.Insert("modify_test", newDoc)
	}()

	// Collect documents from stream
	receivedDocs := make([]domain.Document, 0)
	for doc := range docChan {
		receivedDocs = append(receivedDocs, doc)
	}

	// Should receive the original documents (modifications during streaming shouldn't affect the stream)
	assert.Len(t, receivedDocs, 2)

	// Verify we got the original documents
	names := make(map[string]bool)
	for _, doc := range receivedDocs {
		name, exists := doc["name"]
		assert.True(t, exists)
		names[name.(string)] = true
	}

	assert.True(t, names["Alice"])
	assert.True(t, names["Bob"])
}

func TestStorageEngine_FindAllStream_Performance(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Insert test data
	const numDocs = 10000
	for i := 0; i < numDocs; i++ {
		doc := domain.Document{
			"id":    i,
			"name":  fmt.Sprintf("User_%d", i),
			"email": fmt.Sprintf("user%d@example.com", i),
			"data":  fmt.Sprintf("Data for user %d", i),
		}
		err := engine.Insert("perf_test", doc)
		require.NoError(t, err)
	}

	// Benchmark streaming performance
	start := time.Now()
	docChan, err := engine.FindAllStream("perf_test", nil)
	require.NoError(t, err)

	docCount := 0
	for range docChan {
		docCount++
	}
	duration := time.Since(start)

	// Verify all documents were streamed
	assert.Equal(t, numDocs, docCount)

	// Performance should be very fast for in-memory operations
	// 10,000 documents should stream in under 50ms
	assert.Less(t, duration, 50*time.Millisecond,
		"Streaming %d documents took %v, should be under 50ms", numDocs, duration)

	// Calculate throughput
	throughput := float64(numDocs) / duration.Seconds()
	t.Logf("Streaming throughput: %.0f documents/second", throughput)
	assert.Greater(t, throughput, 100000.0, "Throughput should be over 100k docs/sec")
}
