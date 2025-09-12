package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryMapManager(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-mmap-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	manager := NewMemoryMapManager(tempDir, ".test")

	t.Run("OpenCollection", func(t *testing.T) {
		// Test opening a new collection
		mf, err := manager.OpenCollection("test_collection", false)
		require.NoError(t, err)
		assert.NotNil(t, mf)
		assert.Equal(t, "test_collection", mf.CollectionName())
		assert.False(t, mf.IsReadOnly())
		assert.Equal(t, int64(4096), mf.Size()) // Minimum size

		// Test opening the same collection again (should return existing)
		mf2, err := manager.OpenCollection("test_collection", false)
		require.NoError(t, err)
		assert.Equal(t, mf, mf2) // Should be the same instance

		// Test opening as read-only
		mf3, err := manager.OpenCollection("test_collection", true)
		require.NoError(t, err)
		assert.Equal(t, mf, mf3) // Should be the same instance
	})

	t.Run("GetCollection", func(t *testing.T) {
		// Test getting existing collection
		mf, exists := manager.GetCollection("test_collection")
		assert.True(t, exists)
		assert.NotNil(t, mf)

		// Test getting non-existent collection
		mf2, exists := manager.GetCollection("nonexistent")
		assert.False(t, exists)
		assert.Nil(t, mf2)
	})

	t.Run("CloseCollection", func(t *testing.T) {
		// Test closing existing collection
		err := manager.CloseCollection("test_collection")
		require.NoError(t, err)

		// Test getting closed collection
		mf, exists := manager.GetCollection("test_collection")
		assert.False(t, exists)
		assert.Nil(t, mf)

		// Test closing non-existent collection (should not error)
		err = manager.CloseCollection("nonexistent")
		require.NoError(t, err)
	})

	t.Run("CloseAll", func(t *testing.T) {
		// Open multiple collections
		_, err := manager.OpenCollection("collection1", false)
		require.NoError(t, err)
		_, err = manager.OpenCollection("collection2", false)
		require.NoError(t, err)

		// Close all
		err = manager.CloseAll()
		require.NoError(t, err)

		// Verify all are closed
		_, exists1 := manager.GetCollection("collection1")
		_, exists2 := manager.GetCollection("collection2")
		assert.False(t, exists1)
		assert.False(t, exists2)
	})
}

func TestMemoryMappedFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-mmap-file-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	manager := NewMemoryMapManager(tempDir, ".test")

	t.Run("WriteAndRead", func(t *testing.T) {
		mf, err := manager.OpenCollection("test_file", false)
		require.NoError(t, err)
		defer manager.CloseCollection("test_file")

		// Test writing data
		testData := []byte("Hello, Memory-Mapped World!")
		err = mf.Write(0, testData)
		require.NoError(t, err)

		// Test reading data
		readData, err := mf.Read(0, len(testData))
		require.NoError(t, err)
		assert.Equal(t, testData, readData)

		// Test reading partial data
		partialData, err := mf.Read(7, 6) // "Memory"
		require.NoError(t, err)
		assert.Equal(t, []byte("Memory"), partialData)
	})

	t.Run("WriteAtOffset", func(t *testing.T) {
		mf, err := manager.OpenCollection("test_offset", false)
		require.NoError(t, err)
		defer manager.CloseCollection("test_offset")

		// Write at different offsets
		err = mf.Write(0, []byte("First"))
		require.NoError(t, err)
		err = mf.Write(10, []byte("Second"))
		require.NoError(t, err)
		err = mf.Write(20, []byte("Third"))
		require.NoError(t, err)

		// Read and verify
		first, err := mf.Read(0, 5)
		require.NoError(t, err)
		assert.Equal(t, []byte("First"), first)

		second, err := mf.Read(10, 6)
		require.NoError(t, err)
		assert.Equal(t, []byte("Second"), second)

		third, err := mf.Read(20, 5)
		require.NoError(t, err)
		assert.Equal(t, []byte("Third"), third)
	})

	t.Run("ReadOnlyFile", func(t *testing.T) {
		// First create a file with data
		mf, err := manager.OpenCollection("readonly_test", false)
		require.NoError(t, err)
		err = mf.Write(0, []byte("Read-only test data"))
		require.NoError(t, err)
		manager.CloseCollection("readonly_test")

		// Now open as read-only
		mf, err = manager.OpenCollection("readonly_test", true)
		require.NoError(t, err)
		defer manager.CloseCollection("readonly_test")

		assert.True(t, mf.IsReadOnly())

		// Test reading (should work)
		data, err := mf.Read(0, 19)
		require.NoError(t, err)
		assert.Equal(t, []byte("Read-only test data"), data)

		// Test writing (should fail)
		err = mf.Write(0, []byte("This should fail"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "read-only")
	})

	t.Run("Resize", func(t *testing.T) {
		mf, err := manager.OpenCollection("resize_test", false)
		require.NoError(t, err)
		defer manager.CloseCollection("resize_test")

		initialSize := mf.Size()
		assert.Equal(t, int64(4096), initialSize)

		// Resize to larger size
		newSize := int64(8192)
		err = mf.Resize(newSize)
		require.NoError(t, err)
		assert.Equal(t, newSize, mf.Size())

		// Test that we can write to the new space
		err = mf.Write(4096, []byte("Data in resized space"))
		require.NoError(t, err)

		// Read back the data
		data, err := mf.Read(4096, 21)
		require.NoError(t, err)
		assert.Equal(t, []byte("Data in resized space"), data)
	})

	t.Run("Sync", func(t *testing.T) {
		mf, err := manager.OpenCollection("sync_test", false)
		require.NoError(t, err)
		defer manager.CloseCollection("sync_test")

		// Write some data
		err = mf.Write(0, []byte("Sync test data"))
		require.NoError(t, err)

		// Sync to disk
		err = mf.Sync()
		require.NoError(t, err)

		// For read-only file, sync should not error
		manager.CloseCollection("sync_test")
		mf, err = manager.OpenCollection("sync_test", true)
		require.NoError(t, err)
		defer manager.CloseCollection("sync_test")

		err = mf.Sync()
		require.NoError(t, err)
	})

	t.Run("ErrorCases", func(t *testing.T) {
		mf, err := manager.OpenCollection("error_test", false)
		require.NoError(t, err)
		defer manager.CloseCollection("error_test")

		// Test reading out of bounds
		_, err = mf.Read(-1, 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")

		_, err = mf.Read(10000, 10)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")

		// Test writing out of bounds
		err = mf.Write(-1, []byte("test"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")

		err = mf.Write(10000, []byte("test"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")

		// Test writing beyond file size
		err = mf.Write(4090, []byte("This is too long"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exceed file size")
	})
}

func TestMemoryMapIntegration(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "go-db-mmap-integration-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Test that files are actually created on disk
	manager := NewMemoryMapManager(tempDir, ".godb")

	t.Run("FileCreation", func(t *testing.T) {
		mf, err := manager.OpenCollection("integration_test", false)
		require.NoError(t, err)
		defer manager.CloseCollection("integration_test")

		// Verify file exists on disk
		filePath := filepath.Join(tempDir, "integration_test.godb")
		_, err = os.Stat(filePath)
		require.NoError(t, err)

		// Write some data
		err = mf.Write(0, []byte("Integration test data"))
		require.NoError(t, err)

		// Sync to ensure data is written to disk
		err = mf.Sync()
		require.NoError(t, err)

		// Close and reopen to verify persistence
		manager.CloseCollection("integration_test")
		mf, err = manager.OpenCollection("integration_test", true)
		require.NoError(t, err)
		defer manager.CloseCollection("integration_test")

		// Read back the data
		data, err := mf.Read(0, 21)
		require.NoError(t, err)
		assert.Equal(t, []byte("Integration test data"), data)
	})
}
