package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/pierrec/lz4/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// SaveToFile saves all collections to a single file (for backward compatibility)
func (se *StorageEngine) SaveToFile(filename string) error {
	se.mu.RLock()
	defer se.mu.RUnlock()
	storageData := NewStorageData()
	for collName, collection := range se.cache.cache {
		entry := collection.Value.(*cacheEntry)
		storageData.Collections[collName] = make(map[string]interface{})
		for docID, doc := range entry.value.Documents {
			storageData.Collections[collName][docID] = map[string]interface{}(doc)
		}
	}

	// Export indexes for persistence
	storageData.Indexes = se.indexEngine.ExportIndexes()
	msgpackData, err := msgpack.Marshal(storageData)
	if err != nil {
		return fmt.Errorf("failed to encode MessagePack: %w", err)
	}
	compressedData := make([]byte, lz4.CompressBlockBound(len(msgpackData)))
	var hashTable [1 << 16]int
	n, err := lz4.CompressBlock(msgpackData, compressedData, hashTable[:])
	if err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}
	compressedData = compressedData[:n]
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()
	if err := WriteHeader(file); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if _, err := file.Write(compressedData); err != nil {
		return fmt.Errorf("failed to write compressed data: %w", err)
	}
	return nil
}

// LoadCollectionMetadata loads only collection metadata from disk
func (se *StorageEngine) LoadCollectionMetadata(filename string) error {
	// Store the filename for later use in collection loading
	se.dataFile = filename
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	_, err = ReadHeader(file)
	if err != nil {
		return fmt.Errorf("invalid file header: %w", err)
	}
	compressedData, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read compressed data: %w", err)
	}
	decompressedData := make([]byte, len(compressedData)*10)
	n, err := lz4.UncompressBlock(compressedData, decompressedData)
	if err != nil {
		return fmt.Errorf("failed to decompress data: %w", err)
	}
	decompressedData = decompressedData[:n]
	var storageData StorageData
	if err := msgpack.Unmarshal(decompressedData, &storageData); err != nil {
		return fmt.Errorf("failed to decode MessagePack: %w", err)
	}
	se.mu.Lock()
	defer se.mu.Unlock()
	for collName := range storageData.Collections {
		se.collections[collName] = &CollectionInfo{
			Name:          collName,
			DocumentCount: int64(len(storageData.Collections[collName])),
			State:         CollectionStateUnloaded,
			LastModified:  time.Now(),
		}
	}

	// Import indexes if they exist
	if len(storageData.Indexes) > 0 {
		se.indexEngine.ImportIndexes(storageData.Indexes)
	}

	return nil
}

// loadCollectionFromSingleFile loads a collection from the single file format
func (se *StorageEngine) loadCollectionFromSingleFile(collName, filename string) (*domain.Collection, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = ReadHeader(file)
	if err != nil {
		return nil, err
	}

	compressedData, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	decompressedData := make([]byte, len(compressedData)*10)
	n, err := lz4.UncompressBlock(compressedData, decompressedData)
	if err != nil {
		return nil, err
	}
	decompressedData = decompressedData[:n]

	var storageData StorageData
	if err := msgpack.Unmarshal(decompressedData, &storageData); err != nil {
		return nil, err
	}

	docs, exists := storageData.Collections[collName]
	if !exists {
		return nil, fmt.Errorf("collection %s not found in file", collName)
	}

	collection := domain.NewCollection(collName)
	for docID, docData := range docs {
		doc := domain.Document{}
		for key, value := range docData.(map[string]interface{}) {
			doc[key] = value
		}
		collection.Documents[docID] = doc
	}

	// Rebuild indexes for this collection after loading
	se.indexEngine.RebuildIndexForCollection(collName, collection)

	return collection, nil
}

// loadCollectionFromDisk loads a single collection from disk
func (se *StorageEngine) loadCollectionFromDisk(collName string) (*domain.Collection, error) {
	filename := fmt.Sprintf("%s/collections/%s.godb", se.dataDir, collName)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	_, err = ReadHeader(file)
	if err != nil {
		return nil, err
	}
	compressedData, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	decompressedData := make([]byte, len(compressedData)*10)
	n, err := lz4.UncompressBlock(compressedData, decompressedData)
	if err != nil {
		return nil, err
	}
	decompressedData = decompressedData[:n]
	var storageData StorageData
	if err := msgpack.Unmarshal(decompressedData, &storageData); err != nil {
		return nil, err
	}
	docs, exists := storageData.Collections[collName]
	if !exists {
		return nil, fmt.Errorf("collection %s not found in file", collName)
	}
	collection := domain.NewCollection(collName)

	// Track the highest numeric ID to restore the counter properly
	maxID := int64(0)

	for docID, docData := range docs {
		if doc, ok := docData.(map[string]interface{}); ok {
			collection.Documents[docID] = domain.Document(doc)

			// Try to parse the document ID as a number to find the highest one
			if id, err := strconv.ParseInt(docID, 10, 64); err == nil {
				if id > maxID {
					maxID = id
				}
			}
		}
	}

	// Restore the ID counter for this collection to the highest existing ID
	// This ensures new documents get unique IDs that don't conflict with existing ones
	se.idCountersMu.Lock()
	se.idCounters[collName] = &maxID
	se.idCountersMu.Unlock()

	log.Printf("INFO: Loaded collection '%s' with %d documents, restored ID counter to %d",
		collName, len(collection.Documents), maxID)

	return collection, nil
}

// saveDirtyCollections saves all dirty collections to individual files
func (se *StorageEngine) saveDirtyCollections() {
	start := time.Now()
	savedCount := 0
	errorCount := 0

	// Get list of dirty collections (read-only operation)
	se.mu.RLock()
	var dirtyCollections []string
	for collName, info := range se.collections {
		if info.State == CollectionStateDirty {
			dirtyCollections = append(dirtyCollections, collName)
		}
	}
	se.mu.RUnlock()

	if len(dirtyCollections) == 0 {
		log.Printf("DEBUG: No dirty collections to save")
		return
	}

	log.Printf("INFO: Background save starting - %d dirty collections to save", len(dirtyCollections))

	// Ensure collections directory exists
	collectionsDir := filepath.Join(se.dataDir, "collections")
	if err := os.MkdirAll(collectionsDir, 0755); err != nil {
		log.Printf("ERROR: Failed to create collections directory: %v", err)
		return
	}

	// Save each dirty collection
	for _, collName := range dirtyCollections {
		if err := se.saveCollectionToFile(collName); err != nil {
			log.Printf("ERROR: Failed to save collection %s: %v", collName, err)
			errorCount++
		} else {
			savedCount++
		}
	}

	elapsed := time.Since(start)
	if errorCount > 0 {
		log.Printf("WARN: Background save completed with errors - saved: %d, errors: %d, time: %v",
			savedCount, errorCount, elapsed)
	} else {
		log.Printf("INFO: Background save completed successfully - saved: %d collections in %v",
			savedCount, elapsed)
	}
}

// saveCollectionToFile saves a single collection to its individual file
func (se *StorageEngine) saveCollectionToFile(collName string) error {
	// Use write lock for this collection to prevent modifications during save
	return se.withCollectionWriteLock(collName, func() error {
		return se.saveCollectionToFileUnsafe(collName)
	})
}

// saveCollectionToFileUnsafe saves a collection without acquiring locks (caller must hold collection write lock)
func (se *StorageEngine) saveCollectionToFileUnsafe(collName string) error {
	// Mark collection as being saved
	lock := se.getOrCreateCollectionLock(collName)
	lock.saving = true
	defer func() { lock.saving = false }()

	// Get collection from cache (already holding collection write lock)
	cachedCollection, collectionInfo, found := se.cache.Get(collName)
	if !found {
		return fmt.Errorf("collection %s not found in cache", collName)
	}

	// Check if still dirty (might have been saved by another goroutine)
	if collectionInfo.State != CollectionStateDirty {
		return nil // Already saved, skip
	}

	// Prepare storage data
	storageData := NewStorageData()
	storageData.Collections[collName] = make(map[string]interface{})

	// Take a safe snapshot of the documents map
	// The collection write lock we're already holding should protect against structural changes
	// but we need to be careful about concurrent map access from document operations
	documentsCopy := make(map[string]domain.Document)

	// Since we're holding a collection write lock, no new documents should be added/removed
	// But individual documents might still be modified - we'll take a safe snapshot
	for docID, doc := range cachedCollection.Documents {
		// Create a deep copy of each document to avoid races on document content
		docCopy := make(domain.Document)
		for k, v := range doc {
			docCopy[k] = v
		}
		documentsCopy[docID] = docCopy
	}

	for docID, doc := range documentsCopy {
		storageData.Collections[collName][docID] = map[string]interface{}(doc)
	}

	// Serialize and compress
	msgpackData, err := msgpack.Marshal(storageData)
	if err != nil {
		return fmt.Errorf("failed to encode MessagePack: %w", err)
	}

	compressedData := make([]byte, lz4.CompressBlockBound(len(msgpackData)))
	var hashTable [1 << 16]int
	n, err := lz4.CompressBlock(msgpackData, compressedData, hashTable[:])
	if err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}
	compressedData = compressedData[:n]

	// Ensure collections directory exists
	collectionsDir := filepath.Join(se.dataDir, "collections")
	if err := os.MkdirAll(collectionsDir, 0755); err != nil {
		return fmt.Errorf("failed to create collections directory: %w", err)
	}

	// Write to file
	filename := filepath.Join(collectionsDir, collName+".godb")
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if err := WriteHeader(file); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if _, err := file.Write(compressedData); err != nil {
		return fmt.Errorf("failed to write compressed data: %w", err)
	}

	// Update collection state to clean (already holding collection write lock)
	if info, exists := se.collections[collName]; exists {
		info.State = CollectionStateLoaded // Mark as clean
		info.SizeOnDisk = int64(len(compressedData))
	}

	log.Printf("DEBUG: Saved collection %s (%d bytes compressed)", collName, len(compressedData))
	return nil
}

// saveDocumentToDisk saves a single document to disk immediately
func (se *StorageEngine) saveDocumentToDisk(collection, docID string, doc domain.Document) error {
	// Ensure collection directory exists
	collectionsDir := filepath.Join(se.dataDir, "collections")
	if err := os.MkdirAll(collectionsDir, 0755); err != nil {
		return fmt.Errorf("failed to create collections directory: %w", err)
	}

	// Get the collection to check if it exists
	se.mu.RLock()
	_, exists := se.collections[collection]
	if !exists {
		se.mu.RUnlock()
		return fmt.Errorf("collection %s does not exist", collection)
	}
	se.mu.RUnlock()

	// Load existing collection data from disk
	collectionFile := filepath.Join(collectionsDir, collection+".godb")
	existingData := make(map[string]interface{})

	if _, err := os.Stat(collectionFile); err == nil {
		// File exists, load it
		if err := se.loadCollectionFromFile(collectionFile, existingData); err != nil {
			// If we can't load existing data, start fresh (this is normal during concurrent operations)
			log.Printf("DEBUG: Could not load existing collection data for %s: %v", collection, err)
		}
	}

	// Add/update the document in the existing data
	existingData[docID] = map[string]interface{}(doc)

	// Create storage data structure
	storageData := NewStorageData()
	storageData.Collections[collection] = existingData

	// collectionFile is already defined above

	// Serialize and compress
	data, err := msgpack.Marshal(storageData)
	if err != nil {
		return fmt.Errorf("failed to marshal collection data: %w", err)
	}

	// Compress with LZ4
	compressedData := make([]byte, lz4.CompressBlockBound(len(data)))
	n, err := lz4.CompressBlock(data, compressedData, nil)
	if err != nil {
		return fmt.Errorf("failed to compress collection data: %w", err)
	}
	compressedData = compressedData[:n]

	// Create file with proper GODB header
	var buf bytes.Buffer
	header := &FileHeader{
		Magic:   [4]byte{'G', 'O', 'D', 'B'},
		Version: FormatVersion,
		Flags:   0,
	}

	// Write header
	if err := binary.Write(&buf, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write file header: %w", err)
	}

	// Write compressed data
	if _, err := buf.Write(compressedData); err != nil {
		return fmt.Errorf("failed to write compressed data: %w", err)
	}

	// Write to temporary file first, then rename (atomic operation)
	tempFile := collectionFile + ".tmp"
	if err := os.WriteFile(tempFile, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write collection file: %w", err)
	}

	if err := os.Rename(tempFile, collectionFile); err != nil {
		os.Remove(tempFile) // Clean up temp file
		return fmt.Errorf("failed to rename collection file: %w", err)
	}

	// Update collection metadata
	se.mu.Lock()
	if info, exists := se.collections[collection]; exists {
		info.State = CollectionStateLoaded
		info.SizeOnDisk = int64(len(compressedData))
	}
	se.mu.Unlock()

	return nil
}

// loadCollectionFromFile loads collection data from a file
func (se *StorageEngine) loadCollectionFromFile(filename string, target map[string]interface{}) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	// Check if file is empty
	if len(data) == 0 {
		return nil // Empty file, nothing to load
	}

	// Read and validate header
	reader := bytes.NewReader(data)
	_, err = ReadHeader(reader)
	if err != nil {
		// If header reading fails, try to read as old format (just compressed data)
		// This handles backward compatibility
		return se.loadCollectionFromFileLegacy(data, target)
	}

	// Read compressed data after header
	compressedData := make([]byte, reader.Len())
	if _, err := reader.Read(compressedData); err != nil {
		return fmt.Errorf("failed to read compressed data: %w", err)
	}

	// Check if compressed data is too small for LZ4
	if len(compressedData) < 4 {
		return fmt.Errorf("compressed data too small for LZ4 decompression")
	}

	// Decompress
	decompressedData := make([]byte, len(compressedData)*4) // Start with 4x size
	n, err := lz4.UncompressBlock(compressedData, decompressedData)
	if err != nil {
		return fmt.Errorf("failed to decompress collection data: %w", err)
	}
	decompressedData = decompressedData[:n]

	// Unmarshal
	var storageData StorageData
	if err := msgpack.Unmarshal(decompressedData, &storageData); err != nil {
		return fmt.Errorf("failed to unmarshal collection data: %w", err)
	}

	// Extract collection data
	for _, collData := range storageData.Collections {
		for docID, docData := range collData {
			target[docID] = docData
		}
	}

	return nil
}

// loadCollectionFromFileLegacy loads collection data from old format files (without header)
func (se *StorageEngine) loadCollectionFromFileLegacy(data []byte, target map[string]interface{}) error {
	// Check if data is too small for LZ4
	if len(data) < 4 {
		return fmt.Errorf("data too small for LZ4 decompression")
	}

	// Decompress
	decompressedData := make([]byte, len(data)*4) // Start with 4x size
	n, err := lz4.UncompressBlock(data, decompressedData)
	if err != nil {
		return fmt.Errorf("failed to decompress collection data: %w", err)
	}
	decompressedData = decompressedData[:n]

	// Unmarshal
	var storageData StorageData
	if err := msgpack.Unmarshal(decompressedData, &storageData); err != nil {
		return fmt.Errorf("failed to unmarshal collection data: %w", err)
	}

	// Extract collection data
	for _, collData := range storageData.Collections {
		for docID, docData := range collData {
			target[docID] = docData
		}
	}

	return nil
}
