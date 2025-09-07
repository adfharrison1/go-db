package storage

import (
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

	for docID, doc := range cachedCollection.Documents {
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
