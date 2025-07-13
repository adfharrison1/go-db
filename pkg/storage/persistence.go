package storage

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/adfharrison1/go-db/pkg/data"
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
	return nil
}

// loadCollectionFromDisk loads a single collection from disk
func (se *StorageEngine) loadCollectionFromDisk(collName string) (*data.Collection, error) {
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
	collection := data.NewCollection(collName)
	for docID, docData := range docs {
		if doc, ok := docData.(map[string]interface{}); ok {
			collection.Documents[docID] = data.Document(doc)
		}
	}
	return collection, nil
}

// saveDirtyCollections saves all dirty collections to disk
func (se *StorageEngine) saveDirtyCollections() {
	// Implementation for saving dirty collections
}
