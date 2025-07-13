package storage

import (
	"fmt"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// GetCollection loads a collection on-demand (lazy loading)
func (se *StorageEngine) GetCollection(collName string) (*domain.Collection, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.getCollectionInternal(collName)
}

// getCollectionInternal contains the actual collection loading logic without locking
func (se *StorageEngine) getCollectionInternal(collName string) (*domain.Collection, error) {
	// First check cache
	if collection, _, found := se.cache.Get(collName); found {
		return collection, nil
	}

	// Check if collection exists in metadata
	collectionInfo, exists := se.collections[collName]

	if !exists {
		return nil, fmt.Errorf("collection %s does not exist", collName)
	}

	// Load collection from disk
	collection, err := se.loadCollectionFromDisk(collName)
	if err != nil {
		return nil, fmt.Errorf("failed to load collection %s: %w", collName, err)
	}

	// Add to cache
	collectionInfo.State = CollectionStateLoaded
	collectionInfo.LastAccessed = time.Now()
	se.cache.Put(collName, collection, collectionInfo)

	return collection, nil
}

// CreateCollection creates a new collection
func (se *StorageEngine) CreateCollection(collName string) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	if collName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}

	if _, exists := se.collections[collName]; exists {
		return fmt.Errorf("collection %s already exists", collName)
	}

	collection := domain.NewCollection(collName)
	info := &CollectionInfo{
		Name:          collName,
		DocumentCount: 0,
		State:         CollectionStateLoaded,
		LastModified:  time.Now(),
	}

	se.collections[collName] = info
	se.cache.Put(collName, collection, info)

	// Initialize indexes for this collection using the index engine
	se.indexEngine.CreateIndex(collName, "_id")

	return nil
}
