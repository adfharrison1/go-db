package storage

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/data"
)

// StorageEngine provides memory management with LRU caching and lazy loading
type StorageEngine struct {
	mu          sync.RWMutex
	cache       *LRUCache
	collections map[string]*CollectionInfo // Collection metadata (always in memory)
	indexes     map[string]*data.Collection
	metadata    map[string]interface{}

	// Configuration
	maxMemoryMB    int
	dataDir        string
	backgroundSave bool
	saveInterval   time.Duration

	// Background workers
	backgroundWg sync.WaitGroup
	stopChan     chan struct{}
}

// NewStorageEngine creates a new storage engine
func NewStorageEngine(options ...StorageOption) *StorageEngine {
	engine := &StorageEngine{
		collections:    make(map[string]*CollectionInfo),
		indexes:        make(map[string]*data.Collection),
		metadata:       make(map[string]interface{}),
		maxMemoryMB:    1024, // 1GB default
		dataDir:        ".",
		backgroundSave: false,
		saveInterval:   5 * time.Minute,
		stopChan:       make(chan struct{}),
	}

	// Apply options
	for _, option := range options {
		option(engine)
	}

	// Initialize cache with capacity based on max memory
	engine.cache = NewLRUCache(engine.maxMemoryMB / 100) // Rough estimate: 100MB per collection

	return engine
}

// GetCollection loads a collection on-demand (lazy loading)
func (se *StorageEngine) GetCollection(collName string) (*data.Collection, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.getCollectionInternal(collName)
}

// getCollectionInternal contains the actual collection loading logic without locking
func (se *StorageEngine) getCollectionInternal(collName string) (*data.Collection, error) {
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

// Insert inserts a document into a collection
func (se *StorageEngine) Insert(collName string, doc data.Document) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	// Get or load collection
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		// Collection doesn't exist, create it
		collection = data.NewCollection(collName)
		se.cache.Put(collName, collection, &CollectionInfo{
			Name:          collName,
			DocumentCount: 0,
			State:         CollectionStateDirty,
			LastModified:  time.Now(),
		})
	}

	// Generate unique ID
	newID := fmt.Sprintf("%d", len(collection.Documents)+1)
	doc["_id"] = newID
	collection.Documents[newID] = doc

	// Mark as dirty
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount++
		collectionInfo.LastModified = time.Now()
	}

	return nil
}

// FindAll returns all documents in a collection (for backward compatibility)
func (se *StorageEngine) FindAll(collName string) ([]data.Document, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	results := make([]data.Document, 0, len(collection.Documents))
	for _, doc := range collection.Documents {
		results = append(results, doc)
	}
	return results, nil
}

// GetMemoryStats returns current memory usage statistics
func (se *StorageEngine) GetMemoryStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"alloc_mb":       m.Alloc / 1024 / 1024,
		"total_alloc_mb": m.TotalAlloc / 1024 / 1024,
		"sys_mb":         m.Sys / 1024 / 1024,
		"num_goroutines": runtime.NumGoroutine(),
		"cache_size":     se.cache.list.Len(),
		"collections":    len(se.collections),
	}
}

// StartBackgroundWorkers starts background save workers
func (se *StorageEngine) StartBackgroundWorkers() {
	if !se.backgroundSave {
		return
	}

	se.backgroundWg.Add(1)
	go func() {
		defer se.backgroundWg.Done()
		ticker := time.NewTicker(se.saveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				se.saveDirtyCollections()
			case <-se.stopChan:
				return
			}
		}
	}()
}

// StopBackgroundWorkers stops background workers
func (se *StorageEngine) StopBackgroundWorkers() {
	select {
	case <-se.stopChan:
		// Channel already closed, do nothing
	default:
		close(se.stopChan)
	}
	se.backgroundWg.Wait()
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

	collection := data.NewCollection(collName)
	info := &CollectionInfo{
		Name:          collName,
		DocumentCount: 0,
		State:         CollectionStateLoaded,
		LastModified:  time.Now(),
	}

	se.collections[collName] = info
	se.cache.Put(collName, collection, info)

	return nil
}
