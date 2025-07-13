package storage

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// StorageEngine provides memory management with LRU caching and lazy loading
type StorageEngine struct {
	mu          sync.RWMutex
	cache       *LRUCache
	collections map[string]*CollectionInfo // Collection metadata (always in memory)
	indexes     map[string]*domain.Collection
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
		indexes:        make(map[string]*domain.Collection),
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

// Insert inserts a document into a collection
func (se *StorageEngine) Insert(collName string, doc domain.Document) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	// Get or load collection
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		// Collection doesn't exist, create it
		collection = domain.NewCollection(collName)
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
func (se *StorageEngine) FindAll(collName string) ([]domain.Document, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	results := make([]domain.Document, 0, len(collection.Documents))
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

	collection := domain.NewCollection(collName)
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

// GetById retrieves a specific document by its ID
func (se *StorageEngine) GetById(collName, docId string) (domain.Document, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	doc, exists := collection.Documents[docId]
	if !exists {
		return nil, fmt.Errorf("document with id %s not found in collection %s", docId, collName)
	}

	return doc, nil
}

// UpdateById updates a specific document by its ID
func (se *StorageEngine) UpdateById(collName, docId string, updates domain.Document) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return err
	}

	doc, exists := collection.Documents[docId]
	if !exists {
		return fmt.Errorf("document with id %s not found in collection %s", docId, collName)
	}

	// Apply updates to the document
	for key, value := range updates {
		if key != "_id" { // Prevent updating the document ID
			doc[key] = value
		}
	}

	// Mark collection as dirty for persistence
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.LastModified = time.Now()
	}

	return nil
}

// DeleteById removes a specific document by its ID
func (se *StorageEngine) DeleteById(collName, docId string) error {
	se.mu.Lock()
	defer se.mu.Unlock()

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return err
	}

	if _, exists := collection.Documents[docId]; !exists {
		return fmt.Errorf("document with id %s not found in collection %s", docId, collName)
	}

	delete(collection.Documents, docId)

	// Mark collection as dirty for persistence
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount--
		collectionInfo.LastModified = time.Now()
	}

	return nil
}

// FindAllWithFilter returns documents that match the given filter criteria
func (se *StorageEngine) FindAllWithFilter(collName string, filter map[string]interface{}) ([]domain.Document, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	var results []domain.Document
	for _, doc := range collection.Documents {
		if matchesFilter(doc, filter) {
			results = append(results, doc)
		}
	}

	return results, nil
}

// matchesFilter checks if a document matches the given filter criteria
func matchesFilter(doc domain.Document, filter map[string]interface{}) bool {
	for field, expectedValue := range filter {
		actualValue, exists := doc[field]
		if !exists {
			return false // Field doesn't exist in document
		}

		if !valuesMatch(actualValue, expectedValue) {
			return false // Values don't match
		}
	}
	return true // All filter criteria match
}

// valuesMatch compares two values for equality, handling different types
func valuesMatch(actual, expected interface{}) bool {
	// Handle nil values
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	// Handle string comparison (case-insensitive for better UX)
	if actualStr, ok1 := actual.(string); ok1 {
		if expectedStr, ok2 := expected.(string); ok2 {
			return strings.EqualFold(actualStr, expectedStr)
		}
	}

	// Handle numeric comparison
	if actualNum, ok1 := toFloat64(actual); ok1 {
		if expectedNum, ok2 := toFloat64(expected); ok2 {
			return actualNum == expectedNum
		}
	}

	// Default to direct comparison
	return actual == expected
}

// toFloat64 converts various numeric types to float64 for comparison
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}
