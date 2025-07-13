package storage

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/indexing"
)

// StorageEngine provides memory management with LRU caching and lazy loading
type StorageEngine struct {
	mu          sync.RWMutex
	cache       *LRUCache
	collections map[string]*CollectionInfo // Collection metadata (always in memory)
	indexEngine *indexing.IndexEngine
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
		indexEngine:    indexing.NewIndexEngine(),
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

	// Update indexes before inserting (oldDoc is nil for new documents)
	se.updateIndexes(collName, newID, nil, doc)

	collection.Documents[newID] = doc

	// Mark as dirty
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount++
		collectionInfo.LastModified = time.Now()
	}

	return nil
}

// FindAll returns documents that match the given filter criteria
// If filter is nil or empty, returns all documents
func (se *StorageEngine) FindAll(collName string, filter map[string]interface{}) ([]domain.Document, error) {
	docChan, err := se.docGenerator(collName, filter)
	if err != nil {
		return nil, err
	}
	var results []domain.Document
	for doc := range docChan {
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

	// Initialize indexes for this collection using the index engine
	se.indexEngine.CreateIndex(collName, "_id")

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

	// Create a copy of the old document for index updates
	oldDoc := make(domain.Document)
	for k, v := range doc {
		oldDoc[k] = v
	}

	// Apply updates to the document
	for key, value := range updates {
		if key != "_id" { // Prevent updating the document ID
			doc[key] = value
		}
	}

	// Update indexes with the change
	se.updateIndexes(collName, docId, oldDoc, doc)

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

	doc, exists := collection.Documents[docId]
	if !exists {
		return fmt.Errorf("document with id %s not found in collection %s", docId, collName)
	}

	// Update indexes before deleting (newDoc is nil for deletions)
	se.updateIndexes(collName, docId, doc, nil)

	delete(collection.Documents, docId)

	// Mark collection as dirty for persistence
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount--
		collectionInfo.LastModified = time.Now()
	}

	return nil
}

// CreateIndex creates an index on a specific field in a collection
func (se *StorageEngine) CreateIndex(collName, fieldName string) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return err
	}
	if err := se.indexEngine.CreateIndex(collName, fieldName); err != nil {
		return err
	}
	return se.indexEngine.BuildIndexForCollection(collName, fieldName, collection)
}

// DropIndex removes an index from a collection
func (se *StorageEngine) DropIndex(collName, fieldName string) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	return se.indexEngine.DropIndex(collName, fieldName)
}

// FindByIndex finds documents using an index
func (se *StorageEngine) FindByIndex(collName, fieldName string, value interface{}) ([]domain.Document, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}
	index, exists := se.indexEngine.GetIndex(collName, fieldName)
	if !exists {
		return nil, nil
	}
	ids := index.Query(value)
	var results []domain.Document
	for _, id := range ids {
		if doc, ok := collection.Documents[id]; ok {
			results = append(results, doc)
		}
	}
	return results, nil
}

// GetIndexes returns all index names for a collection
func (se *StorageEngine) GetIndexes(collName string) ([]string, error) {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.indexEngine.GetIndexes(collName)
}

// UpdateIndex rebuilds an index for a collection
func (se *StorageEngine) UpdateIndex(collName, fieldName string) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return err
	}
	return se.indexEngine.BuildIndexForCollection(collName, fieldName, collection)
}

// getIndex returns an index for a specific field in a collection
func (se *StorageEngine) getIndex(collName, fieldName string) (*indexing.Index, bool) {
	return se.indexEngine.GetIndex(collName, fieldName)
}

// updateIndexes updates all indexes for a collection when a document changes
func (se *StorageEngine) updateIndexes(collName, docID string, oldDoc, newDoc domain.Document) {
	se.indexEngine.UpdateIndexForDocument(collName, docID, oldDoc, newDoc)
}

// docGenerator yields matching documents for a given filter, using index optimization if possible.
func (se *StorageEngine) docGenerator(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	out := make(chan domain.Document, 100)

	se.mu.RLock()
	collection, err := se.getCollectionInternal(collName)
	se.mu.RUnlock()
	if err != nil {
		close(out)
		return nil, err
	}

	go func() {
		defer close(out)
		var candidateIDs []string
		var useIndex bool

		// Try to use index optimization if filter is present
		if len(filter) > 0 {
			for fieldName, expectedValue := range filter {
				if index, exists := se.getIndex(collName, fieldName); exists {
					candidateIDs = index.Query(expectedValue)
					useIndex = true
					break // Only use the first available index
				}
			}
		}

		if useIndex {
			for _, docID := range candidateIDs {
				if doc, exists := collection.Documents[docID]; exists {
					if MatchesFilter(doc, filter) {
						out <- doc
					}
				}
			}
		} else {
			for _, doc := range collection.Documents {
				if len(filter) == 0 || MatchesFilter(doc, filter) {
					out <- doc
				}
			}
		}
	}()
	return out, nil
}
