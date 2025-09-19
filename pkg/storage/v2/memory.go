package v2

import (
	"fmt"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// NewMemoryManager creates a new memory manager
func NewMemoryManager(engine *StorageEngine) *MemoryManager {
	return &MemoryManager{
		engine:      engine,
		maxMemoryMB: engine.maxMemoryMB,
		cache:       NewLRUCache(engine.maxMemoryMB / 100), // 100MB per collection estimate
		collections: make(map[string]*Collection),
	}
}

// InsertDocument inserts a document into memory
func (mm *MemoryManager) InsertDocument(collName string, doc domain.Document) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Get or create collection
	coll, err := mm.getOrCreateCollection(collName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// Insert document
	docID := doc["_id"].(string)
	coll.Documents[docID] = doc

	// Update cache
	mm.cache.Put(collName+":"+docID, doc, collName)

	return nil
}

// BatchInsertDocuments inserts multiple documents into memory
func (mm *MemoryManager) BatchInsertDocuments(collName string, docs []domain.Document) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Get or create collection
	coll, err := mm.getOrCreateCollection(collName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// Insert all documents
	for _, doc := range docs {
		docID := doc["_id"].(string)
		coll.Documents[docID] = doc
		mm.cache.Put(collName+":"+docID, doc, collName)
	}

	return nil
}

// GetById retrieves a document by ID
func (mm *MemoryManager) GetById(collName, docID string) (domain.Document, error) {
	// Try cache first
	if doc, found := mm.cache.Get(collName + ":" + docID); found {
		if domainDoc, ok := doc.(domain.Document); ok {
			return domainDoc, nil
		}
	}

	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Get collection
	coll, exists := mm.collections[collName]
	if !exists {
		return nil, fmt.Errorf("collection %s not found", collName)
	}

	// Get document
	doc, exists := coll.Documents[docID]
	if !exists {
		return nil, fmt.Errorf("document %s not found in collection %s", docID, collName)
	}

	// Update cache
	mm.cache.Put(collName+":"+docID, doc, collName)

	return doc, nil
}

// UpdateDocument updates a document in memory
func (mm *MemoryManager) UpdateDocument(collName, docID string, doc domain.Document) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Get collection
	coll, exists := mm.collections[collName]
	if !exists {
		return fmt.Errorf("collection %s not found", collName)
	}

	// Update document
	coll.Documents[docID] = doc

	// Update cache
	mm.cache.Put(collName+":"+docID, doc, collName)

	return nil
}

// ReplaceDocument replaces a document in memory
func (mm *MemoryManager) ReplaceDocument(collName, docID string, doc domain.Document) error {
	return mm.UpdateDocument(collName, docID, doc)
}

// DeleteDocument deletes a document from memory
func (mm *MemoryManager) DeleteDocument(collName, docID string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Get collection
	coll, exists := mm.collections[collName]
	if !exists {
		return fmt.Errorf("collection %s not found", collName)
	}

	// Delete document
	delete(coll.Documents, docID)

	// Remove from cache
	mm.cache.Remove(collName + ":" + docID)

	return nil
}

// FindAll finds all documents matching a filter
func (mm *MemoryManager) FindAll(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Get collection
	coll, exists := mm.collections[collName]
	if !exists {
		return &domain.PaginationResult{
			Documents: []domain.Document{},
			Total:     0,
			HasNext:   false,
			HasPrev:   false,
		}, nil
	}

	// Filter documents
	var filteredDocs []domain.Document
	for _, doc := range coll.Documents {
		if mm.matchesFilter(doc, filter) {
			filteredDocs = append(filteredDocs, doc)
		}
	}

	// Apply pagination
	total := len(filteredDocs)
	limit := 50
	offset := 0

	if options != nil {
		if options.Limit > 0 {
			limit = options.Limit
		}
		if options.Offset > 0 {
			offset = options.Offset
		}
	}

	start := offset
	end := start + limit

	if start >= total {
		filteredDocs = []domain.Document{}
	} else {
		if end > total {
			end = total
		}
		filteredDocs = filteredDocs[start:end]
	}

	// Generate cursors for pagination
	var nextCursor, prevCursor string
	if end < total && len(filteredDocs) > 0 {
		// Use the last document's ID as next cursor
		lastDoc := filteredDocs[len(filteredDocs)-1]
		if docID, ok := lastDoc["_id"].(string); ok {
			nextCursor = docID
		}
	}
	if offset > 0 && len(filteredDocs) > 0 {
		// Use the first document's ID as prev cursor
		firstDoc := filteredDocs[0]
		if docID, ok := firstDoc["_id"].(string); ok {
			prevCursor = docID
		}
	}

	return &domain.PaginationResult{
		Documents:  filteredDocs,
		Total:      int64(total),
		HasNext:    end < total,
		HasPrev:    offset > 0,
		NextCursor: nextCursor,
		PrevCursor: prevCursor,
	}, nil
}

// FindAllStream finds all documents matching a filter and streams them
func (mm *MemoryManager) FindAllStream(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	ch := make(chan domain.Document, 100) // Buffer for performance

	go func() {
		defer close(ch)

		mm.mu.RLock()
		coll, exists := mm.collections[collName]
		mm.mu.RUnlock()

		if !exists {
			return
		}

		for _, doc := range coll.Documents {
			if mm.matchesFilter(doc, filter) {
				select {
				case ch <- doc:
				case <-time.After(5 * time.Second):
					return // Timeout to prevent blocking
				}
			}
		}
	}()

	return ch, nil
}

// BatchUpdateDocuments updates multiple documents in memory atomically
func (mm *MemoryManager) BatchUpdateDocuments(collName string, updates []domain.BatchUpdateOperation) ([]domain.Document, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Get collection
	coll, exists := mm.collections[collName]
	if !exists {
		return nil, fmt.Errorf("collection %s not found", collName)
	}

	// Validate all operations first (atomic behavior)
	for i, update := range updates {
		if update.ID == "" {
			return nil, fmt.Errorf("operation %d: document ID cannot be empty", i)
		}

		// Check if document exists
		_, exists := coll.Documents[update.ID]
		if !exists {
			return nil, fmt.Errorf("operation %d: document with id %s not found", i, update.ID)
		}
	}

	// All validations passed, now apply updates atomically
	var results []domain.Document
	for _, update := range updates {
		// Get existing document (we know it exists from validation above)
		existing := coll.Documents[update.ID]

		// Merge updates
		updated := mm.mergeDocuments(existing, update.Updates)

		// Update document
		coll.Documents[update.ID] = updated
		results = append(results, updated)

		// Update cache
		mm.cache.Put(collName+":"+update.ID, updated, collName)
	}

	return results, nil
}

// GetAllDocuments returns all documents in a collection
func (mm *MemoryManager) GetAllDocuments(collName string) (map[string]interface{}, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Get collection
	coll, exists := mm.collections[collName]
	if !exists {
		return make(map[string]interface{}), nil
	}

	// Convert to map[string]interface{}
	result := make(map[string]interface{})
	for docID, doc := range coll.Documents {
		result[docID] = doc
	}

	return result, nil
}

// GetMemoryStats returns memory usage statistics
func (mm *MemoryManager) GetMemoryStats() map[string]interface{} {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	totalDocs := 0
	for _, coll := range mm.collections {
		totalDocs += len(coll.Documents)
	}

	return map[string]interface{}{
		"collections":     len(mm.collections),
		"total_documents": totalDocs,
		"cache_size":      mm.cache.Size(),
		"max_memory_mb":   mm.maxMemoryMB,
	}
}

// Private methods

func (mm *MemoryManager) getOrCreateCollection(collName string) (*Collection, error) {
	if coll, exists := mm.collections[collName]; exists {
		return coll, nil
	}

	// Create new collection
	coll := &Collection{
		Name:      collName,
		Documents: make(map[string]domain.Document),
		CreatedAt: time.Now(),
	}

	mm.collections[collName] = coll
	return coll, nil
}

func (mm *MemoryManager) matchesFilter(doc domain.Document, filter map[string]interface{}) bool {
	if len(filter) == 0 {
		return true
	}

	for key, expectedValue := range filter {
		actualValue, exists := doc[key]
		if !exists {
			return false
		}

		if actualValue != expectedValue {
			return false
		}
	}

	return true
}

func (mm *MemoryManager) mergeDocuments(existing, updates domain.Document) domain.Document {
	merged := make(domain.Document)

	// Copy existing document
	for k, v := range existing {
		merged[k] = v
	}

	// Apply updates
	for k, v := range updates {
		merged[k] = v
	}

	return merged
}

// LRU Cache implementation

// NewLRUCache creates a new LRU cache
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*CacheEntry),
	}
}

// Get retrieves a value from the cache
func (c *LRUCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.cache[key]
	if !exists {
		return nil, false
	}

	// Move to head (most recently used)
	c.moveToHead(entry)
	entry.lastAccess = time.Now()

	return entry.value, true
}

// Put stores a value in the cache
func (c *LRUCache) Put(key string, value interface{}, collection string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.cache[key]; exists {
		// Update existing entry
		entry.value = value
		entry.lastAccess = time.Now()
		c.moveToHead(entry)
		return
	}

	// Create new entry
	entry := &CacheEntry{
		key:        key,
		value:      value,
		collection: collection,
		lastAccess: time.Now(),
	}

	// Add to cache
	c.cache[key] = entry
	c.addToHead(entry)

	// Evict if over capacity
	if len(c.cache) > c.capacity {
		c.evictLRU()
	}
}

// Remove removes a value from the cache
func (c *LRUCache) Remove(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[key]
	if !exists {
		return
	}

	c.removeEntry(entry)
	delete(c.cache, key)
}

// Size returns the current cache size
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// Private LRU methods

func (c *LRUCache) addToHead(entry *CacheEntry) {
	entry.prev = nil
	entry.next = c.head

	if c.head != nil {
		c.head.prev = entry
	}

	c.head = entry

	if c.tail == nil {
		c.tail = entry
	}
}

func (c *LRUCache) removeEntry(entry *CacheEntry) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		c.head = entry.next
	}

	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		c.tail = entry.prev
	}
}

func (c *LRUCache) moveToHead(entry *CacheEntry) {
	c.removeEntry(entry)
	c.addToHead(entry)
}

func (c *LRUCache) evictLRU() {
	if c.tail == nil {
		return
	}

	// Remove tail (least recently used)
	c.removeEntry(c.tail)
	delete(c.cache, c.tail.key)
}
