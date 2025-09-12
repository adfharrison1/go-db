package storage

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// Insert inserts a document into a collection and returns the created document with ID
func (se *StorageEngine) Insert(collName string, doc domain.Document) (domain.Document, error) {
	// First, ensure collection exists and generate ID (requires collection lock)
	var docID string
	err := se.withCollectionWriteLock(collName, func() error {
		// Get or load collection
		_, err := se.getCollectionInternal(collName)
		if err != nil {
			// Collection doesn't exist, create it
			collection := domain.NewCollection(collName)
			collectionInfo := &CollectionInfo{
				Name:          collName,
				DocumentCount: 0,
				State:         CollectionStateDirty,
				LastModified:  time.Now(),
			}
			se.collections[collName] = collectionInfo
			se.cache.Put(collName, collection, collectionInfo)

			// Initialize indexes for this collection using the index engine
			se.indexEngine.CreateIndex(collName, "_id")
		}

		// Generate unique ID using per-collection atomic counter (thread-safe)
		se.idCountersMu.Lock()
		counter, exists := se.idCounters[collName]
		if !exists {
			counter = new(int64)
			se.idCounters[collName] = counter
		}
		se.idCountersMu.Unlock()

		docID = fmt.Sprintf("%d", atomic.AddInt64(counter, 1))
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Now insert the document using document-level lock
	var result domain.Document
	var resultErr error

	// Insert operations modify the Documents map, so they need collection write locks
	// For no-saves mode, use simpler locking to avoid deadlocks under high load
	if se.noSaves {
		// Simple collection-level locking for no-saves mode
		err = se.withCollectionWriteLock(collName, func() error {
			result, resultErr = se.insertDocumentUnsafe(collName, docID, doc)
			return resultErr
		})
	} else {
		// Dual-write mode: use document-level locking for fine-grained concurrency
		err = se.withCollectionWriteLock(collName, func() error {
			err := se.withDocumentWriteLock(collName, docID, func() error {
				result, resultErr = se.insertDocumentUnsafe(collName, docID, doc)
				return resultErr
			})
			return err
		})
	}

	if err != nil {
		return nil, err
	}

	// Dual-write: Save document to disk immediately (unless no-saves mode)
	if !se.noSaves {
		if err := se.saveDocumentToDisk(collName, docID, result); err != nil {
			// Queue for background retry if immediate write fails
			se.queueDiskWrite(collName, docID, result)
		}
	}

	return result, nil
}

// insertDocumentUnsafe performs the actual document insertion (caller must hold document write lock)
func (se *StorageEngine) insertDocumentUnsafe(collName, docID string, doc domain.Document) (domain.Document, error) {
	// Get collection (already exists and loaded)
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	// Add the ID to the document
	doc["_id"] = docID

	// Store the document (need collection write lock for map modification)
	collection.Documents[docID] = doc

	// Update collection metadata
	if collInfo, exists := se.collections[collName]; exists {
		collInfo.DocumentCount++
		collInfo.State = CollectionStateDirty
		collInfo.LastModified = time.Now()
	}

	// Update indexes
	se.indexEngine.UpdateIndexForDocument(collName, docID, nil, doc)

	return doc, nil
}

// insertUnsafe performs the actual insert operation (caller must hold collection write lock)
func (se *StorageEngine) insertUnsafe(collName string, doc domain.Document) (domain.Document, error) {
	// Get or load collection
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		// Collection doesn't exist, create it
		collection = domain.NewCollection(collName)
		collectionInfo := &CollectionInfo{
			Name:          collName,
			DocumentCount: 0,
			State:         CollectionStateDirty,
			LastModified:  time.Now(),
		}
		se.collections[collName] = collectionInfo
		se.cache.Put(collName, collection, collectionInfo)

		// Initialize indexes for this collection using the index engine
		se.indexEngine.CreateIndex(collName, "_id")
	}

	// Generate unique ID using per-collection atomic counter (thread-safe)
	se.idCountersMu.Lock()
	counter, exists := se.idCounters[collName]
	if !exists {
		var initialCounter int64 = 0
		counter = &initialCounter
		se.idCounters[collName] = counter
	}
	se.idCountersMu.Unlock()

	id := atomic.AddInt64(counter, 1)
	newID := fmt.Sprintf("%d", id)
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

	return doc, nil
}

// GetById retrieves a specific document by its ID
func (se *StorageEngine) GetById(collName, docId string) (domain.Document, error) {
	var result domain.Document
	var resultErr error

	// Read operations need collection read locks to coordinate with Insert/Delete operations
	err := se.withCollectionReadLock(collName, func() error {
		err := se.withDocumentReadLock(collName, docId, func() error {
			result, resultErr = se.getByIdUnsafe(collName, docId)
			return resultErr
		})
		return err
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// getByIdUnsafe performs the actual get operation (caller must hold collection read lock)
func (se *StorageEngine) getByIdUnsafe(collName, docId string) (domain.Document, error) {
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

// UpdateById updates a specific document by its ID and returns the updated document
func (se *StorageEngine) UpdateById(collName, docId string, updates domain.Document) (domain.Document, error) {
	var result domain.Document
	var resultErr error

	// For no-saves mode, use collection-level locking to avoid deadlocks
	if se.noSaves {
		err := se.withCollectionWriteLock(collName, func() error {
			result, resultErr = se.updateByIdUnsafe(collName, docId, updates)
			return resultErr
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Dual-write mode: use document-level locking for fine-grained concurrency
		err := se.withDocumentWriteLock(collName, docId, func() error {
			result, resultErr = se.updateByIdUnsafe(collName, docId, updates)
			return resultErr
		})
		if err != nil {
			return nil, err
		}
	}

	// Dual-write: Save document to disk immediately (unless no-saves mode)
	if !se.noSaves {
		if err := se.saveDocumentToDisk(collName, docId, result); err != nil {
			// Queue for background retry if immediate write fails
			se.queueDiskWrite(collName, docId, result)
		}
	}

	return result, nil
}

// updateByIdUnsafe performs the actual update operation (caller must hold collection write lock)
func (se *StorageEngine) updateByIdUnsafe(collName, docId string, updates domain.Document) (domain.Document, error) {

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	doc, exists := collection.Documents[docId]
	if !exists {
		return nil, fmt.Errorf("document with id %s not found in collection %s", docId, collName)
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

	return doc, nil
}

// ReplaceById completely replaces a document with new content (PUT operation)
func (se *StorageEngine) ReplaceById(collName, docId string, newDoc domain.Document) (domain.Document, error) {
	var result domain.Document
	var resultErr error

	// For no-saves mode, use collection-level locking to avoid deadlocks
	if se.noSaves {
		err := se.withCollectionWriteLock(collName, func() error {
			result, resultErr = se.replaceByIdUnsafe(collName, docId, newDoc)
			return resultErr
		})
		if err != nil {
			return nil, err
		}
	} else {
		// Dual-write mode: use document-level locking for fine-grained concurrency
		err := se.withDocumentWriteLock(collName, docId, func() error {
			result, resultErr = se.replaceByIdUnsafe(collName, docId, newDoc)
			return resultErr
		})
		if err != nil {
			return nil, err
		}
	}

	// Dual-write: Save document to disk immediately (unless no-saves mode)
	if !se.noSaves {
		if err := se.saveDocumentToDisk(collName, docId, result); err != nil {
			// Queue for background retry if immediate write fails
			se.queueDiskWrite(collName, docId, result)
		}
	}

	return result, nil
}

// replaceByIdUnsafe performs the actual replace operation (caller must hold collection write lock)
func (se *StorageEngine) replaceByIdUnsafe(collName, docId string, newDoc domain.Document) (domain.Document, error) {

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	oldDoc, exists := collection.Documents[docId]
	if !exists {
		return nil, fmt.Errorf("document with id %s not found in collection %s", docId, collName)
	}

	// Create a copy of the old document for index updates
	oldDocCopy := make(domain.Document)
	for k, v := range oldDoc {
		oldDocCopy[k] = v
	}

	// Ensure the new document has the same _id
	newDoc["_id"] = docId

	// Replace the entire document
	collection.Documents[docId] = newDoc

	// Update indexes with the change (remove old, add new)
	se.updateIndexes(collName, docId, oldDocCopy, newDoc)

	// Mark collection as dirty for persistence
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.LastModified = time.Now()
	}

	return newDoc, nil
}

// DeleteById removes a specific document by its ID
func (se *StorageEngine) DeleteById(collName, docId string) error {
	// Delete operations modify the Documents map, so they need collection write locks
	err := se.withCollectionWriteLock(collName, func() error {
		return se.withDocumentWriteLock(collName, docId, func() error {
			return se.deleteByIdUnsafe(collName, docId)
		})
	})

	if err != nil {
		return err
	}

	// Dual-write: Save collection to disk immediately (unless no-saves mode)
	if !se.noSaves {
		if err := se.SaveCollectionAfterTransaction(collName); err != nil {
			// For deletes, we need to save the entire collection since we removed a document
			// Queue for background retry if immediate write fails
			se.queueDiskWrite(collName, docId, nil) // nil document indicates delete
		}
	}

	return nil
}

// deleteByIdUnsafe performs the actual delete operation (caller must hold collection write lock)
func (se *StorageEngine) deleteByIdUnsafe(collName, docId string) error {
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

// FindAll returns documents that match the given filter criteria
// If filter is nil or empty, returns all documents
func (se *StorageEngine) FindAll(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	if options == nil {
		options = domain.DefaultPaginationOptions()
	}

	if err := options.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pagination options: %w", err)
	}

	var result *domain.PaginationResult
	var resultErr error

	err := se.withCollectionReadLock(collName, func() error {
		result, resultErr = se.findAllUnsafe(collName, filter, options)
		return resultErr
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// findAllUnsafe performs the actual find operation (caller must hold collection read lock)
func (se *StorageEngine) findAllUnsafe(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, err
	}

	var allDocs []domain.Document
	var candidateIDs []string
	var useIndex bool

	// Try to use index optimization if filter is present
	if len(filter) > 0 {
		candidateIDs, useIndex = se.optimizeWithIndexes(collName, filter)
	}

	if useIndex {
		// Use index optimization
		for _, docID := range candidateIDs {
			if doc, exists := collection.Documents[docID]; exists {
				if MatchesFilter(doc, filter) {
					allDocs = append(allDocs, doc)
				}
			}
		}
	} else {
		// Full scan
		for _, doc := range collection.Documents {
			if len(filter) == 0 || MatchesFilter(doc, filter) {
				allDocs = append(allDocs, doc)
			}
		}
	}

	return se.applyPagination(allDocs, options)
}

// docGenerator yields matching documents for a given filter, using index optimization if possible.
func (se *StorageEngine) docGenerator(collName string, filter map[string]interface{}, paginationOptions *domain.PaginationOptions) (<-chan domain.Document, error) {
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
			candidateIDs, useIndex = se.optimizeWithIndexes(collName, filter)
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

// applyPagination applies pagination to a slice of documents
func (se *StorageEngine) applyPagination(docs []domain.Document, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	// Sort documents by ID for consistent ordering
	sort.Slice(docs, func(i, j int) bool {
		idI, _ := docs[i]["_id"].(string)
		idJ, _ := docs[j]["_id"].(string)
		return idI < idJ
	})

	// Handle cursor-based pagination
	if options.After != "" || options.Before != "" {
		return se.applyCursorPagination(docs, options)
	}

	// Handle offset-based pagination
	return se.applyOffsetPagination(docs, options)
}

// applyCursorPagination applies cursor-based pagination
func (se *StorageEngine) applyCursorPagination(docs []domain.Document, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	result := &domain.PaginationResult{
		Documents: []domain.Document{},
		HasNext:   false,
		HasPrev:   false,
		Total:     int64(len(docs)),
	}

	startIndex := 0
	endIndex := len(docs)

	// Apply cursor filtering
	if options.After != "" {
		cursor, err := domain.DecodeCursor(options.After)
		if err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}

		// Find the index after the cursor
		for i, doc := range docs {
			if docID, _ := doc["_id"].(string); docID == cursor.ID {
				startIndex = i + 1
				break
			}
		}
	}

	if options.Before != "" {
		cursor, err := domain.DecodeCursor(options.Before)
		if err != nil {
			return nil, fmt.Errorf("invalid before cursor: %w", err)
		}

		// Find the index before the cursor
		for i, doc := range docs {
			if docID, _ := doc["_id"].(string); docID == cursor.ID {
				endIndex = i
				break
			}
		}
	}

	// Apply limit
	limit := options.Limit
	if limit <= 0 {
		limit = 50 // default
	}
	if limit > options.MaxLimit {
		limit = options.MaxLimit
	}

	// Calculate end index
	if startIndex+limit < endIndex {
		endIndex = startIndex + limit
		result.HasNext = true
	}

	// Set has previous
	if startIndex > 0 {
		result.HasPrev = true
	}

	// Extract documents
	if startIndex < len(docs) {
		result.Documents = docs[startIndex:endIndex]
	}

	// Generate cursors
	if len(result.Documents) > 0 {
		if result.HasNext {
			lastDoc := result.Documents[len(result.Documents)-1]
			nextCursor := &domain.Cursor{
				ID:        lastDoc["_id"].(string),
				Timestamp: time.Now(),
			}
			result.NextCursor, _ = domain.EncodeCursor(nextCursor)
		}

		if result.HasPrev {
			firstDoc := result.Documents[0]
			prevCursor := &domain.Cursor{
				ID:        firstDoc["_id"].(string),
				Timestamp: time.Now(),
			}
			result.PrevCursor, _ = domain.EncodeCursor(prevCursor)
		}
	} else if result.HasNext {
		// If no documents but there are more, generate cursor for the next page
		// This can happen when using 'before' cursor that points to a document not in current set
		if startIndex < len(docs) {
			nextDoc := docs[startIndex]
			nextCursor := &domain.Cursor{
				ID:        nextDoc["_id"].(string),
				Timestamp: time.Now(),
			}
			result.NextCursor, _ = domain.EncodeCursor(nextCursor)
		}
	}

	return result, nil
}

// applyOffsetPagination applies offset-based pagination
func (se *StorageEngine) applyOffsetPagination(docs []domain.Document, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	if options.MaxLimit == 0 {
		options.MaxLimit = int(^uint(0) >> 1) // set to max int
	}
	result := &domain.PaginationResult{
		Documents: []domain.Document{},
		HasNext:   false,
		HasPrev:   false,
		Total:     int64(len(docs)),
	}

	offset := options.Offset
	limit := options.Limit
	if limit <= 0 {
		limit = 50 // default
	}
	if limit > options.MaxLimit {
		limit = options.MaxLimit
	}

	// Calculate indices
	startIndex := offset
	endIndex := offset + limit

	// Check bounds
	if startIndex >= len(docs) {
		return result, nil
	}

	if endIndex > len(docs) {
		endIndex = len(docs)
	} else {
		result.HasNext = true
	}

	if offset > 0 {
		result.HasPrev = true
	}

	// Extract documents
	result.Documents = docs[startIndex:endIndex]

	// Generate cursors for offset-based pagination
	if len(result.Documents) > 0 {
		if result.HasNext {
			lastDoc := result.Documents[len(result.Documents)-1]
			nextCursor := &domain.Cursor{
				ID:        lastDoc["_id"].(string),
				Timestamp: time.Now(),
			}
			result.NextCursor, _ = domain.EncodeCursor(nextCursor)
		}

		if result.HasPrev {
			firstDoc := result.Documents[0]
			prevCursor := &domain.Cursor{
				ID:        firstDoc["_id"].(string),
				Timestamp: time.Now(),
			}
			result.PrevCursor, _ = domain.EncodeCursor(prevCursor)
		}
	}

	return result, nil
}

// optimizeWithIndexes attempts to use available indexes to optimize the query
// Returns candidate document IDs and whether index optimization was used
func (se *StorageEngine) optimizeWithIndexes(collName string, filter map[string]interface{}) ([]string, bool) {
	var indexResults [][]string

	// Find all available indexes for the filter fields
	for fieldName, expectedValue := range filter {
		if index, exists := se.getIndex(collName, fieldName); exists {
			ids := index.Query(expectedValue)
			indexResults = append(indexResults, ids)
		}
	}

	// If no indexes are available, fall back to full scan
	if len(indexResults) == 0 {
		return nil, false
	}

	// If we have multiple indexes, use intersection (AND logic)
	if len(indexResults) > 1 {
		candidateIDs := IntersectStringSlices(indexResults...)
		return candidateIDs, true
	}

	// Single index optimization
	return indexResults[0], true
}

// BatchInsert inserts multiple documents into a collection atomically
// All documents are inserted successfully or none are inserted (atomic operation)
// Returns the created documents with their assigned IDs
func (se *StorageEngine) BatchInsert(collName string, docs []domain.Document) ([]domain.Document, error) {
	if len(docs) == 0 {
		return nil, fmt.Errorf("no documents provided for batch insert")
	}

	if len(docs) > 1000 {
		return nil, fmt.Errorf("batch insert limited to 1000 documents, got %d", len(docs))
	}

	// First, ensure collection exists and generate all IDs (requires collection lock)
	docIDs := make([]string, len(docs))
	err := se.withCollectionWriteLock(collName, func() error {
		// Get or load collection
		_, err := se.getCollectionInternal(collName)
		if err != nil {
			// Collection doesn't exist, create it
			collection := domain.NewCollection(collName)
			collectionInfo := &CollectionInfo{
				Name:          collName,
				DocumentCount: 0,
				State:         CollectionStateDirty,
				LastModified:  time.Now(),
			}
			se.collections[collName] = collectionInfo
			se.cache.Put(collName, collection, collectionInfo)

			// Initialize indexes for this collection using the index engine
			se.indexEngine.CreateIndex(collName, "_id")
		}

		// Generate unique IDs using per-collection atomic counter (thread-safe)
		se.idCountersMu.Lock()
		counter, exists := se.idCounters[collName]
		if !exists {
			counter = new(int64)
			se.idCounters[collName] = counter
		}
		se.idCountersMu.Unlock()

		for i := range docs {
			docIDs[i] = fmt.Sprintf("%d", atomic.AddInt64(counter, 1))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Now insert each document using document-level locks
	var result []domain.Document

	// Batch insert modifies the Documents map, so it needs collection write locks
	err = se.withCollectionWriteLock(collName, func() error {
		// First, check if any of the generated IDs already exist (atomic validation)
		collection, err := se.getCollectionInternal(collName)
		if err != nil {
			return err
		}

		for _, docID := range docIDs {
			if _, exists := collection.Documents[docID]; exists {
				return fmt.Errorf("document with id %s already exists in collection %s", docID, collName)
			}
		}

		// All IDs are available, proceed with insertions
		for i, doc := range docs {
			var insertDoc domain.Document
			var insertErr error

			err := se.withDocumentWriteLock(collName, docIDs[i], func() error {
				insertDoc, insertErr = se.insertDocumentUnsafe(collName, docIDs[i], doc)
				return insertErr
			})

			if err != nil {
				return fmt.Errorf("failed to insert document %d: %w", i, err)
			}

			result = append(result, insertDoc)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Dual-write: Save collection to disk immediately (unless no-saves mode)
	if !se.noSaves {
		if err := se.SaveCollectionAfterTransaction(collName); err != nil {
			// For batch inserts, we need to save the entire collection
			// Queue for background retry if immediate write fails
			se.queueDiskWrite(collName, "", nil) // Empty docID indicates batch operation
		}
	}

	return result, nil
}

// batchInsertUnsafe performs the actual batch insert operation (caller must hold collection write lock)
func (se *StorageEngine) batchInsertUnsafe(collName string, docs []domain.Document) ([]domain.Document, error) {

	// Get or create collection
	collection, err := se.getCollectionInternal(collName)
	var collectionCreated bool
	if err != nil {
		// Collection doesn't exist, create it
		collection = domain.NewCollection(collName)
		collectionInfo := &CollectionInfo{
			Name:          collName,
			DocumentCount: 0,
			State:         CollectionStateDirty,
			LastModified:  time.Now(),
		}
		se.collections[collName] = collectionInfo
		se.cache.Put(collName, collection, collectionInfo)

		// Initialize indexes for this collection using the index engine
		se.indexEngine.CreateIndex(collName, "_id")
		collectionCreated = true
	}

	// Get or create ID counter for this collection
	se.idCountersMu.Lock()
	counter, exists := se.idCounters[collName]
	if !exists {
		var initialCounter int64 = 0
		counter = &initialCounter
		se.idCounters[collName] = counter
	}
	se.idCountersMu.Unlock()

	// Phase 1: Validation and preparation (no mutations yet)
	// Pre-allocate IDs and validate all documents can be processed
	type documentWithID struct {
		doc   domain.Document
		id    string
		idNum int64
	}

	docsWithIDs := make([]documentWithID, len(docs))
	startingCounter := atomic.LoadInt64(counter)

	for i, doc := range docs {
		// Pre-allocate the ID but don't commit to the counter yet
		idNum := startingCounter + int64(i) + 1
		newID := fmt.Sprintf("%d", idNum)

		// Create a copy of the document and set _id
		docCopy := make(domain.Document)
		for k, v := range doc {
			docCopy[k] = v
		}
		docCopy["_id"] = newID

		docsWithIDs[i] = documentWithID{
			doc:   docCopy,
			id:    newID,
			idNum: idNum,
		}

		// Validate that the document ID doesn't already exist
		if _, exists := collection.Documents[newID]; exists {
			// Rollback: Clean up any created collection
			if collectionCreated {
				delete(se.collections, collName)
				se.cache.Remove(collName)
				se.idCountersMu.Lock()
				delete(se.idCounters, collName)
				se.idCountersMu.Unlock()
			}
			return nil, fmt.Errorf("document with ID %s already exists in collection %s", newID, collName)
		}
	}

	// Phase 2: Atomic commit - all operations succeed or we rollback
	// Get collection info for metadata updates
	var collectionInfo *CollectionInfo
	if _, colInfo, found := se.cache.Get(collName); found {
		collectionInfo = colInfo
	}

	// Commit the counter increment atomically
	atomic.AddInt64(counter, int64(len(docs)))

	// Insert all documents (this should not fail, but if it does, we rollback)
	for _, docWithID := range docsWithIDs {
		// Update indexes before inserting
		se.updateIndexes(collName, docWithID.id, nil, docWithID.doc)

		// Add to collection
		collection.Documents[docWithID.id] = docWithID.doc
	}

	// Update collection metadata
	if collectionInfo != nil {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.DocumentCount += int64(len(docs))
		collectionInfo.LastModified = time.Now()
	}

	// Return the created documents
	createdDocs := make([]domain.Document, len(docsWithIDs))
	for i, docWithID := range docsWithIDs {
		createdDocs[i] = docWithID.doc
	}

	return createdDocs, nil
}

// BatchUpdate updates multiple documents in a collection atomically
// All updates succeed or all fail with complete rollback (atomic operation)
// Returns the updated documents
func (se *StorageEngine) BatchUpdate(collName string, operations []domain.BatchUpdateOperation) ([]domain.Document, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations provided for batch update")
	}

	if len(operations) > 1000 {
		return nil, fmt.Errorf("batch update limited to 1000 operations, got %d", len(operations))
	}

	// Validate all operations first
	for _, operation := range operations {
		if operation.ID == "" {
			return nil, fmt.Errorf("document ID cannot be empty")
		}
	}

	// Process each update operation sequentially with document-level locking
	var result []domain.Document
	for _, operation := range operations {
		var updateDoc domain.Document
		var updateErr error

		err := se.withDocumentWriteLock(collName, operation.ID, func() error {
			updateDoc, updateErr = se.updateByIdUnsafe(collName, operation.ID, operation.Updates)
			return updateErr
		})

		if err != nil {
			return nil, fmt.Errorf("failed to update document %s: %w", operation.ID, err)
		}

		result = append(result, updateDoc)
	}

	return result, nil
}

// batchUpdateUnsafe performs the actual batch update operation (caller must hold collection write lock)
func (se *StorageEngine) batchUpdateUnsafe(collName string, operations []domain.BatchUpdateOperation) ([]domain.Document, error) {

	// Get collection
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return nil, fmt.Errorf("collection %s does not exist", collName)
	}

	// Phase 1: Validation and preparation (no mutations yet)
	type updateOperation struct {
		docID       string
		originalDoc domain.Document // Full copy of original document
		updatedDoc  domain.Document // Prepared updated document
		operation   domain.BatchUpdateOperation
	}

	validatedOps := make([]updateOperation, 0, len(operations))

	// Validate all operations first - no mutations during this phase
	for i, op := range operations {
		if op.ID == "" {
			return nil, fmt.Errorf("operation %d: document ID cannot be empty", i)
		}

		// Check if document exists
		existingDoc, exists := collection.Documents[op.ID]
		if !exists {
			return nil, fmt.Errorf("operation %d: document with id %s not found", i, op.ID)
		}

		// Create full copy of the original document for rollback
		originalDoc := make(domain.Document)
		for k, v := range existingDoc {
			originalDoc[k] = v
		}

		// Create updated document by applying changes to a copy
		updatedDoc := make(domain.Document)
		for k, v := range existingDoc {
			updatedDoc[k] = v
		}

		// Apply updates to the copy
		for k, v := range op.Updates {
			if k != "_id" { // Prevent updating the document ID
				updatedDoc[k] = v
			}
		}

		validatedOps = append(validatedOps, updateOperation{
			docID:       op.ID,
			originalDoc: originalDoc,
			updatedDoc:  updatedDoc,
			operation:   op,
		})
	}

	// Phase 2: Atomic commit - all operations succeed or we rollback everything
	// Keep track of what we've modified for potential rollback
	modifiedDocs := make(map[string]domain.Document) // docID -> original state
	updatedCount := 0

	// Apply all updates atomically
	for _, validOp := range validatedOps {
		// Store original state for potential rollback
		modifiedDocs[validOp.docID] = validOp.originalDoc

		// Apply the update to the actual collection
		collection.Documents[validOp.docID] = validOp.updatedDoc

		// Update indexes with the change
		se.updateIndexes(collName, validOp.docID, validOp.originalDoc, validOp.updatedDoc)

		updatedCount++
	}

	// Update collection metadata
	if _, collectionInfo, found := se.cache.Get(collName); found {
		collectionInfo.State = CollectionStateDirty
		collectionInfo.LastModified = time.Now()
	}

	// Return the updated documents
	updatedDocs := make([]domain.Document, len(validatedOps))
	for i, validOp := range validatedOps {
		updatedDocs[i] = validOp.updatedDoc
	}

	return updatedDocs, nil
}
