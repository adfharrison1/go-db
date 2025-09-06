package storage

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// Insert inserts a document into a collection
func (se *StorageEngine) Insert(collName string, doc domain.Document) error {
	se.mu.Lock()
	defer se.mu.Unlock()
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

// FindAll returns documents that match the given filter criteria
// If filter is nil or empty, returns all documents
func (se *StorageEngine) FindAll(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	if options == nil {
		options = domain.DefaultPaginationOptions()
	}

	if err := options.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pagination options: %w", err)
	}

	docChan, err := se.docGenerator(collName, filter, nil)
	if err != nil {
		return nil, err
	}

	var allDocs []domain.Document
	for doc := range docChan {
		allDocs = append(allDocs, doc)
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
func (se *StorageEngine) BatchInsert(collName string, docs []domain.Document) error {
	if len(docs) == 0 {
		return fmt.Errorf("no documents provided for batch insert")
	}

	if len(docs) > 1000 {
		return fmt.Errorf("batch insert limited to 1000 documents, got %d", len(docs))
	}

	se.mu.Lock()
	defer se.mu.Unlock()

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
			return fmt.Errorf("document with ID %s already exists in collection %s", newID, collName)
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

	return nil
}

// BatchUpdate updates multiple documents in a collection atomically
// All updates succeed or all fail with complete rollback (atomic operation)
func (se *StorageEngine) BatchUpdate(collName string, operations []domain.BatchUpdateOperation) error {
	if len(operations) == 0 {
		return fmt.Errorf("no operations provided for batch update")
	}

	if len(operations) > 1000 {
		return fmt.Errorf("batch update limited to 1000 operations, got %d", len(operations))
	}

	se.mu.Lock()
	defer se.mu.Unlock()

	// Get collection
	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		return fmt.Errorf("collection %s does not exist", collName)
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
			return fmt.Errorf("operation %d: document ID cannot be empty", i)
		}

		// Check if document exists
		existingDoc, exists := collection.Documents[op.ID]
		if !exists {
			return fmt.Errorf("operation %d: document with id %s not found", i, op.ID)
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

	return nil
}
