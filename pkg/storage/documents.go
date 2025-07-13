package storage

import (
	"fmt"
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
