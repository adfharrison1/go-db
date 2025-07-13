package storage

import (
	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/indexing"
)

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
