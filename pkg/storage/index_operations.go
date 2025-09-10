package storage

import (
	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/indexing"
)

// CreateIndex creates an index on a specific field in a collection
func (se *StorageEngine) CreateIndex(collName, fieldName string) error {
	return se.withCollectionWriteLock(collName, func() error {
		collection, err := se.getCollectionInternal(collName)
		if err != nil {
			return err
		}
		if err := se.indexEngine.CreateIndex(collName, fieldName); err != nil {
			return err
		}
		return se.indexEngine.BuildIndexForCollection(collName, fieldName, collection)
	})
}

// DropIndex removes an index from a collection
func (se *StorageEngine) DropIndex(collName, fieldName string) error {
	return se.withCollectionWriteLock(collName, func() error {
		return se.indexEngine.DropIndex(collName, fieldName)
	})
}

// FindByIndex finds documents using an index
func (se *StorageEngine) FindByIndex(collName, fieldName string, value interface{}) ([]domain.Document, error) {
	var results []domain.Document
	var resultErr error

	err := se.withCollectionReadLock(collName, func() error {
		collection, err := se.getCollectionInternal(collName)
		if err != nil {
			return err
		}
		index, exists := se.indexEngine.GetIndex(collName, fieldName)
		if !exists {
			results = nil
			return nil
		}
		ids := index.Query(value)
		for _, id := range ids {
			if doc, ok := collection.Documents[id]; ok {
				results = append(results, doc)
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return results, resultErr
}

// GetIndexes returns all index names for a collection
func (se *StorageEngine) GetIndexes(collName string) ([]string, error) {
	var result []string
	var resultErr error

	err := se.withCollectionReadLock(collName, func() error {
		result, resultErr = se.indexEngine.GetIndexes(collName)
		return resultErr
	})

	if err != nil {
		return nil, err
	}
	return result, nil
}

// UpdateIndex rebuilds an index for a collection
func (se *StorageEngine) UpdateIndex(collName, fieldName string) error {
	return se.withCollectionWriteLock(collName, func() error {
		collection, err := se.getCollectionInternal(collName)
		if err != nil {
			return err
		}
		return se.indexEngine.BuildIndexForCollection(collName, fieldName, collection)
	})
}

// getIndex returns an index for a specific field in a collection
func (se *StorageEngine) getIndex(collName, fieldName string) (*indexing.Index, bool) {
	return se.indexEngine.GetIndex(collName, fieldName)
}

// updateIndexes updates all indexes for a collection when a document changes
func (se *StorageEngine) updateIndexes(collName, docID string, oldDoc, newDoc domain.Document) {
	se.indexEngine.UpdateIndexForDocument(collName, docID, oldDoc, newDoc)
}
