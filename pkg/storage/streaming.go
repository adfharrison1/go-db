package storage

import (
	"github.com/adfharrison1/go-db/pkg/data"
)

// FindAllStream streams all documents in a collection
func (se *StorageEngine) FindAllStream(collName string) (<-chan data.Document, error) {
	se.mu.RLock()
	collection, err := se.getCollectionInternal(collName)
	se.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	docChan := make(chan data.Document, 100)
	go func() {
		defer close(docChan)
		for _, doc := range collection.Documents {
			select {
			case docChan <- doc:
				// Document sent
			case <-se.stopChan:
				return
			}
		}
	}()
	return docChan, nil
}
