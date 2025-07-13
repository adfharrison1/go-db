package storage

import (
	"github.com/adfharrison1/go-db/pkg/domain"
)

// FindAllStream streams documents in a collection that match the given filter criteria
// If filter is nil or empty, streams all documents
func (se *StorageEngine) FindAllStream(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	se.mu.RLock()
	collection, err := se.findDocumentsInternal(collName, filter)
	se.mu.RUnlock()
	if err != nil {
		return nil, err
	}

	docChan := make(chan domain.Document, 100)
	go func() {
		defer close(docChan)
		for _, doc := range collection.Documents {
			// Apply filter if provided
			if len(filter) > 0 && !matchesFilter(doc, filter) {
				continue
			}

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
