package storage

import (
	"github.com/adfharrison1/go-db/pkg/domain"
)

// FindAllStream streams documents in a collection that match the given filter criteria
// If filter is nil or empty, streams all documents
func (se *StorageEngine) FindAllStream(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	return se.docGenerator(collName, filter)
}
