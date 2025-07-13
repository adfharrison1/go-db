package indexing

import (
	"github.com/adfharrison1/go-db/pkg/domain"
)

// Index stores a mapping from a field's value to document IDs.
type Index struct {
	Field    string
	Inverted map[interface{}][]string
}

// NewIndex creates an index on a specific field.
func NewIndex(field string) *Index {
	return &Index{
		Field:    field,
		Inverted: make(map[interface{}][]string),
	}
}

// BuildIndex indexes all documents in a collection by the specified field.
func (idx *Index) BuildIndex(collection *domain.Collection) {
	for docID, doc := range collection.Documents {
		val, ok := doc[idx.Field]
		if ok {
			idx.Inverted[val] = append(idx.Inverted[val], docID)
		}
	}
}

// Query returns document IDs that match a given value in the indexed field.
func (idx *Index) Query(value interface{}) []string {
	if docIDs, ok := idx.Inverted[value]; ok {
		return docIDs
	}
	return nil
}

// UpdateIndex updates index after an insert/update/delete operation.
func (idx *Index) UpdateIndex(docID string, oldDoc, newDoc domain.Document) {
	// Remove old entry
	if oldVal, ok := oldDoc[idx.Field]; ok {
		// remove docID from the oldVal array
		docList := idx.Inverted[oldVal]
		for i, id := range docList {
			if id == docID {
				idx.Inverted[oldVal] = append(docList[:i], docList[i+1:]...)
				break
			}
		}
	}
	// Add new entry
	if newVal, ok := newDoc[idx.Field]; ok {
		idx.Inverted[newVal] = append(idx.Inverted[newVal], docID)
	}
}
