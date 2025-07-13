package indexing

import (
	"fmt"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// IndexEngine implements domain.IndexEngine interface
type IndexEngine struct {
	indexes map[string]map[string]*Index // Collection name -> field name -> index
}

// NewIndexEngine creates a new index engine
func NewIndexEngine() *IndexEngine {
	return &IndexEngine{
		indexes: make(map[string]map[string]*Index),
	}
}

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

// CreateIndex creates an index on a specific field in a collection
func (ie *IndexEngine) CreateIndex(collectionName, fieldName string) error {
	// Initialize indexes map for this collection if it doesn't exist
	if ie.indexes[collectionName] == nil {
		ie.indexes[collectionName] = make(map[string]*Index)
	}

	// Check if index already exists
	if _, exists := ie.indexes[collectionName][fieldName]; exists {
		return fmt.Errorf("index on field %s already exists in collection %s", fieldName, collectionName)
	}

	// Create new index
	index := NewIndex(fieldName)
	ie.indexes[collectionName][fieldName] = index

	return nil
}

// DropIndex removes an index from a collection
func (ie *IndexEngine) DropIndex(collectionName, fieldName string) error {
	// Check if index exists
	if ie.indexes[collectionName] == nil {
		return fmt.Errorf("no indexes exist for collection %s", collectionName)
	}

	if _, exists := ie.indexes[collectionName][fieldName]; !exists {
		return fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collectionName)
	}

	// Remove the index
	delete(ie.indexes[collectionName], fieldName)

	return nil
}

// FindByIndex finds documents using an index
func (ie *IndexEngine) FindByIndex(collectionName, fieldName string, value interface{}) ([]domain.Document, error) {
	// Get the index
	index, exists := ie.getIndex(collectionName, fieldName)
	if !exists {
		return nil, fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collectionName)
	}

	// Query the index
	docIDs := index.Query(value)
	if len(docIDs) == 0 {
		return []domain.Document{}, nil
	}

	// Note: This method would need access to the actual documents
	// For now, we return the document IDs that match
	// In a real implementation, you'd need to pass the collection or storage engine
	return nil, fmt.Errorf("FindByIndex requires access to documents - use storage engine instead")
}

// GetIndexes returns all index names for a collection
func (ie *IndexEngine) GetIndexes(collectionName string) ([]string, error) {
	// Get indexes for the collection
	collectionIndexes, exists := ie.indexes[collectionName]
	if !exists {
		return []string{}, nil // No indexes exist
	}

	// Extract index names
	var indexNames []string
	for fieldName := range collectionIndexes {
		indexNames = append(indexNames, fieldName)
	}

	return indexNames, nil
}

// UpdateIndex rebuilds an index for a collection
func (ie *IndexEngine) UpdateIndex(collectionName, fieldName string) error {
	// Check if index exists
	if ie.indexes[collectionName] == nil {
		return fmt.Errorf("no indexes exist for collection %s", collectionName)
	}

	_, exists := ie.indexes[collectionName][fieldName]
	if !exists {
		return fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collectionName)
	}

	// Note: This method would need access to the actual collection
	// For now, we just return success
	// In a real implementation, you'd need to pass the collection
	return fmt.Errorf("UpdateIndex requires access to collection - use storage engine instead")
}

// getIndex returns an index for a specific field in a collection
func (ie *IndexEngine) getIndex(collectionName, fieldName string) (*Index, bool) {
	if collectionIndexes, exists := ie.indexes[collectionName]; exists {
		if index, exists := collectionIndexes[fieldName]; exists {
			return index, true
		}
	}
	return nil, false
}

// Export the GetIndex method on IndexEngine so it can be used by the storage engine.
func (ie *IndexEngine) GetIndex(collectionName, fieldName string) (*Index, bool) {
	return ie.getIndex(collectionName, fieldName)
}

// BuildIndexForCollection builds an index for a specific collection
func (ie *IndexEngine) BuildIndexForCollection(collectionName, fieldName string, collection *domain.Collection) error {
	// Get or create the index
	if ie.indexes[collectionName] == nil {
		ie.indexes[collectionName] = make(map[string]*Index)
	}

	index, exists := ie.indexes[collectionName][fieldName]
	if !exists {
		index = NewIndex(fieldName)
		ie.indexes[collectionName][fieldName] = index
	}

	// Build the index
	index.BuildIndex(collection)
	return nil
}

// UpdateIndexForDocument updates an index when a document changes
func (ie *IndexEngine) UpdateIndexForDocument(collectionName, docID string, oldDoc, newDoc domain.Document) {
	if collectionIndexes, exists := ie.indexes[collectionName]; exists {
		for _, index := range collectionIndexes {
			index.UpdateIndex(docID, oldDoc, newDoc)
		}
	}
}
