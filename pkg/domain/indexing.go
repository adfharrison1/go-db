package domain

// IndexEngine defines the interface for indexing operations
type IndexEngine interface {
	CreateIndex(collectionName, fieldName string) error
	DropIndex(collectionName, fieldName string) error
	FindByIndex(collectionName, fieldName string, value interface{}) ([]Document, error)
	GetIndexes(collectionName string) ([]string, error)
}

// Index represents an index on a collection field
type Index struct {
	CollectionName string                 `json:"collection_name"`
	FieldName      string                 `json:"field_name"`
	Values         map[interface{}]string `json:"values"` // value -> document ID
}
