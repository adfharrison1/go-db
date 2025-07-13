package domain

// Document represents a document in the database
type Document map[string]interface{}

// Collection represents a collection of documents
type Collection struct {
	Name      string              `json:"name"`
	Documents map[string]Document `json:"documents"`
}

// NewCollection creates a new collection
func NewCollection(name string) *Collection {
	return &Collection{
		Name:      name,
		Documents: make(map[string]Document),
	}
}
