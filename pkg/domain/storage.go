package domain

// BatchUpdateOperation represents a single update operation in a batch
type BatchUpdateOperation struct {
	ID      string   `json:"id"`      // Document ID to update
	Updates Document `json:"updates"` // Fields to update
}

// StorageEngine defines the interface for storage operations
// This is the core business interface that implementations must conform to
type StorageEngine interface {
	Insert(collName string, doc Document) error
	BatchInsert(collName string, docs []Document) error
	FindAll(collName string, filter map[string]interface{}, options *PaginationOptions) (*PaginationResult, error)
	FindAllStream(collName string, filter map[string]interface{}) (<-chan Document, error)
	GetById(collName, docId string) (Document, error)
	UpdateById(collName, docId string, updates Document) error
	BatchUpdate(collName string, updates []BatchUpdateOperation) error
	DeleteById(collName, docId string) error
	CreateCollection(collName string) error
	GetCollection(collName string) (*Collection, error)
	LoadCollectionMetadata(filename string) error
	SaveToFile(filename string) error
	GetMemoryStats() map[string]interface{}
	StartBackgroundWorkers()
	StopBackgroundWorkers()
	SaveCollectionAfterTransaction(collName string) error
	IsTransactionSaveEnabled() bool
}

// DatabaseEngine combines StorageEngine and IndexEngine interfaces
type DatabaseEngine interface {
	StorageEngine
	IndexEngine
}
