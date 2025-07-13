package domain

// StorageEngine defines the interface for storage operations
// This is the core business interface that implementations must conform to
type StorageEngine interface {
	Insert(collName string, doc Document) error
	FindAll(collName string, filter map[string]interface{}) ([]Document, error)
	FindAllStream(collName string) (<-chan Document, error)
	GetById(collName, docId string) (Document, error)
	UpdateById(collName, docId string, updates Document) error
	DeleteById(collName, docId string) error
	CreateCollection(collName string) error
	GetCollection(collName string) (*Collection, error)
	LoadCollectionMetadata(filename string) error
	SaveToFile(filename string) error
	GetMemoryStats() map[string]interface{}
	StartBackgroundWorkers()
	StopBackgroundWorkers()
}

// DatabaseEngine combines StorageEngine and IndexEngine interfaces
type DatabaseEngine interface {
	StorageEngine
	IndexEngine
}
