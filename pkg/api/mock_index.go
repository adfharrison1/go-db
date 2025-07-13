package api

import (
	"fmt"
	"sync"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// MockIndexEngine provides a mock implementation of domain.IndexEngine for testing
type MockIndexEngine struct {
	mu          sync.RWMutex
	indexes     map[string]map[string]bool // collection -> field -> exists
	storage     *MockStorageEngine         // Reference to storage for collection validation
	createCalls int
	dropCalls   int
	findCalls   int
	getCalls    int
	updateCalls int
}

// NewMockIndexEngine creates a new mock index engine
func NewMockIndexEngine() *MockIndexEngine {
	return &MockIndexEngine{
		indexes: make(map[string]map[string]bool),
	}
}

// NewMockIndexEngineWithStorage creates a new mock index engine with storage reference
func NewMockIndexEngineWithStorage(storage *MockStorageEngine) *MockIndexEngine {
	return &MockIndexEngine{
		indexes: make(map[string]map[string]bool),
		storage: storage,
	}
}

// CreateIndex creates an index on a field
func (m *MockIndexEngine) CreateIndex(collectionName, fieldName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.createCalls++

	// Prevent creating index on _id (it's automatically created)
	if fieldName == "_id" {
		return fmt.Errorf("cannot create index on _id field (automatically indexed)")
	}

	// Check if collection exists in storage if we have a reference
	if m.storage != nil {
		if m.storage.GetCollectionCount(collectionName) == 0 {
			return &CollectionNotFoundError{collectionName}
		}
	}

	// Initialize collection indexes if not exists
	if m.indexes[collectionName] == nil {
		m.indexes[collectionName] = make(map[string]bool)
	}

	// Check if index already exists
	if m.indexes[collectionName][fieldName] {
		return fmt.Errorf("index on field %s already exists in collection %s", fieldName, collectionName)
	}

	// Create the index
	m.indexes[collectionName][fieldName] = true
	return nil
}

// DropIndex removes an index
func (m *MockIndexEngine) DropIndex(collectionName, fieldName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.dropCalls++

	// Check if collection exists
	if m.indexes[collectionName] == nil {
		return &CollectionNotFoundError{collectionName}
	}

	// Check if index exists
	if !m.indexes[collectionName][fieldName] {
		return fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collectionName)
	}

	// Remove the index
	delete(m.indexes[collectionName], fieldName)
	return nil
}

// FindByIndex finds documents using an index
func (m *MockIndexEngine) FindByIndex(collectionName, fieldName string, value interface{}) ([]domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.findCalls++

	// Check if collection exists
	if m.indexes[collectionName] == nil {
		return nil, &CollectionNotFoundError{collectionName}
	}

	// Check if index exists
	if !m.indexes[collectionName][fieldName] {
		return nil, fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collectionName)
	}

	// Mock implementation - return empty results
	// In a real implementation, this would use the actual index to find documents
	return []domain.Document{}, nil
}

// GetIndexes returns all index names for a collection
func (m *MockIndexEngine) GetIndexes(collectionName string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.getCalls++

	// Check if collection exists
	if m.indexes[collectionName] == nil {
		return nil, &CollectionNotFoundError{collectionName}
	}

	// Get all index names for the collection
	var indexNames []string
	for fieldName := range m.indexes[collectionName] {
		indexNames = append(indexNames, fieldName)
	}

	// Always include _id as it's automatically indexed
	indexNames = append(indexNames, "_id")

	return indexNames, nil
}

// UpdateIndex rebuilds an index
func (m *MockIndexEngine) UpdateIndex(collectionName, fieldName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateCalls++

	// Check if collection exists
	if m.indexes[collectionName] == nil {
		return &CollectionNotFoundError{collectionName}
	}

	// Check if index exists
	if !m.indexes[collectionName][fieldName] {
		return fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collectionName)
	}

	// Mock implementation - just return success
	// In a real implementation, this would rebuild the index
	return nil
}

// GetCreateCalls returns the number of create index calls
func (m *MockIndexEngine) GetCreateCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.createCalls
}

// GetDropCalls returns the number of drop index calls
func (m *MockIndexEngine) GetDropCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dropCalls
}

// GetFindCalls returns the number of find by index calls
func (m *MockIndexEngine) GetFindCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.findCalls
}

// GetGetCalls returns the number of get indexes calls
func (m *MockIndexEngine) GetGetCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getCalls
}

// GetUpdateCalls returns the number of update index calls
func (m *MockIndexEngine) GetUpdateCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.updateCalls
}

// HasIndex checks if an index exists for a collection and field
func (m *MockIndexEngine) HasIndex(collectionName, fieldName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.indexes[collectionName] == nil {
		return false
	}

	return m.indexes[collectionName][fieldName]
}

// GetIndexCount returns the number of indexes for a collection
func (m *MockIndexEngine) GetIndexCount(collectionName string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.indexes[collectionName] == nil {
		return 0
	}

	return len(m.indexes[collectionName])
}
