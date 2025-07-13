package api

import (
	"fmt"
	"strings"
	"sync"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// MockStorageEngine provides a mock implementation of domain.StorageEngine for testing
type MockStorageEngine struct {
	mu          sync.RWMutex
	collections map[string][]domain.Document
	insertCalls int
	findCalls   int
	streamCalls int
}

// NewMockStorageEngine creates a new mock storage engine
func NewMockStorageEngine() *MockStorageEngine {
	return &MockStorageEngine{
		collections: make(map[string][]domain.Document),
	}
}

// Insert adds a document to a collection
func (m *MockStorageEngine) Insert(collName string, doc domain.Document) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.insertCalls++

	if m.collections[collName] == nil {
		m.collections[collName] = make([]domain.Document, 0)
	}

	// Add ID if not present - use string IDs consistently
	if _, exists := doc["_id"]; !exists {
		doc["_id"] = fmt.Sprintf("%d", len(m.collections[collName])+1)
	} else {
		// Convert numeric IDs to strings for consistency
		if id, ok := doc["_id"]; ok {
			switch v := id.(type) {
			case int:
				doc["_id"] = fmt.Sprintf("%d", v)
			case float64:
				doc["_id"] = fmt.Sprintf("%.0f", v)
			case int64:
				doc["_id"] = fmt.Sprintf("%d", v)
			}
		}
	}

	m.collections[collName] = append(m.collections[collName], doc)
	return nil
}

// FindAll returns all documents in a collection
func (m *MockStorageEngine) FindAll(collName string) ([]domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.findCalls++

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	return docs, nil
}

// FindAllStream streams documents from a collection
func (m *MockStorageEngine) FindAllStream(collName string) (<-chan domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.streamCalls++

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	// Create channel and stream documents
	docChan := make(chan domain.Document, 100)

	go func() {
		defer close(docChan)
		for _, doc := range docs {
			docChan <- doc
		}
	}()

	return docChan, nil
}

// CreateCollection creates a new collection
func (m *MockStorageEngine) CreateCollection(collName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.collections[collName] != nil {
		return &CollectionExistsError{collName}
	}

	m.collections[collName] = make([]domain.Document, 0)
	return nil
}

// GetCollection returns a collection (not used in current API)
func (m *MockStorageEngine) GetCollection(collName string) (*domain.Collection, error) {
	return nil, nil
}

// LoadCollectionMetadata loads collection metadata
func (m *MockStorageEngine) LoadCollectionMetadata(filename string) error {
	return nil
}

// SaveToFile saves the database to file
func (m *MockStorageEngine) SaveToFile(filename string) error {
	return nil
}

// GetMemoryStats returns memory statistics
func (m *MockStorageEngine) GetMemoryStats() map[string]interface{} {
	return map[string]interface{}{
		"alloc_mb":       0,
		"total_alloc_mb": 0,
		"sys_mb":         0,
		"num_goroutines": 0,
		"cache_size":     0,
		"collections":    len(m.collections),
	}
}

// StartBackgroundWorkers starts background workers
func (m *MockStorageEngine) StartBackgroundWorkers() {
	// No-op for mock
}

// StopBackgroundWorkers stops background workers
func (m *MockStorageEngine) StopBackgroundWorkers() {
	// No-op for mock
}

// GetInsertCalls returns the number of Insert calls made
func (m *MockStorageEngine) GetInsertCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.insertCalls
}

// GetFindCalls returns the number of FindAll calls made
func (m *MockStorageEngine) GetFindCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.findCalls
}

// GetStreamCalls returns the number of FindAllStream calls made
func (m *MockStorageEngine) GetStreamCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streamCalls
}

// GetCollectionCount returns the number of documents in a collection
func (m *MockStorageEngine) GetCollectionCount(collName string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	docs, exists := m.collections[collName]
	if !exists {
		return 0
	}
	return len(docs)
}

// Custom error types for better error handling
type CollectionNotFoundError struct {
	CollectionName string
}

func (e *CollectionNotFoundError) Error() string {
	return "collection " + e.CollectionName + " does not exist"
}

type CollectionExistsError struct {
	CollectionName string
}

func (e *CollectionExistsError) Error() string {
	return "collection " + e.CollectionName + " already exists"
}

// GetById returns a document by ID
func (m *MockStorageEngine) GetById(collName, docId string) (domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	// Find document by ID
	for _, doc := range docs {
		if id, ok := doc["_id"]; ok {
			if idStr, ok := id.(string); ok && idStr == docId {
				return doc, nil
			}
		}
	}

	return nil, &DocumentNotFoundError{docId}
}

// UpdateById updates a document by ID
func (m *MockStorageEngine) UpdateById(collName, docId string, updates domain.Document) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	docs, exists := m.collections[collName]
	if !exists {
		return &CollectionNotFoundError{collName}
	}

	// Find and update document by ID
	for i, doc := range docs {
		if id, ok := doc["_id"]; ok {
			if idStr, ok := id.(string); ok && idStr == docId {
				// Apply updates
				for key, value := range updates {
					if key != "_id" { // Prevent updating ID
						docs[i][key] = value
					}
				}
				return nil
			}
		}
	}

	return &DocumentNotFoundError{docId}
}

// DeleteById removes a document by ID
func (m *MockStorageEngine) DeleteById(collName, docId string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	docs, exists := m.collections[collName]
	if !exists {
		return &CollectionNotFoundError{collName}
	}

	// Find and remove document by ID
	for i, doc := range docs {
		if id, ok := doc["_id"]; ok {
			if idStr, ok := id.(string); ok && idStr == docId {
				// Remove document
				m.collections[collName] = append(docs[:i], docs[i+1:]...)
				return nil
			}
		}
	}

	return &DocumentNotFoundError{docId}
}

// FindAllWithFilter returns documents matching filter criteria
func (m *MockStorageEngine) FindAllWithFilter(collName string, filter map[string]interface{}) ([]domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	var results []domain.Document
	for _, doc := range docs {
		if matchesFilter(doc, filter) {
			results = append(results, doc)
		}
	}

	return results, nil
}

// matchesFilter checks if a document matches filter criteria
func matchesFilter(doc domain.Document, filter map[string]interface{}) bool {
	for field, expectedValue := range filter {
		actualValue, exists := doc[field]
		if !exists {
			return false
		}

		if !valuesMatch(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

// valuesMatch compares two values for equality, handling different types
func valuesMatch(actual, expected interface{}) bool {
	// Handle nil values
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	// Handle string comparison (case-insensitive for better UX)
	if actualStr, ok1 := actual.(string); ok1 {
		if expectedStr, ok2 := expected.(string); ok2 {
			return strings.EqualFold(actualStr, expectedStr)
		}
	}

	// Handle numeric comparison
	if actualNum, ok1 := toFloat64(actual); ok1 {
		if expectedNum, ok2 := toFloat64(expected); ok2 {
			return actualNum == expectedNum
		}
	}

	// Default to direct comparison
	return actual == expected
}

// toFloat64 converts various numeric types to float64 for comparison
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

// DocumentNotFoundError represents a document not found error
type DocumentNotFoundError struct {
	DocID string
}

func (e *DocumentNotFoundError) Error() string {
	return "document not found: " + e.DocID
}
