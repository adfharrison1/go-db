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

// FindAll returns documents that match the given filter criteria
// If filter is nil or empty, returns all documents
func (m *MockStorageEngine) FindAll(collName string, filter map[string]interface{}) ([]domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.findCalls++

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	if len(filter) == 0 {
		return docs, nil
	}

	var results []domain.Document
	for _, doc := range docs {
		if matchesFilter(doc, filter) {
			results = append(results, doc)
		}
	}

	return results, nil
}

// FindAllStream streams documents from a collection
func (m *MockStorageEngine) FindAllStream(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
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

// GetById retrieves a document by ID
func (m *MockStorageEngine) GetById(collName, docId string) (domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	for _, doc := range docs {
		if id, exists := doc["_id"]; exists {
			// Convert ID to string for comparison
			var idStr string
			switch v := id.(type) {
			case string:
				idStr = v
			case int:
				idStr = fmt.Sprintf("%d", v)
			case float64:
				idStr = fmt.Sprintf("%.0f", v)
			case int64:
				idStr = fmt.Sprintf("%d", v)
			default:
				idStr = fmt.Sprintf("%v", v)
			}

			if idStr == docId {
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

	for i, doc := range docs {
		if id, exists := doc["_id"]; exists {
			// Convert ID to string for comparison
			var idStr string
			switch v := id.(type) {
			case string:
				idStr = v
			case int:
				idStr = fmt.Sprintf("%d", v)
			case float64:
				idStr = fmt.Sprintf("%.0f", v)
			case int64:
				idStr = fmt.Sprintf("%d", v)
			default:
				idStr = fmt.Sprintf("%v", v)
			}

			if idStr == docId {
				// Apply updates (excluding _id)
				for key, value := range updates {
					if key != "_id" {
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

	for i, doc := range docs {
		if id, exists := doc["_id"]; exists {
			// Convert ID to string for comparison
			var idStr string
			switch v := id.(type) {
			case string:
				idStr = v
			case int:
				idStr = fmt.Sprintf("%d", v)
			case float64:
				idStr = fmt.Sprintf("%.0f", v)
			case int64:
				idStr = fmt.Sprintf("%d", v)
			default:
				idStr = fmt.Sprintf("%v", v)
			}

			if idStr == docId {
				// Remove document from slice
				m.collections[collName] = append(docs[:i], docs[i+1:]...)
				return nil
			}
		}
	}

	return &DocumentNotFoundError{docId}
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
		"collections": len(m.collections),
		"documents":   m.getTotalDocumentCount(),
	}
}

// StartBackgroundWorkers starts background workers
func (m *MockStorageEngine) StartBackgroundWorkers() {
	// Mock implementation
}

// StopBackgroundWorkers stops background workers
func (m *MockStorageEngine) StopBackgroundWorkers() {
	// Mock implementation
}

// GetInsertCalls returns the number of insert calls
func (m *MockStorageEngine) GetInsertCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.insertCalls
}

// GetFindCalls returns the number of find calls
func (m *MockStorageEngine) GetFindCalls() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.findCalls
}

// GetStreamCalls returns the number of stream calls
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

// getTotalDocumentCount returns the total number of documents across all collections
func (m *MockStorageEngine) getTotalDocumentCount() int {
	total := 0
	for _, docs := range m.collections {
		total += len(docs)
	}
	return total
}

// CollectionNotFoundError represents a collection not found error
type CollectionNotFoundError struct {
	CollectionName string
}

func (e *CollectionNotFoundError) Error() string {
	return fmt.Sprintf("collection %s does not exist", e.CollectionName)
}

// CollectionExistsError represents a collection already exists error
type CollectionExistsError struct {
	CollectionName string
}

func (e *CollectionExistsError) Error() string {
	return fmt.Sprintf("collection %s already exists", e.CollectionName)
}

// DocumentNotFoundError represents a document not found error
type DocumentNotFoundError struct {
	DocID string
}

func (e *DocumentNotFoundError) Error() string {
	return fmt.Sprintf("document %s not found", e.DocID)
}

// matchesFilter checks if a document matches the given filter criteria
func matchesFilter(doc domain.Document, filter map[string]interface{}) bool {
	for key, expectedValue := range filter {
		actualValue, exists := doc[key]
		if !exists {
			return false
		}

		if !valuesMatch(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

// valuesMatch compares two values for equality, handling type conversions
func valuesMatch(actual, expected interface{}) bool {
	// Direct equality check
	if actual == expected {
		return true
	}

	// Handle numeric type conversions
	if actualFloat, ok := toFloat64(actual); ok {
		if expectedFloat, ok := toFloat64(expected); ok {
			return actualFloat == expectedFloat
		}
	}

	// Handle string case-insensitive comparison
	if actualStr, ok := actual.(string); ok {
		if expectedStr, ok := expected.(string); ok {
			return strings.EqualFold(actualStr, expectedStr)
		}
	}

	return false
}

// toFloat64 converts a value to float64 if possible
func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	default:
		return 0, false
	}
}
