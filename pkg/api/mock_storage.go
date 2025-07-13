package api

import (
	"fmt"
	"sort"
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
func (m *MockStorageEngine) FindAll(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.findCalls++

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	// Apply filter first
	var filteredDocs []domain.Document
	if len(filter) == 0 {
		filteredDocs = docs
	} else {
		for _, doc := range docs {
			if matchesFilter(doc, filter) {
				filteredDocs = append(filteredDocs, doc)
			}
		}
	}

	// Sort by ID for consistent ordering
	sort.Slice(filteredDocs, func(i, j int) bool {
		idI, _ := filteredDocs[i]["_id"].(string)
		idJ, _ := filteredDocs[j]["_id"].(string)
		return idI < idJ
	})

	// Handle cursor-based pagination
	if options != nil && (options.After != "" || options.Before != "") {
		return m.applyCursorPagination(filteredDocs, options)
	}

	// Handle offset-based pagination
	return m.applyOffsetPagination(filteredDocs, options)
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

	out := make(chan domain.Document, 100)

	go func() {
		defer close(out)

		if len(filter) == 0 {
			for _, doc := range docs {
				out <- doc
			}
		} else {
			for _, doc := range docs {
				if matchesFilter(doc, filter) {
					out <- doc
				}
			}
		}
	}()

	return out, nil
}

// FindAllStreamWithPagination streams paginated documents from a collection
func (m *MockStorageEngine) FindAllStreamWithPagination(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (<-chan domain.Document, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.streamCalls++

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	// Apply filter first
	var filteredDocs []domain.Document
	if len(filter) == 0 {
		filteredDocs = docs
	} else {
		for _, doc := range docs {
			if matchesFilter(doc, filter) {
				filteredDocs = append(filteredDocs, doc)
			}
		}
	}

	// Sort by ID for consistent ordering
	sort.Slice(filteredDocs, func(i, j int) bool {
		idI, _ := filteredDocs[i]["_id"].(string)
		idJ, _ := filteredDocs[j]["_id"].(string)
		return idI < idJ
	})

	// Apply pagination
	var paginatedDocs []domain.Document
	if options != nil && (options.After != "" || options.Before != "") {
		result, err := m.applyCursorPagination(filteredDocs, options)
		if err != nil {
			return nil, err
		}
		paginatedDocs = result.Documents
	} else {
		result, err := m.applyOffsetPagination(filteredDocs, options)
		if err != nil {
			return nil, err
		}
		paginatedDocs = result.Documents
	}

	out := make(chan domain.Document, 100)

	go func() {
		defer close(out)
		for _, doc := range paginatedDocs {
			out <- doc
		}
	}()

	return out, nil
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

// GetCollection returns a collection by name
func (m *MockStorageEngine) GetCollection(collName string) (*domain.Collection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	docs, exists := m.collections[collName]
	if !exists {
		return nil, &CollectionNotFoundError{collName}
	}

	collection := domain.NewCollection(collName)
	for _, doc := range docs {
		// Convert slice index to string ID for consistency
		docCopy := make(domain.Document)
		for k, v := range doc {
			docCopy[k] = v
		}
		if id, exists := docCopy["_id"]; exists {
			collection.Documents[id.(string)] = docCopy
		}
	}

	return collection, nil
}

// LoadCollectionMetadata loads collection metadata (mock implementation)
func (m *MockStorageEngine) LoadCollectionMetadata(filename string) error {
	// Mock implementation - do nothing
	return nil
}

// SaveToFile saves collections to file (mock implementation)
func (m *MockStorageEngine) SaveToFile(filename string) error {
	// Mock implementation - do nothing
	return nil
}

// GetMemoryStats returns memory statistics (mock implementation)
func (m *MockStorageEngine) GetMemoryStats() map[string]interface{} {
	return map[string]interface{}{
		"alloc_mb":       uint64(0),
		"total_alloc_mb": uint64(0),
		"sys_mb":         uint64(0),
		"num_goroutines": 0,
		"cache_size":     0,
		"collections":    len(m.collections),
	}
}

// StartBackgroundWorkers starts background workers (mock implementation)
func (m *MockStorageEngine) StartBackgroundWorkers() {
	// Mock implementation - do nothing
}

// StopBackgroundWorkers stops background workers (mock implementation)
func (m *MockStorageEngine) StopBackgroundWorkers() {
	// Mock implementation - do nothing
}

// Helper function to check if a document matches a filter
func matchesFilter(doc domain.Document, filter map[string]interface{}) bool {
	for key, expectedValue := range filter {
		docValue, exists := doc[key]
		if !exists {
			return false
		}

		// Simple string comparison for now
		docStr := fmt.Sprintf("%v", docValue)
		expectedStr := fmt.Sprintf("%v", expectedValue)
		if docStr != expectedStr {
			return false
		}
	}
	return true
}

// Helper for cursor-based pagination
func (m *MockStorageEngine) applyCursorPagination(docs []domain.Document, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	result := &domain.PaginationResult{
		Documents: []domain.Document{},
		HasNext:   false,
		HasPrev:   false,
		Total:     int64(len(docs)),
	}

	startIndex := 0
	endIndex := len(docs)

	if options.After != "" {
		cursor, err := domain.DecodeCursor(options.After)
		if err != nil {
			return nil, fmt.Errorf("invalid after cursor: %w", err)
		}
		for i, doc := range docs {
			if docID, _ := doc["_id"].(string); docID == cursor.ID {
				startIndex = i + 1
				break
			}
		}
	}

	if options.Before != "" {
		cursor, err := domain.DecodeCursor(options.Before)
		if err != nil {
			return nil, fmt.Errorf("invalid before cursor: %w", err)
		}
		for i, doc := range docs {
			if docID, _ := doc["_id"].(string); docID == cursor.ID {
				endIndex = i
				break
			}
		}
	}

	limit := options.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > options.MaxLimit {
		limit = options.MaxLimit
	}

	if startIndex+limit < endIndex {
		endIndex = startIndex + limit
		result.HasNext = true
	}
	if startIndex > 0 {
		result.HasPrev = true
	}
	if startIndex < len(docs) {
		result.Documents = docs[startIndex:endIndex]
	}
	return result, nil
}

// Helper for offset-based pagination
func (m *MockStorageEngine) applyOffsetPagination(docs []domain.Document, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	result := &domain.PaginationResult{
		Documents: []domain.Document{},
		HasNext:   false,
		HasPrev:   false,
		Total:     int64(len(docs)),
	}
	offset := options.Offset
	limit := options.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > options.MaxLimit {
		limit = options.MaxLimit
	}
	startIndex := offset
	endIndex := offset + limit
	if startIndex >= len(docs) {
		return result, nil
	}
	if endIndex > len(docs) {
		endIndex = len(docs)
	} else {
		result.HasNext = true
	}
	if offset > 0 {
		result.HasPrev = true
	}
	result.Documents = docs[startIndex:endIndex]
	return result, nil
}

// CollectionNotFoundError represents a collection not found error
type CollectionNotFoundError struct {
	CollectionName string
}

func (e *CollectionNotFoundError) Error() string {
	return fmt.Sprintf("collection '%s' not found", e.CollectionName)
}

// CollectionExistsError represents a collection already exists error
type CollectionExistsError struct {
	CollectionName string
}

func (e *CollectionExistsError) Error() string {
	return fmt.Sprintf("collection '%s' already exists", e.CollectionName)
}

// DocumentNotFoundError represents a document not found error
type DocumentNotFoundError struct {
	DocumentID string
}

func (e *DocumentNotFoundError) Error() string {
	return fmt.Sprintf("document with id '%s' not found", e.DocumentID)
}
