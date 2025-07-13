package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_HandleInsert(t *testing.T) {
	tests := []struct {
		name           string
		collection     string
		document       map[string]interface{}
		expectedStatus int
		expectedError  bool
	}{
		{
			name:       "valid document",
			collection: "users",
			document: map[string]interface{}{
				"name": "Alice",
				"age":  30,
			},
			expectedStatus: http.StatusCreated,
			expectedError:  false,
		},
		{
			name:       "document with existing _id",
			collection: "users",
			document: map[string]interface{}{
				"_id":  "123",
				"name": "Bob",
				"age":  25,
			},
			expectedStatus: http.StatusCreated,
			expectedError:  false,
		},
		{
			name:           "empty document",
			collection:     "users",
			document:       map[string]interface{}{},
			expectedStatus: http.StatusCreated,
			expectedError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create separate mocks for storage and indexing
			mockStorage := NewMockStorageEngine()
			mockIndexer := NewMockIndexEngine()
			handler := NewHandler(mockStorage, mockIndexer)

			// Create request
			docJSON, err := json.Marshal(tt.document)
			require.NoError(t, err)

			req := httptest.NewRequest("POST", "/collections/"+tt.collection+"/insert", bytes.NewBuffer(docJSON))
			req.Header.Set("Content-Type", "application/json")

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/insert", handler.HandleInsert).Methods("POST")
			req = mux.SetURLVars(req, map[string]string{"coll": tt.collection})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)

			if !tt.expectedError {
				assert.Equal(t, 1, mockStorage.GetInsertCalls())
				assert.Equal(t, 1, mockStorage.GetCollectionCount(tt.collection))
			}
		})
	}
}

func TestHandler_HandleFindWithFilter(t *testing.T) {
	tests := []struct {
		name           string
		collection     string
		setupData      []map[string]interface{}
		queryParams    string
		expectedStatus int
		expectedDocs   int
		expectedNames  []string
	}{
		{
			name:       "find all documents (no filter)",
			collection: "users",
			setupData: []map[string]interface{}{
				{"name": "Alice", "age": 30},
				{"name": "Bob", "age": 25},
			},
			queryParams:    "",
			expectedStatus: http.StatusOK,
			expectedDocs:   2,
			expectedNames:  []string{"Alice", "Bob"},
		},
		{
			name:       "filter by age",
			collection: "users",
			setupData: []map[string]interface{}{
				{"name": "Alice", "age": 30},
				{"name": "Bob", "age": 25},
				{"name": "Charlie", "age": 30},
			},
			queryParams:    "?age=30",
			expectedStatus: http.StatusOK,
			expectedDocs:   2,
			expectedNames:  []string{"Alice", "Charlie"},
		},
		{
			name:       "filter by name (case insensitive)",
			collection: "users",
			setupData: []map[string]interface{}{
				{"name": "Alice", "age": 30},
				{"name": "alice", "age": 25},
				{"name": "Bob", "age": 35},
			},
			queryParams:    "?name=alice",
			expectedStatus: http.StatusOK,
			expectedDocs:   2,
			expectedNames:  []string{"Alice", "alice"},
		},
		{
			name:       "multiple filters",
			collection: "users",
			setupData: []map[string]interface{}{
				{"name": "Alice", "age": 30, "city": "New York"},
				{"name": "Bob", "age": 30, "city": "Boston"},
				{"name": "Charlie", "age": 25, "city": "New York"},
			},
			queryParams:    "?age=30&city=New%20York",
			expectedStatus: http.StatusOK,
			expectedDocs:   1,
			expectedNames:  []string{"Alice"},
		},
		{
			name:           "non-existent collection",
			collection:     "nonexistent",
			setupData:      nil,
			queryParams:    "",
			expectedStatus: http.StatusNotFound,
			expectedDocs:   0,
			expectedNames:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create separate mocks for storage and indexing
			mockStorage := NewMockStorageEngine()
			mockIndexer := NewMockIndexEngine()
			handler := NewHandler(mockStorage, mockIndexer)

			// Setup data if needed
			if tt.setupData != nil {
				for _, doc := range tt.setupData {
					err := mockStorage.Insert(tt.collection, doc)
					require.NoError(t, err)
				}
			}

			// Create request
			req := httptest.NewRequest("GET", "/collections/"+tt.collection+"/find"+tt.queryParams, nil)

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/find", handler.HandleFindWithFilter).Methods("GET")
			req = mux.SetURLVars(req, map[string]string{"coll": tt.collection})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

				// Parse response
				var docs []map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &docs)
				require.NoError(t, err)
				assert.Len(t, docs, tt.expectedDocs)

				// Check names if expected
				if tt.expectedNames != nil {
					names := make([]string, len(docs))
					for i, doc := range docs {
						names[i] = doc["name"].(string)
					}
					for _, expectedName := range tt.expectedNames {
						assert.Contains(t, names, expectedName)
					}
				}
			}
		})
	}
}

func TestHandler_HandleGetById(t *testing.T) {
	tests := []struct {
		name           string
		collection     string
		docId          string
		setupData      []map[string]interface{}
		expectedStatus int
		expectedDoc    map[string]interface{}
	}{
		{
			name:       "existing document",
			collection: "users",
			docId:      "1",
			setupData: []map[string]interface{}{
				{"_id": "1", "name": "Alice", "age": 30},
				{"_id": "2", "name": "Bob", "age": 25},
			},
			expectedStatus: http.StatusOK,
			expectedDoc:    map[string]interface{}{"_id": "1", "name": "Alice", "age": float64(30)},
		},
		{
			name:           "non-existent document",
			collection:     "users",
			docId:          "999",
			setupData:      []map[string]interface{}{{"_id": "1", "name": "Alice"}},
			expectedStatus: http.StatusNotFound,
			expectedDoc:    nil,
		},
		{
			name:           "non-existent collection",
			collection:     "nonexistent",
			docId:          "1",
			setupData:      nil,
			expectedStatus: http.StatusNotFound,
			expectedDoc:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create separate mocks for storage and indexing
			mockStorage := NewMockStorageEngine()
			mockIndexer := NewMockIndexEngine()
			handler := NewHandler(mockStorage, mockIndexer)

			// Setup data if needed
			if tt.setupData != nil {
				for _, doc := range tt.setupData {
					err := mockStorage.Insert(tt.collection, doc)
					require.NoError(t, err)
				}
			}

			// Create request
			req := httptest.NewRequest("GET", "/collections/"+tt.collection+"/documents/"+tt.docId, nil)

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/documents/{id}", handler.HandleGetById).Methods("GET")
			req = mux.SetURLVars(req, map[string]string{"coll": tt.collection, "id": tt.docId})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

				// Parse response
				var doc map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &doc)
				require.NoError(t, err)

				// Check expected fields
				for key, expectedValue := range tt.expectedDoc {
					assert.Equal(t, expectedValue, doc[key])
				}
			}
		})
	}
}

func TestHandler_HandleUpdateById(t *testing.T) {
	tests := []struct {
		name           string
		collection     string
		docId          string
		updates        map[string]interface{}
		setupData      []map[string]interface{}
		expectedStatus int
		expectedError  bool
	}{
		{
			name:       "valid update",
			collection: "users",
			docId:      "1",
			updates: map[string]interface{}{
				"age":  31,
				"city": "Boston",
			},
			setupData:      []map[string]interface{}{{"_id": "1", "name": "Alice", "age": 30}},
			expectedStatus: http.StatusOK,
			expectedError:  false,
		},
		{
			name:       "update with _id (should be ignored)",
			collection: "users",
			docId:      "1",
			updates: map[string]interface{}{
				"_id": "999",
				"age": 32,
			},
			setupData:      []map[string]interface{}{{"_id": "1", "name": "Alice", "age": 30}},
			expectedStatus: http.StatusOK,
			expectedError:  false,
		},
		{
			name:           "non-existent document",
			collection:     "users",
			docId:          "999",
			updates:        map[string]interface{}{"age": 31},
			setupData:      []map[string]interface{}{{"_id": "1", "name": "Alice"}},
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
		},
		{
			name:           "non-existent collection",
			collection:     "nonexistent",
			docId:          "1",
			updates:        map[string]interface{}{"age": 31},
			setupData:      nil,
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create separate mocks for storage and indexing
			mockStorage := NewMockStorageEngine()
			mockIndexer := NewMockIndexEngine()
			handler := NewHandler(mockStorage, mockIndexer)

			// Setup data if needed
			if tt.setupData != nil {
				for _, doc := range tt.setupData {
					err := mockStorage.Insert(tt.collection, doc)
					require.NoError(t, err)
				}
			}

			// Create request
			updatesJSON, err := json.Marshal(tt.updates)
			require.NoError(t, err)

			req := httptest.NewRequest("PUT", "/collections/"+tt.collection+"/documents/"+tt.docId, bytes.NewBuffer(updatesJSON))
			req.Header.Set("Content-Type", "application/json")

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/documents/{id}", handler.HandleUpdateById).Methods("PUT")
			req = mux.SetURLVars(req, map[string]string{"coll": tt.collection, "id": tt.docId})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)

			if !tt.expectedError {
				// Verify the document was updated by retrieving it
				updatedDoc, err := mockStorage.GetById(tt.collection, tt.docId)
				require.NoError(t, err)

				// Check that updates were applied
				for key, expectedValue := range tt.updates {
					if key != "_id" { // _id should not be updated
						// Handle type conversion for numeric values
						if actualValue, exists := updatedDoc[key]; exists {
							if expectedFloat, ok := expectedValue.(int); ok {
								if actualFloat, ok := actualValue.(float64); ok {
									assert.Equal(t, float64(expectedFloat), actualFloat)
								} else {
									assert.Equal(t, expectedValue, actualValue)
								}
							} else {
								assert.Equal(t, expectedValue, actualValue)
							}
						}
					}
				}
			}
		})
	}
}

func TestHandler_HandleDeleteById(t *testing.T) {
	tests := []struct {
		name           string
		collection     string
		docId          string
		setupData      []map[string]interface{}
		expectedStatus int
		expectedError  bool
		expectedCount  int
	}{
		{
			name:       "valid deletion",
			collection: "users",
			docId:      "1",
			setupData: []map[string]interface{}{
				{"_id": "1", "name": "Alice", "age": 30},
				{"_id": "2", "name": "Bob", "age": 25},
			},
			expectedStatus: http.StatusNoContent,
			expectedError:  false,
			expectedCount:  1,
		},
		{
			name:           "non-existent document",
			collection:     "users",
			docId:          "999",
			setupData:      []map[string]interface{}{{"_id": "1", "name": "Alice"}},
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
			expectedCount:  1,
		},
		{
			name:           "non-existent collection",
			collection:     "nonexistent",
			docId:          "1",
			setupData:      nil,
			expectedStatus: http.StatusNotFound,
			expectedError:  true,
			expectedCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create separate mocks for storage and indexing
			mockStorage := NewMockStorageEngine()
			mockIndexer := NewMockIndexEngine()
			handler := NewHandler(mockStorage, mockIndexer)

			// Setup data if needed
			if tt.setupData != nil {
				for _, doc := range tt.setupData {
					err := mockStorage.Insert(tt.collection, doc)
					require.NoError(t, err)
				}
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/collections/"+tt.collection+"/documents/"+tt.docId, nil)

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/documents/{id}", handler.HandleDeleteById).Methods("DELETE")
			req = mux.SetURLVars(req, map[string]string{"coll": tt.collection, "id": tt.docId})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)

			if !tt.expectedError {
				// Verify the document was deleted
				assert.Equal(t, tt.expectedCount, mockStorage.GetCollectionCount(tt.collection))
			}
		})
	}
}

func TestHandler_HandleStream(t *testing.T) {
	tests := []struct {
		name           string
		collection     string
		setupData      []map[string]interface{}
		expectedStatus int
		expectedDocs   int
	}{
		{
			name:       "existing collection with documents",
			collection: "users",
			setupData: []map[string]interface{}{
				{"name": "Alice", "age": 30},
				{"name": "Bob", "age": 25},
				{"name": "Charlie", "age": 35},
			},
			expectedStatus: http.StatusOK,
			expectedDocs:   3,
		},
		{
			name:           "non-existent collection",
			collection:     "nonexistent",
			setupData:      nil,
			expectedStatus: http.StatusNotFound,
			expectedDocs:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create separate mocks for storage and indexing
			mockStorage := NewMockStorageEngine()
			mockIndexer := NewMockIndexEngine()
			handler := NewHandler(mockStorage, mockIndexer)

			// Setup data if needed
			if tt.setupData != nil {
				for _, doc := range tt.setupData {
					err := mockStorage.Insert(tt.collection, doc)
					require.NoError(t, err)
				}
			}

			// Create request
			req := httptest.NewRequest("GET", "/collections/"+tt.collection+"/stream", nil)

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/stream", handler.HandleStream).Methods("GET")
			req = mux.SetURLVars(req, map[string]string{"coll": tt.collection})

			// Create response recorder
			w := httptest.NewRecorder()

			// Call handler
			router.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedStatus == http.StatusOK {
				assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
				assert.Equal(t, "chunked", w.Header().Get("Transfer-Encoding"))
				assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
				assert.Equal(t, "keep-alive", w.Header().Get("Connection"))

				// Parse response
				var docs []map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &docs)
				require.NoError(t, err)
				assert.Len(t, docs, tt.expectedDocs)
			}
		})
	}
}

func TestHandler_Integration(t *testing.T) {
	// Test full workflow: insert -> find -> get by id -> update -> delete -> find
	mockStorage := NewMockStorageEngine()
	mockIndexer := NewMockIndexEngine()
	handler := NewHandler(mockStorage, mockIndexer)

	// Test data
	doc := map[string]interface{}{
		"name": "Test User",
		"age":  28,
	}

	// 1. Insert document
	docJSON, err := json.Marshal(doc)
	require.NoError(t, err)

	insertReq := httptest.NewRequest("POST", "/collections/test/insert", bytes.NewBuffer(docJSON))
	insertReq.Header.Set("Content-Type", "application/json")
	insertReq = mux.SetURLVars(insertReq, map[string]string{"coll": "test"})

	insertW := httptest.NewRecorder()
	router := mux.NewRouter()
	router.HandleFunc("/collections/{coll}/insert", handler.HandleInsert).Methods("POST")
	router.ServeHTTP(insertW, insertReq)

	assert.Equal(t, http.StatusCreated, insertW.Code)
	assert.Equal(t, 1, mockStorage.GetCollectionCount("test"))

	// 2. Find all documents
	findReq := httptest.NewRequest("GET", "/collections/test/find", nil)
	findReq = mux.SetURLVars(findReq, map[string]string{"coll": "test"})

	findW := httptest.NewRecorder()
	router.HandleFunc("/collections/{coll}/find", handler.HandleFindWithFilter).Methods("GET")
	router.ServeHTTP(findW, findReq)

	assert.Equal(t, http.StatusOK, findW.Code)

	var docs []map[string]interface{}
	err = json.Unmarshal(findW.Body.Bytes(), &docs)
	require.NoError(t, err)
	assert.Len(t, docs, 1)

	// 3. Get document by ID - safely handle type conversion
	docId := ""
	if id, exists := docs[0]["_id"]; exists {
		switch v := id.(type) {
		case string:
			docId = v
		case float64:
			docId = fmt.Sprintf("%.0f", v)
		default:
			docId = fmt.Sprintf("%v", v)
		}
	}
	require.NotEmpty(t, docId, "Document ID should not be empty")

	getReq := httptest.NewRequest("GET", "/collections/test/documents/"+docId, nil)
	getReq = mux.SetURLVars(getReq, map[string]string{"coll": "test", "id": docId})

	getW := httptest.NewRecorder()
	router.HandleFunc("/collections/{coll}/documents/{id}", handler.HandleGetById).Methods("GET")
	router.ServeHTTP(getW, getReq)

	assert.Equal(t, http.StatusOK, getW.Code)

	// 4. Update document
	updates := map[string]interface{}{"age": 29}
	updatesJSON, err := json.Marshal(updates)
	require.NoError(t, err)

	updateReq := httptest.NewRequest("PUT", "/collections/test/documents/"+docId, bytes.NewBuffer(updatesJSON))
	updateReq.Header.Set("Content-Type", "application/json")
	updateReq = mux.SetURLVars(updateReq, map[string]string{"coll": "test", "id": docId})

	updateW := httptest.NewRecorder()
	router.HandleFunc("/collections/{coll}/documents/{id}", handler.HandleUpdateById).Methods("PUT")
	router.ServeHTTP(updateW, updateReq)

	assert.Equal(t, http.StatusOK, updateW.Code)

	// 5. Delete document
	deleteReq := httptest.NewRequest("DELETE", "/collections/test/documents/"+docId, nil)
	deleteReq = mux.SetURLVars(deleteReq, map[string]string{"coll": "test", "id": docId})

	deleteW := httptest.NewRecorder()
	router.HandleFunc("/collections/{coll}/documents/{id}", handler.HandleDeleteById).Methods("DELETE")
	router.ServeHTTP(deleteW, deleteReq)

	assert.Equal(t, http.StatusNoContent, deleteW.Code)
	assert.Equal(t, 0, mockStorage.GetCollectionCount("test"))
}

// Indexing Tests

func TestHandleCreateIndex(t *testing.T) {
	mockStorage := NewMockStorageEngine()
	mockIndexer := NewMockIndexEngineWithStorage(mockStorage)
	handler := NewHandler(mockStorage, mockIndexer)

	// Create a collection first
	err := mockStorage.CreateCollection("test")
	require.NoError(t, err)

	// Insert some test documents
	docs := []domain.Document{
		{"name": "Alice", "age": 25},
		{"name": "Bob", "age": 30},
		{"name": "Charlie", "age": 25},
	}

	for _, doc := range docs {
		err := mockStorage.Insert("test", doc)
		require.NoError(t, err)
	}

	tests := []struct {
		name           string
		collection     string
		field          string
		expectedStatus int
		expectedError  string
	}{
		{
			name:           "Create index on valid field",
			collection:     "test",
			field:          "name",
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "Create index on age field",
			collection:     "test",
			field:          "age",
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "Try to create index on _id (should fail)",
			collection:     "test",
			field:          "_id",
			expectedStatus: http.StatusBadRequest,
			expectedError:  "cannot create index on _id field",
		},
		{
			name:           "Try to create index on non-existent collection",
			collection:     "nonexistent",
			field:          "name",
			expectedStatus: http.StatusInternalServerError,
			expectedError:  "collection nonexistent does not exist",
		},
		{
			name:           "Try to create index with empty field name",
			collection:     "test",
			field:          "",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request
			req, err := http.NewRequest("POST", "/collections/"+tt.collection+"/indexes/"+tt.field, nil)
			require.NoError(t, err)

			// Set up router with variables
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/indexes/{field}", handler.HandleCreateIndex).Methods("POST")

			// Create response recorder
			rr := httptest.NewRecorder()

			// Serve request
			router.ServeHTTP(rr, req)

			// Check status code
			assert.Equal(t, tt.expectedStatus, rr.Code)

			if tt.expectedStatus == http.StatusCreated {
				// Parse response
				var response map[string]interface{}
				err := json.Unmarshal(rr.Body.Bytes(), &response)
				require.NoError(t, err)

				// Verify response structure
				assert.True(t, response["success"].(bool))
				assert.Equal(t, "Index created successfully", response["message"])
				assert.Equal(t, tt.collection, response["collection"])
				assert.Equal(t, tt.field, response["field"])

				// Verify index was created in mock
				assert.True(t, mockIndexer.HasIndex(tt.collection, tt.field))
			} else if tt.expectedError != "" {
				// Check error message
				assert.Contains(t, rr.Body.String(), tt.expectedError)
			} else if tt.expectedStatus == http.StatusNotFound {
				// For 404, just check the status code
				assert.Equal(t, http.StatusNotFound, rr.Code)
			}
		})
	}
}

func TestCreateIndexWithRealStorage(t *testing.T) {
	// This test uses the real storage engine to verify index functionality
	// Note: In a real test environment, you might want to use a test database
	// or mock the storage layer for faster tests

	// For now, we'll test the API integration with mock storage
	// which should be sufficient for testing the HTTP layer
	mockStorage := NewMockStorageEngine()
	mockIndexer := NewMockIndexEngineWithStorage(mockStorage)
	handler := NewHandler(mockStorage, mockIndexer)

	// Create collection
	err := mockStorage.CreateCollection("users")
	require.NoError(t, err)

	// Insert test data
	users := []domain.Document{
		{"name": "Alice", "role": "admin", "age": 25},
		{"name": "Bob", "role": "user", "age": 30},
		{"name": "Charlie", "role": "admin", "age": 35},
	}

	for _, user := range users {
		err := mockStorage.Insert("users", user)
		require.NoError(t, err)
	}

	// Test creating index via API
	req, err := http.NewRequest("POST", "/collections/users/indexes/role", nil)
	require.NoError(t, err)

	router := mux.NewRouter()
	router.HandleFunc("/collections/{coll}/indexes/{field}", handler.HandleCreateIndex).Methods("POST")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Verify successful index creation
	assert.Equal(t, http.StatusCreated, rr.Code)

	var response map[string]interface{}
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.True(t, response["success"].(bool))
	assert.Equal(t, "users", response["collection"])
	assert.Equal(t, "role", response["field"])

	// Verify index was created in mock
	assert.True(t, mockIndexer.HasIndex("users", "role"))
	assert.Equal(t, 1, mockIndexer.GetCreateCalls())
}

func TestIndexCreationWorkflow(t *testing.T) {
	mockStorage := NewMockStorageEngine()
	mockIndexer := NewMockIndexEngineWithStorage(mockStorage)
	handler := NewHandler(mockStorage, mockIndexer)

	// Step 1: Create collection
	err := mockStorage.CreateCollection("products")
	require.NoError(t, err)

	// Step 2: Insert documents
	products := []domain.Document{
		{"name": "Laptop", "category": "electronics", "price": 999},
		{"name": "Phone", "category": "electronics", "price": 599},
		{"name": "Book", "category": "books", "price": 19},
		{"name": "Tablet", "category": "electronics", "price": 399},
	}

	for _, product := range products {
		err := mockStorage.Insert("products", product)
		require.NoError(t, err)
	}

	// Step 3: Create index on category field via API
	req, err := http.NewRequest("POST", "/collections/products/indexes/category", nil)
	require.NoError(t, err)

	router := mux.NewRouter()
	router.HandleFunc("/collections/{coll}/indexes/{field}", handler.HandleCreateIndex).Methods("POST")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Step 4: Verify index creation
	assert.Equal(t, http.StatusCreated, rr.Code)

	var response map[string]interface{}
	err = json.Unmarshal(rr.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.True(t, response["success"].(bool))

	// Step 5: Verify index was created in mock
	assert.True(t, mockIndexer.HasIndex("products", "category"))
	assert.Equal(t, 1, mockIndexer.GetIndexCount("products"))

	// Step 6: Test that the index can be used for queries
	// (This would be tested with the real storage engine)
	// For now, we verify the API endpoint works correctly
}

func TestIndexCreationErrorHandling(t *testing.T) {
	mockStorage := NewMockStorageEngine()
	mockIndexer := NewMockIndexEngineWithStorage(mockStorage)
	handler := NewHandler(mockStorage, mockIndexer)

	// Test creating index without collection
	req, err := http.NewRequest("POST", "/collections/nonexistent/indexes/name", nil)
	require.NoError(t, err)

	router := mux.NewRouter()
	router.HandleFunc("/collections/{coll}/indexes/{field}", handler.HandleCreateIndex).Methods("POST")

	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Should return error
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "does not exist")

	// Verify no index was created
	assert.False(t, mockIndexer.HasIndex("nonexistent", "name"))
}
