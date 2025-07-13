package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

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
			name:       "document with existing ID",
			collection: "users",
			document: map[string]interface{}{
				"_id":  "123",
				"name": "Bob",
				"age":  25,
			},
			expectedStatus: http.StatusCreated,
			expectedError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock storage
			mockStorage := NewMockStorageEngine()
			handler := NewHandler(mockStorage)

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
			// Create mock storage
			mockStorage := NewMockStorageEngine()
			handler := NewHandler(mockStorage)

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
			// Create mock storage
			mockStorage := NewMockStorageEngine()
			handler := NewHandler(mockStorage)

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
			// Create mock storage
			mockStorage := NewMockStorageEngine()
			handler := NewHandler(mockStorage)

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
			// Create mock storage
			mockStorage := NewMockStorageEngine()
			handler := NewHandler(mockStorage)

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
			// Create mock storage
			mockStorage := NewMockStorageEngine()
			handler := NewHandler(mockStorage)

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
	handler := NewHandler(mockStorage)

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

	// 6. Verify deletion with find
	finalFindReq := httptest.NewRequest("GET", "/collections/test/find", nil)
	finalFindReq = mux.SetURLVars(finalFindReq, map[string]string{"coll": "test"})

	finalFindW := httptest.NewRecorder()
	router.ServeHTTP(finalFindW, finalFindReq)

	assert.Equal(t, http.StatusOK, finalFindW.Code)

	var finalDocs []map[string]interface{}
	err = json.Unmarshal(finalFindW.Body.Bytes(), &finalDocs)
	require.NoError(t, err)
	assert.Len(t, finalDocs, 0)
}
