package api

import (
	"bytes"
	"encoding/json"
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

func TestHandler_HandleFind(t *testing.T) {
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
			},
			expectedStatus: http.StatusOK,
			expectedDocs:   2,
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
			req := httptest.NewRequest("GET", "/collections/"+tt.collection+"/find", nil)

			// Set up router with vars
			router := mux.NewRouter()
			router.HandleFunc("/collections/{coll}/find", handler.HandleFind).Methods("GET")
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
	// Test full workflow: insert -> find -> stream
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
	assert.Equal(t, 1, mockStorage.GetInsertCalls())

	// 2. Find documents
	findReq := httptest.NewRequest("GET", "/collections/test/find", nil)
	findReq = mux.SetURLVars(findReq, map[string]string{"coll": "test"})

	findW := httptest.NewRecorder()
	router.HandleFunc("/collections/{coll}/find", handler.HandleFind).Methods("GET")
	router.ServeHTTP(findW, findReq)

	assert.Equal(t, http.StatusOK, findW.Code)
	assert.Equal(t, 1, mockStorage.GetFindCalls())

	// 3. Stream documents
	streamReq := httptest.NewRequest("GET", "/collections/test/stream", nil)
	streamReq = mux.SetURLVars(streamReq, map[string]string{"coll": "test"})

	streamW := httptest.NewRecorder()
	router.HandleFunc("/collections/{coll}/stream", handler.HandleStream).Methods("GET")
	router.ServeHTTP(streamW, streamReq)

	assert.Equal(t, http.StatusOK, streamW.Code)
	assert.Equal(t, 1, mockStorage.GetStreamCalls())

	// Verify all responses contain the same data
	var findDocs, streamDocs []map[string]interface{}
	err = json.Unmarshal(findW.Body.Bytes(), &findDocs)
	require.NoError(t, err)
	err = json.Unmarshal(streamW.Body.Bytes(), &streamDocs)
	require.NoError(t, err)

	assert.Len(t, findDocs, 1)
	assert.Len(t, streamDocs, 1)
	assert.Equal(t, findDocs[0]["name"], streamDocs[0]["name"])
	assert.Equal(t, findDocs[0]["age"], streamDocs[0]["age"])
}
