package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adfharrison1/go-db/pkg/storage"
)

// TestModeIntegration runs comprehensive integration tests for all operation modes
func TestModeIntegration(t *testing.T) {
	modes := []struct {
		name string
		mode storage.OperationMode
	}{
		{"dual-write", storage.ModeDualWrite},
		{"no-saves", storage.ModeNoSaves},
		{"memory-map", storage.ModeMemoryMap},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			// Create a sub-test for each mode
			runModeIntegrationTests(t, mode.mode)
		})
	}
}

// runModeIntegrationTests runs all integration tests for a specific operation mode
func runModeIntegrationTests(t *testing.T, mode storage.OperationMode) {
	// Create test server with specific mode
	ts := NewTestServerWithMode(t, mode)
	defer ts.Close(t)

	// Verify the mode is set correctly
	assert.Equal(t, mode, ts.Storage.GetOperationMode())

	// Run all integration test scenarios
	t.Run("BasicCRUD", func(t *testing.T) {
		testBasicCRUD(t, ts)
	})

	t.Run("BatchOperations", func(t *testing.T) {
		testBatchOperations(t, ts)
	})

	t.Run("IndexOperations", func(t *testing.T) {
		testIndexOperations(t, ts)
	})

	t.Run("FindOperations", func(t *testing.T) {
		testFindOperations(t, ts)
	})

	t.Run("StreamingOperations", func(t *testing.T) {
		testStreamingOperations(t, ts)
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		testConcurrentOperations(t, ts)
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		testErrorHandling(t, ts)
	})

	t.Run("DataPersistence", func(t *testing.T) {
		testDataPersistence(t, ts)
	})
}

// NewTestServerWithMode creates a test server with a specific operation mode
func NewTestServerWithMode(t *testing.T, mode storage.OperationMode) *TestServer {
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("go-db-api-test-%s-*", mode.String()))
	require.NoError(t, err)

	// Create storage engine with specific mode
	storageEngine := storage.NewStorageEngine(
		storage.WithDataDir(tempDir),
		storage.WithOperationMode(mode),
	)

	indexEngine := storageEngine.GetIndexEngine()
	handler := NewHandler(storageEngine, indexEngine)

	// Create router and register routes
	router := mux.NewRouter()
	handler.RegisterRoutes(router)

	// Create test server
	server := httptest.NewServer(router)

	return &TestServer{
		Server:  server,
		TempDir: tempDir,
		Storage: storageEngine,
		Handler: handler,
		BaseURL: server.URL,
	}
}

// POSTRaw sends a raw POST request with the given body
func (ts *TestServer) POSTRaw(path string, body string) (*http.Response, error) {
	req, err := http.NewRequest("POST", ts.BaseURL+path, bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

// testBasicCRUD tests basic Create, Read, Update, Delete operations
func testBasicCRUD(t *testing.T, ts *TestServer) {
	collection := "users"

	// Test Insert
	doc := map[string]interface{}{
		"name":  "Alice",
		"age":   30,
		"email": "alice@example.com",
	}

	resp, err := ts.POST(fmt.Sprintf("/collections/%s", collection), doc)
	require.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	// Parse response to get document ID
	body, err := ReadResponseBody(resp)
	require.NoError(t, err)

	var insertResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &insertResp)
	require.NoError(t, err)

	docID, ok := insertResp["_id"].(string)
	require.True(t, ok, "Expected _id field in response")

	// Test Get by ID
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	var getResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &getResp)
	require.NoError(t, err)

	assert.Equal(t, "Alice", getResp["name"])
	assert.Equal(t, float64(30), getResp["age"]) // JSON numbers are float64

	// Test Update (PATCH)
	update := map[string]interface{}{
		"age":  31,
		"city": "New York",
	}

	resp, err = ts.PATCH(fmt.Sprintf("/collections/%s/documents/%s", collection, docID), update)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Verify update
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	var updatedResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &updatedResp)
	require.NoError(t, err)

	assert.Equal(t, float64(31), updatedResp["age"])
	assert.Equal(t, "New York", updatedResp["city"])
	assert.Equal(t, "Alice", updatedResp["name"]) // Original field preserved

	// Test Replace (PUT)
	replace := map[string]interface{}{
		"name":     "Alice Smith",
		"position": "Senior Developer",
		"salary":   95000,
	}

	resp, err = ts.PUT(fmt.Sprintf("/collections/%s/documents/%s", collection, docID), replace)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Verify replace
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	var replacedResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &replacedResp)
	require.NoError(t, err)

	assert.Equal(t, "Alice Smith", replacedResp["name"])
	assert.Equal(t, "Senior Developer", replacedResp["position"])
	assert.Equal(t, float64(95000), replacedResp["salary"])
	assert.NotContains(t, replacedResp, "age")   // Old field removed
	assert.NotContains(t, replacedResp, "email") // Old field removed

	// Test Delete
	resp, err = ts.DELETE(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
	require.NoError(t, err)
	assert.Equal(t, 204, resp.StatusCode)

	// Verify deletion
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)
}

// testBatchOperations tests batch insert and update operations
func testBatchOperations(t *testing.T, ts *TestServer) {
	collection := "employees"

	// Test Batch Insert
	documents := []map[string]interface{}{
		{"name": "Alice", "age": 30, "department": "Engineering"},
		{"name": "Bob", "age": 25, "department": "Sales"},
		{"name": "Charlie", "age": 35, "department": "Engineering"},
		{"name": "Diana", "age": 28, "department": "Marketing"},
		{"name": "Eve", "age": 32, "department": "Engineering"},
	}

	request := BatchInsertRequest{
		Documents: documents,
	}

	resp, err := ts.POST(fmt.Sprintf("/collections/%s/batch", collection), request)
	require.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	body, err := ReadResponseBody(resp)
	require.NoError(t, err)

	var response BatchInsertResponse
	err = json.Unmarshal([]byte(body), &response)
	require.NoError(t, err)

	assert.True(t, response.Success)
	assert.Equal(t, 5, response.InsertedCount)
	assert.Equal(t, collection, response.Collection)
	assert.Len(t, response.Documents, 5)

	// Store document IDs for batch update
	docIDs := make([]string, len(response.Documents))
	for i, doc := range response.Documents {
		docIDs[i] = doc["_id"].(string)
	}

	// Test Batch Update
	operations := []BatchUpdateOperation{
		{
			ID: docIDs[0],
			Updates: map[string]interface{}{
				"age":    31,
				"salary": 75000,
			},
		},
		{
			ID: docIDs[1],
			Updates: map[string]interface{}{
				"age":    26,
				"salary": 60000,
			},
		},
	}

	updateRequest := BatchUpdateRequest{
		Operations: operations,
	}

	resp, err = ts.PATCH(fmt.Sprintf("/collections/%s/batch", collection), updateRequest)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	var updateResponse BatchUpdateResponse
	err = json.Unmarshal([]byte(body), &updateResponse)
	require.NoError(t, err)

	assert.True(t, updateResponse.Success)
	assert.Equal(t, 2, updateResponse.UpdatedCount)
	assert.Equal(t, 0, updateResponse.FailedCount)
}

// testIndexOperations tests index creation and management
func testIndexOperations(t *testing.T, ts *TestServer) {
	collection := "products"

	// Insert some test data
	products := []map[string]interface{}{
		{"name": "Laptop", "category": "Electronics", "price": 999.99},
		{"name": "Phone", "category": "Electronics", "price": 699.99},
		{"name": "Book", "category": "Education", "price": 29.99},
	}

	for _, product := range products {
		resp, err := ts.POST(fmt.Sprintf("/collections/%s", collection), product)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)
	}

	// Test Create Index
	resp, err := ts.POST(fmt.Sprintf("/collections/%s/indexes/category", collection), nil)
	require.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	// Test Get Indexes
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/indexes", collection))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err := ReadResponseBody(resp)
	require.NoError(t, err)

	var indexesResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &indexesResp)
	require.NoError(t, err)

	assert.True(t, indexesResp["success"].(bool))
	indexes := indexesResp["indexes"].([]interface{})
	assert.Contains(t, indexes, "_id")      // Auto-created
	assert.Contains(t, indexes, "category") // Manually created
}

// testFindOperations tests find and query operations
func testFindOperations(t *testing.T, ts *TestServer) {
	collection := "orders"

	// Insert test data
	orders := []map[string]interface{}{
		{"customer": "Alice", "amount": 100.0, "status": "pending"},
		{"customer": "Bob", "amount": 250.0, "status": "completed"},
		{"customer": "Alice", "amount": 75.0, "status": "completed"},
		{"customer": "Charlie", "amount": 300.0, "status": "pending"},
	}

	for _, order := range orders {
		resp, err := ts.POST(fmt.Sprintf("/collections/%s", collection), order)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)
	}

	// Test Find All
	resp, err := ts.GET(fmt.Sprintf("/collections/%s/find", collection))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err := ReadResponseBody(resp)
	require.NoError(t, err)

	var findResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &findResp)
	require.NoError(t, err)

	documents := findResp["documents"].([]interface{})
	assert.Len(t, documents, 4)

	// Test Find with Filter
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/find?status=completed", collection))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	err = json.Unmarshal([]byte(body), &findResp)
	require.NoError(t, err)

	documents = findResp["documents"].([]interface{})
	assert.Len(t, documents, 2) // Only completed orders

	// Test Find with Pagination
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/find?limit=2&offset=1", collection))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	err = json.Unmarshal([]byte(body), &findResp)
	require.NoError(t, err)

	documents = findResp["documents"].([]interface{})
	assert.Len(t, documents, 2)
}

// testStreamingOperations tests streaming find operations
func testStreamingOperations(t *testing.T, ts *TestServer) {
	collection := "logs"

	// Insert test data
	for i := 0; i < 10; i++ {
		log := map[string]interface{}{
			"level":     "info",
			"message":   fmt.Sprintf("Log message %d", i),
			"timestamp": time.Now().Unix(),
		}

		resp, err := ts.POST(fmt.Sprintf("/collections/%s", collection), log)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)
	}

	// Test Streaming Find
	resp, err := ts.GET(fmt.Sprintf("/collections/%s/find_with_stream", collection))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	// Note: Streaming response testing would require more complex parsing
	// For now, we just verify the endpoint responds correctly
}

// testConcurrentOperations tests concurrent access patterns
func testConcurrentOperations(t *testing.T, ts *TestServer) {
	collection := "concurrent_test"

	// This is a simplified concurrent test
	// In a real scenario, you'd use goroutines and sync mechanisms
	doc := map[string]interface{}{
		"name":  "Concurrent Test",
		"value": 1,
	}

	resp, err := ts.POST(fmt.Sprintf("/collections/%s", collection), doc)
	require.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	// Verify the document was created successfully
	body, err := ReadResponseBody(resp)
	require.NoError(t, err)

	var insertResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &insertResp)
	require.NoError(t, err)

	docID := insertResp["_id"].(string)

	// Test concurrent reads (simplified)
	for i := 0; i < 5; i++ {
		resp, err := ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)
	}
}

// testErrorHandling tests error scenarios
func testErrorHandling(t *testing.T, ts *TestServer) {
	collection := "error_test"

	// Test Get non-existent document
	resp, err := ts.GET(fmt.Sprintf("/collections/%s/documents/nonexistent", collection))
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)

	// Test Update non-existent document
	update := map[string]interface{}{"field": "value"}
	resp, err = ts.PATCH(fmt.Sprintf("/collections/%s/documents/nonexistent", collection), update)
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)

	// Test Delete non-existent document
	resp, err = ts.DELETE(fmt.Sprintf("/collections/%s/documents/nonexistent", collection))
	require.NoError(t, err)
	assert.Equal(t, 404, resp.StatusCode)

	// Test invalid JSON
	invalidJSON := `{"invalid": json}`
	resp, err = ts.POSTRaw(fmt.Sprintf("/collections/%s", collection), invalidJSON)
	require.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

// testDataPersistence tests data persistence across operations
func testDataPersistence(t *testing.T, ts *TestServer) {
	collection := "persistence_test"

	// Insert document
	doc := map[string]interface{}{
		"name": "Persistence Test",
		"data": "This should persist",
	}

	resp, err := ts.POST(fmt.Sprintf("/collections/%s", collection), doc)
	require.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	// Get document ID
	body, err := ReadResponseBody(resp)
	require.NoError(t, err)

	var insertResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &insertResp)
	require.NoError(t, err)

	docID := insertResp["_id"].(string)

	// Verify document exists
	resp, err = ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, err = ReadResponseBody(resp)
	require.NoError(t, err)

	var getResp map[string]interface{}
	err = json.Unmarshal([]byte(body), &getResp)
	require.NoError(t, err)

	assert.Equal(t, "Persistence Test", getResp["name"])
	assert.Equal(t, "This should persist", getResp["data"])

	// Test that data persists across multiple operations
	for i := 0; i < 3; i++ {
		resp, err = ts.GET(fmt.Sprintf("/collections/%s/documents/%s", collection, docID))
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)
	}
}
