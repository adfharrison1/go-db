package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adfharrison1/go-db/pkg/indexing"
	"github.com/adfharrison1/go-db/pkg/storage"
)

// TestServer represents a test HTTP server for integration testing
type TestServer struct {
	Server  *httptest.Server
	TempDir string
	Storage *storage.StorageEngine
	Handler *Handler
	BaseURL string
}

// NewTestServer creates a new test server with temporary storage
func NewTestServer(t *testing.T, storageOptions ...storage.StorageOption) *TestServer {
	tempDir, err := os.MkdirTemp("", "go-db-api-test-*")
	require.NoError(t, err)

	// Default options for testing
	defaultOptions := []storage.StorageOption{
		storage.WithDataDir(tempDir),
		storage.WithTransactionSave(true), // Enable transaction saves for testing
	}

	// Merge with provided options
	allOptions := append(defaultOptions, storageOptions...)

	storageEngine := storage.NewStorageEngine(allOptions...)
	indexEngine := indexing.NewIndexEngine()

	handler := NewHandler(storageEngine, indexEngine)

	router := mux.NewRouter()
	handler.RegisterRoutes(router)

	server := httptest.NewServer(router)

	return &TestServer{
		Server:  server,
		TempDir: tempDir,
		Storage: storageEngine,
		Handler: handler,
		BaseURL: server.URL,
	}
}

// Close cleans up the test server and temporary files
func (ts *TestServer) Close(t *testing.T) {
	ts.Server.Close()
	ts.Storage.StopBackgroundWorkers()
	err := os.RemoveAll(ts.TempDir)
	require.NoError(t, err)
}

// Helper methods for making HTTP requests

func (ts *TestServer) POST(path string, body interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	return http.Post(ts.BaseURL+path, "application/json", bytes.NewBuffer(jsonData))
}

func (ts *TestServer) GET(path string) (*http.Response, error) {
	return http.Get(ts.BaseURL + path)
}

func (ts *TestServer) PATCH(path string, body interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PATCH", ts.BaseURL+path, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

func (ts *TestServer) PUT(path string, body interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PUT", ts.BaseURL+path, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	return client.Do(req)
}

func (ts *TestServer) DELETE(path string) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", ts.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

// ReadResponseBody reads and returns the response body as a string
func ReadResponseBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

// Integration Tests

func TestAPI_Integration_BasicCRUD(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	t.Run("Insert Document", func(t *testing.T) {
		user := map[string]interface{}{
			"name":  "Alice",
			"age":   30,
			"email": "alice@example.com",
		}

		resp, err := ts.POST("/collections/users", user)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		// Verify the response contains the created document with ID
		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var createdDoc map[string]interface{}
		err = json.Unmarshal([]byte(body), &createdDoc)
		require.NoError(t, err)

		// Verify the document has an _id field
		assert.Contains(t, createdDoc, "_id")
		assert.Equal(t, "Alice", createdDoc["name"])
		assert.Equal(t, 30, int(createdDoc["age"].(float64)))
		assert.Equal(t, "alice@example.com", createdDoc["email"])

		// Verify file was created due to transaction save
		usersFile := filepath.Join(ts.TempDir, "collections", "users.godb")
		assert.FileExists(t, usersFile)

		// Verify that _id index was automatically created
		indexResp, err := ts.GET("/collections/users/indexes")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, indexResp.StatusCode)

		indexBody, err := ReadResponseBody(indexResp)
		require.NoError(t, err)

		var indexResult map[string]interface{}
		err = json.Unmarshal([]byte(indexBody), &indexResult)
		require.NoError(t, err)

		assert.Equal(t, true, indexResult["success"])
		assert.Equal(t, "users", indexResult["collection"])
		assert.Equal(t, float64(1), indexResult["index_count"])

		indexes, ok := indexResult["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 1)
		assert.Contains(t, indexes, "_id")
	})

	t.Run("Get Document by ID", func(t *testing.T) {
		resp, err := ts.GET("/collections/users/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		assert.Equal(t, "Alice", result["name"])
		assert.Equal(t, float64(30), result["age"]) // JSON numbers are float64
		assert.Equal(t, "alice@example.com", result["email"])
		assert.Equal(t, "1", result["_id"])
	})

	t.Run("Update Document", func(t *testing.T) {
		updates := map[string]interface{}{
			"age":  31,
			"city": "New York",
		}

		resp, err := ts.PATCH("/collections/users/documents/1", updates)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify the response contains the updated document
		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var updatedDoc map[string]interface{}
		err = json.Unmarshal([]byte(body), &updatedDoc)
		require.NoError(t, err)

		// Verify the document has the updated fields
		assert.Equal(t, "1", updatedDoc["_id"])
		assert.Equal(t, "Alice", updatedDoc["name"])              // Original field preserved
		assert.Equal(t, 31, int(updatedDoc["age"].(float64)))     // Updated field
		assert.Equal(t, "New York", updatedDoc["city"])           // New field
		assert.Equal(t, "alice@example.com", updatedDoc["email"]) // Original field preserved
	})

	t.Run("Replace Document", func(t *testing.T) {
		// First, let's get the current document to see what we're replacing
		resp, err := ts.GET("/collections/users/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Now replace the entire document with new content
		newDoc := map[string]interface{}{
			"name":     "Alice Smith",
			"age":      32,
			"position": "Senior Developer",
			"salary":   95000,
		}

		resp, err = ts.PUT("/collections/users/documents/1", newDoc)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify the response contains the replaced document
		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var replacedDoc map[string]interface{}
		err = json.Unmarshal([]byte(body), &replacedDoc)
		require.NoError(t, err)

		// Verify the document was completely replaced
		assert.Equal(t, "1", replacedDoc["_id"])                     // ID preserved
		assert.Equal(t, "Alice Smith", replacedDoc["name"])          // New field
		assert.Equal(t, 32, int(replacedDoc["age"].(float64)))       // New field
		assert.Equal(t, "Senior Developer", replacedDoc["position"]) // New field
		assert.Equal(t, 95000, int(replacedDoc["salary"].(float64))) // New field

		// Verify old fields are gone
		assert.Nil(t, replacedDoc["email"]) // Original field removed
		assert.Nil(t, replacedDoc["city"])  // Original field removed
	})

	t.Run("Find All Documents", func(t *testing.T) {
		// Insert another document first
		user2 := map[string]interface{}{
			"name":  "Bob",
			"age":   25,
			"email": "bob@example.com",
		}

		resp, err := ts.POST("/collections/users", user2)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		// Find all documents
		resp, err = ts.GET("/collections/users/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, 2)
	})

	t.Run("Delete Document", func(t *testing.T) {
		resp, err := ts.DELETE("/collections/users/documents/2")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, resp.StatusCode)

		// Verify document is gone
		resp, err = ts.GET("/collections/users/documents/2")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)

		// Verify other document still exists
		resp, err = ts.GET("/collections/users/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("_id Index Creation and Updates", func(t *testing.T) {
		// Create a new collection to test _id index behavior
		user1 := map[string]interface{}{
			"name": "TestUser1",
			"age":  25,
		}

		// First insert - should create collection and _id index
		resp, err := ts.POST("/collections/id_index_test", user1)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		// Verify _id index was created
		indexResp, err := ts.GET("/collections/id_index_test/indexes")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, indexResp.StatusCode)

		indexBody, err := ReadResponseBody(indexResp)
		require.NoError(t, err)

		var indexResult map[string]interface{}
		err = json.Unmarshal([]byte(indexBody), &indexResult)
		require.NoError(t, err)

		assert.Equal(t, true, indexResult["success"])
		assert.Equal(t, "id_index_test", indexResult["collection"])
		assert.Equal(t, float64(1), indexResult["index_count"])

		indexes, ok := indexResult["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 1)
		assert.Contains(t, indexes, "_id")

		// Insert second document - should NOT recreate _id index, but should update it
		user2 := map[string]interface{}{
			"name": "TestUser2",
			"age":  30,
		}

		resp, err = ts.POST("/collections/id_index_test", user2)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		// Verify _id index still exists and count is still 1 (not recreated)
		indexResp, err = ts.GET("/collections/id_index_test/indexes")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, indexResp.StatusCode)

		indexBody, err = ReadResponseBody(indexResp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(indexBody), &indexResult)
		require.NoError(t, err)

		assert.Equal(t, true, indexResult["success"])
		assert.Equal(t, "id_index_test", indexResult["collection"])
		assert.Equal(t, float64(1), indexResult["index_count"]) // Still only 1 index

		indexes, ok = indexResult["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 1)
		assert.Contains(t, indexes, "_id")

		// Verify both documents can be found (index is working)
		findResp, err := ts.GET("/collections/id_index_test/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, findResp.StatusCode)

		findBody, err := ReadResponseBody(findResp)
		require.NoError(t, err)

		var findResult map[string]interface{}
		err = json.Unmarshal([]byte(findBody), &findResult)
		require.NoError(t, err)

		docs := findResult["documents"].([]interface{})
		assert.Len(t, docs, 2) // Both documents should be found

		// Verify both documents have _id fields
		for _, docInterface := range docs {
			doc := docInterface.(map[string]interface{})
			assert.Contains(t, doc, "_id")
		}
	})
}

func TestAPI_Integration_TransactionSaves(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	t.Run("Files Created After Each Operation", func(t *testing.T) {
		usersFile := filepath.Join(ts.TempDir, "collections", "users.godb")

		// Initially no file
		assert.NoFileExists(t, usersFile)

		// Insert document
		user := map[string]interface{}{"name": "Test", "value": 42}
		resp, err := ts.POST("/collections/users", user)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		// File should be created
		assert.FileExists(t, usersFile)
		initialStat, err := os.Stat(usersFile)
		require.NoError(t, err)

		// Wait a moment to ensure different timestamps
		time.Sleep(10 * time.Millisecond)

		// Update document
		resp, err = ts.PATCH("/collections/users/documents/1", map[string]interface{}{"value": 43})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		// File should be updated
		updatedStat, err := os.Stat(usersFile)
		require.NoError(t, err)
		assert.True(t, updatedStat.ModTime().After(initialStat.ModTime()),
			"File should be updated after modification")
	})
}

func TestAPI_Integration_TransactionSavesDisabled(t *testing.T) {
	// Create server with transaction saves disabled
	ts := NewTestServer(t, storage.WithTransactionSave(false))
	defer ts.Close(t)

	t.Run("No Files Created When Transaction Saves Disabled", func(t *testing.T) {
		usersFile := filepath.Join(ts.TempDir, "collections", "users.godb")

		// Insert document
		user := map[string]interface{}{"name": "Test", "value": 42}
		resp, err := ts.POST("/collections/users", user)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		// File should NOT be created
		assert.NoFileExists(t, usersFile)

		// Document should still be retrievable (in memory)
		resp, err = ts.GET("/collections/users/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestAPI_Integration_ErrorHandling(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	t.Run("Get Non-Existent Document", func(t *testing.T) {
		resp, err := ts.GET("/collections/users/documents/999")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("Update Non-Existent Document", func(t *testing.T) {
		resp, err := ts.PATCH("/collections/users/documents/999", map[string]interface{}{"value": 42})
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("Replace Non-Existent Document", func(t *testing.T) {
		resp, err := ts.PUT("/collections/users/documents/999", map[string]interface{}{"value": 42})
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("Delete Non-Existent Document", func(t *testing.T) {
		resp, err := ts.DELETE("/collections/users/documents/999")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("Invalid JSON in Request Body", func(t *testing.T) {
		resp, err := http.Post(ts.BaseURL+"/collections/users",
			"application/json", bytes.NewBuffer([]byte("{invalid json")))
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

func TestAPI_Integration_ConcurrentRequests(t *testing.T) {
	// Use transaction saves disabled to avoid lock contention in concurrent tests
	ts := NewTestServer(t, storage.WithTransactionSave(false))
	defer ts.Close(t)

	t.Run("Concurrent Inserts", func(t *testing.T) {
		const numGoroutines = 5 // Reduced to avoid lock contention
		const insertsPerGoroutine = 3

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*insertsPerGoroutine)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(routineID int) {
				defer wg.Done()

				for j := 0; j < insertsPerGoroutine; j++ {
					user := map[string]interface{}{
						"name":      fmt.Sprintf("User-%d-%d", routineID, j),
						"routine":   routineID,
						"iteration": j,
					}

					resp, err := ts.POST("/collections/concurrent_users", user)
					if err != nil {
						errors <- err
						return
					}
					resp.Body.Close()

					if resp.StatusCode != http.StatusCreated {
						errors <- fmt.Errorf("unexpected status: %d", resp.StatusCode)
						return
					}
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent insert error: %v", err)
		}

		// Verify all documents were inserted
		resp, err := ts.GET("/collections/concurrent_users/find")
		require.NoError(t, err)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, numGoroutines*insertsPerGoroutine) // Should be 15
	})

	t.Run("Concurrent Read/Write Operations", func(t *testing.T) {
		// Insert initial document
		initialDoc := map[string]interface{}{"name": "Initial", "counter": 0}
		resp, err := ts.POST("/collections/readwrite", initialDoc)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		const numReaders = 3
		const numWriters = 2
		const operationsPerGoroutine = 5

		var wg sync.WaitGroup
		errors := make(chan error, (numReaders+numWriters)*operationsPerGoroutine)

		// Start readers
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					resp, err := ts.GET("/collections/readwrite/documents/1")
					if err != nil {
						errors <- fmt.Errorf("reader %d: %v", readerID, err)
						return
					}
					resp.Body.Close()

					if resp.StatusCode != http.StatusOK {
						errors <- fmt.Errorf("reader %d: unexpected status %d", readerID, resp.StatusCode)
						return
					}

					time.Sleep(1 * time.Millisecond)
				}
			}(i)
		}

		// Start writers
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(writerID int) {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					update := map[string]interface{}{
						"writer": writerID,
						"update": j,
					}

					resp, err := ts.PATCH("/collections/readwrite/documents/1", update)
					if err != nil {
						errors <- fmt.Errorf("writer %d: %v", writerID, err)
						return
					}
					resp.Body.Close()

					if resp.StatusCode != http.StatusOK {
						errors <- fmt.Errorf("writer %d: unexpected status %d", writerID, resp.StatusCode)
						return
					}

					time.Sleep(2 * time.Millisecond)
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			t.Errorf("Concurrent read/write error: %v", err)
		}

		// Verify final document state
		resp, err = ts.GET("/collections/readwrite/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestAPI_Integration_IndexOperations(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	t.Run("Create Index", func(t *testing.T) {
		// Insert some test data first
		users := []map[string]interface{}{
			{"name": "Alice", "age": 30, "department": "Engineering"},
			{"name": "Bob", "age": 25, "department": "Sales"},
			{"name": "Charlie", "age": 35, "department": "Engineering"},
		}

		for _, user := range users {
			resp, err := ts.POST("/collections/employees", user)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
		}

		// Create index on department field
		resp, err := ts.POST("/collections/employees/indexes/department", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		assert.Equal(t, true, result["success"])
		assert.Equal(t, "employees", result["collection"])
		assert.Equal(t, "department", result["field"])
	})

	t.Run("Create Index on Invalid Field", func(t *testing.T) {
		// Try to create index on _id (should fail)
		resp, err := ts.POST("/collections/employees/indexes/_id", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("Get Indexes", func(t *testing.T) {
		// Get indexes for collection (should have _id index from previous inserts)
		resp, err := ts.GET("/collections/employees/indexes")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		assert.Equal(t, true, result["success"])
		assert.Equal(t, "employees", result["collection"])
		// Should have at least _id index, possibly more from previous tests
		indexCount := result["index_count"].(float64)
		assert.GreaterOrEqual(t, indexCount, float64(1))

		indexes, ok := result["indexes"].([]interface{})
		require.True(t, ok)
		assert.GreaterOrEqual(t, len(indexes), 1)
		assert.Contains(t, indexes, "_id")
	})

	t.Run("Get Indexes - Non-Existent Collection", func(t *testing.T) {
		// Get indexes for non-existent collection
		resp, err := ts.GET("/collections/nonexistent/indexes")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		assert.Equal(t, true, result["success"])
		assert.Equal(t, "nonexistent", result["collection"])
		assert.Equal(t, float64(0), result["index_count"])

		indexes, ok := result["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 0)
	})
}

func TestAPI_Integration_Pagination(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	t.Run("Paginated Results", func(t *testing.T) {
		// Insert multiple documents
		for i := 1; i <= 10; i++ {
			user := map[string]interface{}{
				"name": fmt.Sprintf("User%d", i),
				"id":   i,
			}

			resp, err := ts.POST("/collections/paginated_users", user)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
		}

		// Test pagination with limit
		resp, err := ts.GET("/collections/paginated_users/find?limit=3")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, 3)

		// Check pagination metadata
		assert.Equal(t, true, result["has_next"])
		assert.Equal(t, false, result["has_prev"])
		assert.NotEmpty(t, result["next_cursor"])
	})
}

func TestAPI_Integration_IndexOptimization(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	// Insert test documents with different ages and cities
	docs := []map[string]interface{}{
		{"name": "Alice", "age": 25, "city": "New York"},
		{"name": "Bob", "age": 30, "city": "Boston"},
		{"name": "Charlie", "age": 25, "city": "Chicago"},
		{"name": "Diana", "age": 35, "city": "New York"},
		{"name": "Eve", "age": 25, "city": "Boston"},
	}

	for _, doc := range docs {
		resp, err := ts.POST("/collections/indexed_users", doc)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()
	}

	// Create index on age field
	resp, err := ts.POST("/collections/indexed_users/indexes/age", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	// Create index on city field
	resp, err = ts.POST("/collections/indexed_users/indexes/city", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	resp.Body.Close()

	t.Run("Find with Single Index", func(t *testing.T) {
		// Query by age=25 - should use index
		resp, err := ts.GET("/collections/indexed_users/find?age=25")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, 3) // Alice, Charlie, Eve

		// Verify the documents have age=25
		for _, docInterface := range documents {
			doc := docInterface.(map[string]interface{})
			assert.Equal(t, float64(25), doc["age"])
		}
	})

	t.Run("Find with Multiple Indexes", func(t *testing.T) {
		// Query by age=25 AND city=Boston - should use both indexes
		resp, err := ts.GET("/collections/indexed_users/find?age=25&city=Boston")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, 1) // Only Eve (Alice has city=New York)

		// Verify the document has both age=25 and city=Boston
		doc := documents[0].(map[string]interface{})
		assert.Equal(t, float64(25), doc["age"])
		assert.Equal(t, "Boston", doc["city"])
	})

	t.Run("Find with Index and Non-Indexed Field", func(t *testing.T) {
		// Query by age=25 AND name=Alice - should use index for age, filter by name
		resp, err := ts.GET("/collections/indexed_users/find?age=25&name=Alice")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, 1) // Only Alice

		// Verify the document
		doc := documents[0].(map[string]interface{})
		assert.Equal(t, "Alice", doc["name"])
		assert.Equal(t, float64(25), doc["age"])
	})

	t.Run("Stream with Index Optimization", func(t *testing.T) {
		// Test streaming with index optimization
		resp, err := ts.GET("/collections/indexed_users/find_with_stream?age=25")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		// Parse the JSON array response
		var documents []map[string]interface{}
		err = json.Unmarshal([]byte(body), &documents)
		require.NoError(t, err)

		assert.Len(t, documents, 3) // Alice, Charlie, Eve

		// Verify all documents have age=25
		for _, doc := range documents {
			assert.Equal(t, float64(25), doc["age"])
		}
	})

	t.Run("Find without Index (Fallback)", func(t *testing.T) {
		// Query by name=Bob - no index on name, should fall back to full scan
		resp, err := ts.GET("/collections/indexed_users/find?name=Bob")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents, ok := result["documents"].([]interface{})
		require.True(t, ok)
		assert.Len(t, documents, 1) // Only Bob

		// Verify the document
		doc := documents[0].(map[string]interface{})
		assert.Equal(t, "Bob", doc["name"])
	})
}

func TestAPI_Integration_PersistenceAcrossRestart(t *testing.T) {
	// This test simulates a full server restart cycle to ensure all operations persist correctly
	tempDir, err := os.MkdirTemp("", "go-db-persistence-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Use a single data file for persistence (like the main application)
	dataFile := filepath.Join(tempDir, "test-db.godb")

	t.Run("Complete CRUD Operations Persistence", func(t *testing.T) {
		// Phase 1: Create server, perform operations, shutdown
		server1 := NewTestServer(t, storage.WithDataDir(tempDir))

		// Insert initial documents
		doc1 := map[string]interface{}{
			"name":  "Alice",
			"age":   30,
			"email": "alice@example.com",
			"city":  "New York",
		}
		doc2 := map[string]interface{}{
			"name":  "Bob",
			"age":   25,
			"email": "bob@example.com",
			"city":  "Boston",
		}
		doc3 := map[string]interface{}{
			"name":  "Charlie",
			"age":   35,
			"email": "charlie@example.com",
			"city":  "Chicago",
		}

		// Insert documents
		resp, err := server1.POST("/collections/users", doc1)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		resp, err = server1.POST("/collections/users", doc2)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		resp, err = server1.POST("/collections/users", doc3)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		// Create indexes
		resp, err = server1.POST("/collections/users/indexes/age", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		resp, err = server1.POST("/collections/users/indexes/city", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		// PATCH update (partial update)
		patchUpdate := map[string]interface{}{
			"age":  31,
			"city": "San Francisco",
		}
		resp, err = server1.PATCH("/collections/users/documents/1", patchUpdate)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// PUT update (complete replacement)
		putUpdate := map[string]interface{}{
			"name":  "Bob Updated",
			"age":   26,
			"email": "bob.updated@example.com",
			"role":  "Senior Developer",
		}
		resp, err = server1.PUT("/collections/users/documents/2", putUpdate)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Delete a document
		resp, err = server1.DELETE("/collections/users/documents/3")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, resp.StatusCode)

		// Verify state before shutdown
		resp, err = server1.GET("/collections/users/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents := result["documents"].([]interface{})
		assert.Len(t, documents, 2) // Only Alice and Bob should remain

		// Save database to file before shutdown (like the main application)
		err = server1.Storage.SaveToFile(dataFile)
		require.NoError(t, err)

		// Shutdown server1
		server1.Close(t)

		// Phase 2: Create new server (simulating restart), verify persistence
		server2 := NewTestServer(t, storage.WithDataDir(tempDir))
		defer server2.Close(t)

		// Load database from file (like the main application)
		err = server2.Storage.LoadCollectionMetadata(dataFile)
		require.NoError(t, err)

		// First, we need to trigger collection loading by accessing it
		// The storage engine loads collections on-demand
		resp, err = server2.GET("/collections/users/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Now verify documents persisted
		resp, err = server2.GET("/collections/users/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = ReadResponseBody(resp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents = result["documents"].([]interface{})
		assert.Len(t, documents, 2, "Should have 2 documents after restart")

		// Verify Alice's PATCH update persisted
		aliceDoc := documents[0].(map[string]interface{})
		if aliceDoc["_id"] == "1" {
			assert.Equal(t, "Alice", aliceDoc["name"])
			assert.Equal(t, float64(31), aliceDoc["age"])           // Updated from 30 to 31
			assert.Equal(t, "San Francisco", aliceDoc["city"])      // Updated from New York
			assert.Equal(t, "alice@example.com", aliceDoc["email"]) // Original field preserved
		}

		// Verify Bob's PUT update persisted
		bobDoc := documents[1].(map[string]interface{})
		if bobDoc["_id"] == "2" {
			assert.Equal(t, "Bob Updated", bobDoc["name"])              // Completely replaced
			assert.Equal(t, float64(26), bobDoc["age"])                 // Completely replaced
			assert.Equal(t, "bob.updated@example.com", bobDoc["email"]) // Completely replaced
			assert.Equal(t, "Senior Developer", bobDoc["role"])         // New field added
			// city field should be gone (complete replacement)
			_, hasCity := bobDoc["city"]
			assert.False(t, hasCity, "city field should be removed in complete replacement")
		}

		// Verify Charlie was deleted
		resp, err = server2.GET("/collections/users/documents/3")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)

		// Verify indexes need to be recreated after restart (current limitation)
		resp, err = server2.GET("/collections/users/indexes")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = ReadResponseBody(resp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		indexes := result["indexes"].([]interface{})
		// Note: Indexes are not persisted in the current implementation
		// They need to be recreated after restart
		assert.Len(t, indexes, 0, "Indexes are not persisted and need to be recreated after restart")

		// Recreate indexes after restart
		resp, err = server2.POST("/collections/users/indexes/age", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		resp, err = server2.POST("/collections/users/indexes/city", nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		// Now verify index functionality works after recreation
		resp, err = server2.GET("/collections/users/find?age=31")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = ReadResponseBody(resp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents = result["documents"].([]interface{})
		assert.Len(t, documents, 1, "Should find 1 document with age=31")
		assert.Equal(t, "Alice", documents[0].(map[string]interface{})["name"])
	})

	t.Run("Batch Operations Persistence", func(t *testing.T) {
		// Phase 1: Create server, perform batch operations, shutdown
		server1 := NewTestServer(t, storage.WithDataDir(tempDir))

		// Batch insert
		batchDocs := []map[string]interface{}{
			{"name": "User1", "age": 20, "department": "Engineering"},
			{"name": "User2", "age": 25, "department": "Sales"},
			{"name": "User3", "age": 30, "department": "Marketing"},
		}

		batchInsertReq := BatchInsertRequest{Documents: batchDocs}
		resp, err := server1.POST("/collections/employees/batch", batchInsertReq)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		// Batch update
		batchUpdateOps := []BatchUpdateOperation{
			{
				ID: "1",
				Updates: map[string]interface{}{
					"age":    21,
					"salary": 50000,
				},
			},
			{
				ID: "2",
				Updates: map[string]interface{}{
					"department": "Senior Sales",
					"salary":     60000,
				},
			},
		}

		batchUpdateReq := BatchUpdateRequest{Operations: batchUpdateOps}
		resp, err = server1.PATCH("/collections/employees/batch", batchUpdateReq)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Save database to file before shutdown
		err = server1.Storage.SaveToFile(dataFile)
		require.NoError(t, err)

		// Shutdown server1
		server1.Close(t)

		// Phase 2: Create new server, verify batch operations persisted
		server2 := NewTestServer(t, storage.WithDataDir(tempDir))
		defer server2.Close(t)

		// Load database from file
		err = server2.Storage.LoadCollectionMetadata(dataFile)
		require.NoError(t, err)

		// First, trigger collection loading by accessing a document
		resp, err = server2.GET("/collections/employees/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Now verify batch insert persisted
		resp, err = server2.GET("/collections/employees/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents := result["documents"].([]interface{})
		assert.Len(t, documents, 3, "Should have 3 documents from batch insert")

		// Verify batch updates persisted
		for _, docInterface := range documents {
			doc := docInterface.(map[string]interface{})
			if doc["_id"] == "1" {
				assert.Equal(t, "User1", doc["name"])
				assert.Equal(t, float64(21), doc["age"])          // Updated from 20 to 21
				assert.Equal(t, float64(50000), doc["salary"])    // Added
				assert.Equal(t, "Engineering", doc["department"]) // Original preserved
			} else if doc["_id"] == "2" {
				assert.Equal(t, "User2", doc["name"])
				assert.Equal(t, float64(25), doc["age"])           // Original preserved
				assert.Equal(t, float64(60000), doc["salary"])     // Added
				assert.Equal(t, "Senior Sales", doc["department"]) // Updated from Sales
			} else if doc["_id"] == "3" {
				assert.Equal(t, "User3", doc["name"])
				assert.Equal(t, float64(30), doc["age"])        // Original preserved
				assert.Equal(t, "Marketing", doc["department"]) // Original preserved
				_, hasSalary := doc["salary"]
				assert.False(t, hasSalary, "User3 should not have salary field")
			}
		}
	})

	t.Run("Edge Cases Persistence", func(t *testing.T) {
		// Phase 1: Create server, test edge cases, shutdown
		server1 := NewTestServer(t, storage.WithDataDir(tempDir))

		// Test empty document
		emptyDoc := map[string]interface{}{}
		resp, err := server1.POST("/collections/empty_test", emptyDoc)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		// Test document with special characters and large data
		largeDoc := map[string]interface{}{
			"name":        "Test User with Special Characters: !@#$%^&*()",
			"description": strings.Repeat("This is a very long description. ", 100),
			"nested": map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"value": "deeply nested",
					},
				},
			},
			"array": []interface{}{1, 2, 3, "string", true, nil},
		}
		resp, err = server1.POST("/collections/edge_cases", largeDoc)
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		resp.Body.Close()

		// Test updating to empty document (PUT)
		emptyUpdate := map[string]interface{}{}
		resp, err = server1.PUT("/collections/edge_cases/documents/1", emptyUpdate)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Save database to file before shutdown
		err = server1.Storage.SaveToFile(dataFile)
		require.NoError(t, err)

		// Shutdown server1
		server1.Close(t)

		// Phase 2: Create new server, verify edge cases persisted
		server2 := NewTestServer(t, storage.WithDataDir(tempDir))
		defer server2.Close(t)

		// Load database from file
		err = server2.Storage.LoadCollectionMetadata(dataFile)
		require.NoError(t, err)

		// First, trigger collection loading by accessing a document
		resp, err = server2.GET("/collections/empty_test/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		assert.Equal(t, "1", result["_id"])
		// Should only have _id field

		// Verify empty update persisted (document should only have _id)
		resp, err = server2.GET("/collections/edge_cases/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = ReadResponseBody(resp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		assert.Equal(t, "1", result["_id"])
		// Should only have _id field after empty PUT update
		assert.Len(t, result, 1, "Document should only have _id field after empty PUT update")
	})

	t.Run("Multiple Collections Persistence", func(t *testing.T) {
		// Phase 1: Create server, work with multiple collections, shutdown
		server1 := NewTestServer(t, storage.WithDataDir(tempDir))

		// Create data in multiple collections
		users := []map[string]interface{}{
			{"name": "User1", "role": "admin"},
			{"name": "User2", "role": "user"},
		}

		products := []map[string]interface{}{
			{"name": "Product1", "price": 100},
			{"name": "Product2", "price": 200},
		}

		orders := []map[string]interface{}{
			{"user_id": "1", "product_id": "1", "quantity": 2},
			{"user_id": "2", "product_id": "2", "quantity": 1},
		}

		// Insert into users collection
		for _, user := range users {
			resp, err := server1.POST("/collections/users_multi", user)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
			resp.Body.Close()
		}

		// Insert into products collection
		for _, product := range products {
			resp, err := server1.POST("/collections/products_multi", product)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
			resp.Body.Close()
		}

		// Insert into orders collection
		for _, order := range orders {
			resp, err := server1.POST("/collections/orders_multi", order)
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, resp.StatusCode)
			resp.Body.Close()
		}

		// Save database to file before shutdown
		err = server1.Storage.SaveToFile(dataFile)
		require.NoError(t, err)

		// Shutdown server1
		server1.Close(t)

		// Phase 2: Create new server, verify all collections persisted
		server2 := NewTestServer(t, storage.WithDataDir(tempDir))
		defer server2.Close(t)

		// Load database from file
		err = server2.Storage.LoadCollectionMetadata(dataFile)
		require.NoError(t, err)

		// First, trigger collection loading by accessing documents
		resp, err := server2.GET("/collections/users_multi/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		resp, err = server2.GET("/collections/products_multi/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		resp, err = server2.GET("/collections/orders_multi/documents/1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		// Now verify users collection
		resp, err = server2.GET("/collections/users_multi/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var result map[string]interface{}
		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents := result["documents"].([]interface{})
		assert.Len(t, documents, 2, "Should have 2 users")

		// Verify products collection
		resp, err = server2.GET("/collections/products_multi/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = ReadResponseBody(resp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents = result["documents"].([]interface{})
		assert.Len(t, documents, 2, "Should have 2 products")

		// Verify orders collection
		resp, err = server2.GET("/collections/orders_multi/find")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err = ReadResponseBody(resp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(body), &result)
		require.NoError(t, err)

		documents = result["documents"].([]interface{})
		assert.Len(t, documents, 2, "Should have 2 orders")
	})
}
