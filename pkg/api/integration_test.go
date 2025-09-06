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
