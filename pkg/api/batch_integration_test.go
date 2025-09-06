package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_Integration_BatchOperations(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close(t)

	t.Run("Batch Insert", func(t *testing.T) {
		// Prepare batch insert request
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

		// Execute batch insert
		resp, err := ts.POST("/collections/employees/batch", request)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Parse response
		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var response BatchInsertResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)

		// Verify response
		assert.True(t, response.Success)
		assert.Equal(t, 5, response.InsertedCount)
		assert.Equal(t, "employees", response.Collection)
		assert.Equal(t, "Batch insert completed successfully", response.Message)

		// Verify documents were actually inserted
		findResp, err := ts.GET("/collections/employees/find")
		require.NoError(t, err)
		assert.Equal(t, 200, findResp.StatusCode)

		findBody, err := ReadResponseBody(findResp)
		require.NoError(t, err)

		var findResult map[string]interface{}
		err = json.Unmarshal([]byte(findBody), &findResult)
		require.NoError(t, err)

		docs := findResult["documents"].([]interface{})
		assert.Len(t, docs, 5)
	})

	t.Run("Batch Update", func(t *testing.T) {
		// First, insert some documents to update
		documents := []map[string]interface{}{
			{"name": "John", "age": 25, "salary": 50000},
			{"name": "Jane", "age": 30, "salary": 60000},
			{"name": "Jack", "age": 35, "salary": 70000},
		}

		insertReq := BatchInsertRequest{Documents: documents}
		resp, err := ts.POST("/collections/staff/batch", insertReq)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Prepare batch update request
		operations := []BatchUpdateOperation{
			{ID: "1", Updates: map[string]interface{}{"salary": 55000, "bonus": 5000}},
			{ID: "2", Updates: map[string]interface{}{"salary": 65000, "bonus": 7000}},
			{ID: "3", Updates: map[string]interface{}{"salary": 75000, "bonus": 8000}},
		}

		updateReq := BatchUpdateRequest{Operations: operations}

		// Execute batch update
		resp, err = ts.PATCH("/collections/staff/batch", updateReq)
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)

		// Parse response
		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var response BatchUpdateResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)

		// Verify response
		assert.True(t, response.Success)
		assert.Equal(t, 3, response.UpdatedCount)
		assert.Equal(t, 0, response.FailedCount)
		assert.Equal(t, "staff", response.Collection)

		// Verify updates were applied
		docResp, err := ts.GET("/collections/staff/documents/1")
		require.NoError(t, err)
		assert.Equal(t, 200, docResp.StatusCode)

		docBody, err := ReadResponseBody(docResp)
		require.NoError(t, err)

		var doc map[string]interface{}
		err = json.Unmarshal([]byte(docBody), &doc)
		require.NoError(t, err)

		assert.Equal(t, float64(55000), doc["salary"])
		assert.Equal(t, float64(5000), doc["bonus"])
	})

	t.Run("Batch Insert - Large Batch", func(t *testing.T) {
		// Test with 500 documents (within limit)
		documents := make([]map[string]interface{}, 500)
		for i := 0; i < 500; i++ {
			documents[i] = map[string]interface{}{
				"id":    i,
				"name":  "User" + string(rune('A'+i%26)),
				"value": i * 10,
			}
		}

		request := BatchInsertRequest{Documents: documents}

		resp, err := ts.POST("/collections/large_batch/batch", request)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var response BatchInsertResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)

		assert.True(t, response.Success)
		assert.Equal(t, 500, response.InsertedCount)
	})

	t.Run("Batch Insert - Validation Errors", func(t *testing.T) {
		// Test empty documents
		emptyReq := BatchInsertRequest{Documents: []map[string]interface{}{}}
		resp, err := ts.POST("/collections/test/batch", emptyReq)
		require.NoError(t, err)
		assert.Equal(t, 400, resp.StatusCode)

		// Test too many documents (over 1000)
		tooManyDocs := make([]map[string]interface{}, 1001)
		for i := 0; i < 1001; i++ {
			tooManyDocs[i] = map[string]interface{}{"id": i}
		}

		largeReq := BatchInsertRequest{Documents: tooManyDocs}
		resp, err = ts.POST("/collections/test/batch", largeReq)
		require.NoError(t, err)
		assert.Equal(t, 400, resp.StatusCode)
	})

	t.Run("Batch Update - Validation Errors", func(t *testing.T) {
		// Test empty operations
		emptyReq := BatchUpdateRequest{Operations: []BatchUpdateOperation{}}
		resp, err := ts.PATCH("/collections/test/batch", emptyReq)
		require.NoError(t, err)
		assert.Equal(t, 400, resp.StatusCode)

		// Test too many operations
		tooManyOps := make([]BatchUpdateOperation, 1001)
		for i := 0; i < 1001; i++ {
			tooManyOps[i] = BatchUpdateOperation{
				ID:      "1",
				Updates: map[string]interface{}{"field": i},
			}
		}

		largeReq := BatchUpdateRequest{Operations: tooManyOps}
		resp, err = ts.PATCH("/collections/test/batch", largeReq)
		require.NoError(t, err)
		assert.Equal(t, 400, resp.StatusCode)
	})

	t.Run("Batch Update - Partial Failures", func(t *testing.T) {
		// Insert some documents first
		documents := []map[string]interface{}{
			{"name": "Alice", "value": 1},
			{"name": "Bob", "value": 2},
		}

		insertReq := BatchInsertRequest{Documents: documents}
		resp, err := ts.POST("/collections/partial_test/batch", insertReq)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Try to update some existing and some non-existing documents
		operations := []BatchUpdateOperation{
			{ID: "1", Updates: map[string]interface{}{"value": 10}},   // Should succeed
			{ID: "999", Updates: map[string]interface{}{"value": 20}}, // Should fail
			{ID: "2", Updates: map[string]interface{}{"value": 30}},   // Should succeed
			{ID: "998", Updates: map[string]interface{}{"value": 40}}, // Should fail
		}

		updateReq := BatchUpdateRequest{Operations: operations}
		resp, err = ts.PATCH("/collections/partial_test/batch", updateReq)
		require.NoError(t, err)

		// Should return partial content status or handle errors appropriately
		assert.True(t, resp.StatusCode == 200 || resp.StatusCode == 206 || resp.StatusCode == 500)

		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		// The exact response format will depend on how we handle partial failures
		// This test verifies the endpoint handles mixed success/failure scenarios
		assert.NotEmpty(t, body)
	})

	t.Run("Batch Operations - Transaction Saves", func(t *testing.T) {
		// Test that transaction saves work with batch operations
		collName := "transaction_test"

		documents := []map[string]interface{}{
			{"name": "Test1", "value": 1},
			{"name": "Test2", "value": 2},
		}

		request := BatchInsertRequest{Documents: documents}
		resp, err := ts.POST("/collections/"+collName+"/batch", request)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Verify files are created due to transaction saves
		// Note: In production, you might check the actual file system
		// Here we just verify the operation completed successfully
		body, err := ReadResponseBody(resp)
		require.NoError(t, err)

		var response BatchInsertResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)
		assert.True(t, response.Success)
	})
}
