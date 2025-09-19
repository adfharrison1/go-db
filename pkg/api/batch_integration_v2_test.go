package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_IntegrationV2_BatchOperations(t *testing.T) {
	ts := NewTestServerV2(t)
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
		body, err := ReadResponseBodyV2(resp)
		require.NoError(t, err)

		var response BatchInsertResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)

		// Verify response
		assert.True(t, response.Success)
		assert.Equal(t, 5, response.InsertedCount)
		assert.Equal(t, "employees", response.Collection)
		assert.Equal(t, "Batch insert completed successfully", response.Message)

		// Verify the response contains the created documents with IDs
		assert.Len(t, response.Documents, 5)
		expectedNames := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}
		expectedAges := []int{30, 25, 35, 28, 32}
		for i, doc := range response.Documents {
			assert.Contains(t, doc, "_id")
			assert.Equal(t, expectedNames[i], doc["name"])
			assert.Equal(t, expectedAges[i], int(doc["age"].(float64)))
		}

		// Verify documents were actually inserted
		findResp, err := ts.GET("/collections/employees/find")
		require.NoError(t, err)
		assert.Equal(t, 200, findResp.StatusCode)

		findBody, err := ReadResponseBodyV2(findResp)
		require.NoError(t, err)

		var findResult map[string]interface{}
		err = json.Unmarshal([]byte(findBody), &findResult)
		require.NoError(t, err)

		docs := findResult["documents"].([]interface{})
		assert.Len(t, docs, 5)

		// Verify that _id index was automatically created
		indexResp, err := ts.GET("/collections/employees/indexes")
		require.NoError(t, err)
		assert.Equal(t, 200, indexResp.StatusCode)

		indexBody, err := ReadResponseBodyV2(indexResp)
		require.NoError(t, err)

		var indexResult map[string]interface{}
		err = json.Unmarshal([]byte(indexBody), &indexResult)
		require.NoError(t, err)

		assert.Equal(t, true, indexResult["success"])
		assert.Equal(t, "employees", indexResult["collection"])
		assert.Equal(t, float64(1), indexResult["index_count"])

		indexes, ok := indexResult["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 1)
		assert.Contains(t, indexes, "_id")
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

		// Capture the actual document IDs from the insert response
		body, err := ReadResponseBodyV2(resp)
		require.NoError(t, err)
		var insertResponse BatchInsertResponse
		err = json.Unmarshal([]byte(body), &insertResponse)
		require.NoError(t, err)
		require.Len(t, insertResponse.Documents, 3)

		// Extract the actual IDs
		actualIDs := make([]string, 3)
		for i, doc := range insertResponse.Documents {
			actualIDs[i] = doc["_id"].(string)
		}

		// Prepare batch update request using actual IDs
		operations := []BatchUpdateOperation{
			{ID: actualIDs[0], Updates: map[string]interface{}{"salary": 55000, "bonus": 5000}},
			{ID: actualIDs[1], Updates: map[string]interface{}{"salary": 65000, "bonus": 7000}},
			{ID: actualIDs[2], Updates: map[string]interface{}{"salary": 75000, "bonus": 8000}},
		}

		updateReq := BatchUpdateRequest{Operations: operations}

		// Execute batch update
		resp, err = ts.PATCH("/collections/staff/batch", updateReq)
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)

		// Parse response
		body, err = ReadResponseBodyV2(resp)
		require.NoError(t, err)

		var response BatchUpdateResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)

		// Verify response
		assert.True(t, response.Success)
		assert.Equal(t, 3, response.UpdatedCount)
		assert.Equal(t, 0, response.FailedCount)
		assert.Equal(t, "staff", response.Collection)

		// Verify returned documents
		assert.Len(t, response.Documents, 3)

		// Check that all returned documents have the expected structure
		for _, doc := range response.Documents {
			assert.NotEmpty(t, doc["_id"])
			assert.Contains(t, doc, "name")
			assert.Contains(t, doc, "age")
			assert.Contains(t, doc, "salary")
			assert.Contains(t, doc, "bonus")
		}

		// Verify updates were applied
		docResp, err := ts.GET("/collections/staff/documents/" + actualIDs[0])
		require.NoError(t, err)
		assert.Equal(t, 200, docResp.StatusCode)

		docBody, err := ReadResponseBodyV2(docResp)
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

		body, err := ReadResponseBodyV2(resp)
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

	t.Run("Batch Update - Atomic Failure", func(t *testing.T) {
		// Insert some documents first
		documents := []map[string]interface{}{
			{"name": "Alice", "value": 1},
			{"name": "Bob", "value": 2},
		}

		insertReq := BatchInsertRequest{Documents: documents}
		resp, err := ts.POST("/collections/partial_test/batch", insertReq)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Capture the actual document IDs from the insert response
		body, err := ReadResponseBodyV2(resp)
		require.NoError(t, err)
		var insertResponse BatchInsertResponse
		err = json.Unmarshal([]byte(body), &insertResponse)
		require.NoError(t, err)
		require.Len(t, insertResponse.Documents, 2)

		// Extract the actual IDs
		actualIDs := make([]string, 2)
		for i, doc := range insertResponse.Documents {
			actualIDs[i] = doc["_id"].(string)
		}

		// Try to update some existing and some non-existing documents
		// This should fail atomically - no updates should be applied
		operations := []BatchUpdateOperation{
			{ID: actualIDs[0], Updates: map[string]interface{}{"value": 10}}, // Valid
			{ID: "999", Updates: map[string]interface{}{"value": 20}},        // Invalid - doesn't exist
			{ID: actualIDs[1], Updates: map[string]interface{}{"value": 30}}, // Valid but won't be applied due to atomic failure
			{ID: "998", Updates: map[string]interface{}{"value": 40}},        // Invalid - doesn't exist
		}

		updateReq := BatchUpdateRequest{Operations: operations}
		resp, err = ts.PATCH("/collections/partial_test/batch", updateReq)
		require.NoError(t, err)

		// Should return 500 for atomic failure (document not found)
		assert.Equal(t, 500, resp.StatusCode)

		body, err = ReadResponseBodyV2(resp)
		require.NoError(t, err)
		assert.Contains(t, body, "document with id 999 not found")
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
		body, err := ReadResponseBodyV2(resp)
		require.NoError(t, err)

		var response BatchInsertResponse
		err = json.Unmarshal([]byte(body), &response)
		require.NoError(t, err)
		assert.True(t, response.Success)
	})

	t.Run("Batch Insert _id Index Creation and Updates", func(t *testing.T) {
		// Test that batch insert creates _id index and updates it properly
		documents := []map[string]interface{}{
			{"name": "BatchUser1", "age": 25},
			{"name": "BatchUser2", "age": 30},
			{"name": "BatchUser3", "age": 35},
		}

		// First batch insert - should create collection and _id index
		insertReq := BatchInsertRequest{Documents: documents}
		resp, err := ts.POST("/collections/batch_id_test/batch", insertReq)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Verify _id index was created
		indexResp, err := ts.GET("/collections/batch_id_test/indexes")
		require.NoError(t, err)
		assert.Equal(t, 200, indexResp.StatusCode)

		indexBody, err := ReadResponseBodyV2(indexResp)
		require.NoError(t, err)

		var indexResult map[string]interface{}
		err = json.Unmarshal([]byte(indexBody), &indexResult)
		require.NoError(t, err)

		assert.Equal(t, true, indexResult["success"])
		assert.Equal(t, "batch_id_test", indexResult["collection"])
		assert.Equal(t, float64(1), indexResult["index_count"])

		indexes, ok := indexResult["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 1)
		assert.Contains(t, indexes, "_id")

		// Second batch insert - should NOT recreate _id index, but should update it
		moreDocuments := []map[string]interface{}{
			{"name": "BatchUser4", "age": 40},
			{"name": "BatchUser5", "age": 45},
		}

		insertReq2 := BatchInsertRequest{Documents: moreDocuments}
		resp, err = ts.POST("/collections/batch_id_test/batch", insertReq2)
		require.NoError(t, err)
		assert.Equal(t, 201, resp.StatusCode)

		// Verify _id index still exists and count is still 1 (not recreated)
		indexResp, err = ts.GET("/collections/batch_id_test/indexes")
		require.NoError(t, err)
		assert.Equal(t, 200, indexResp.StatusCode)

		indexBody, err = ReadResponseBodyV2(indexResp)
		require.NoError(t, err)

		err = json.Unmarshal([]byte(indexBody), &indexResult)
		require.NoError(t, err)

		assert.Equal(t, true, indexResult["success"])
		assert.Equal(t, "batch_id_test", indexResult["collection"])
		assert.Equal(t, float64(1), indexResult["index_count"]) // Still only 1 index

		indexes, ok = indexResult["indexes"].([]interface{})
		require.True(t, ok)
		assert.Len(t, indexes, 1)
		assert.Contains(t, indexes, "_id")

		// Verify all documents can be found (index is working)
		findResp, err := ts.GET("/collections/batch_id_test/find")
		require.NoError(t, err)
		assert.Equal(t, 200, findResp.StatusCode)

		findBody, err := ReadResponseBodyV2(findResp)
		require.NoError(t, err)

		var findResult map[string]interface{}
		err = json.Unmarshal([]byte(findBody), &findResult)
		require.NoError(t, err)

		docs := findResult["documents"].([]interface{})
		assert.Len(t, docs, 5) // All 5 documents should be found

		// Verify all documents have _id fields
		for _, docInterface := range docs {
			doc := docInterface.(map[string]interface{})
			assert.Contains(t, doc, "_id")
		}
	})
}
