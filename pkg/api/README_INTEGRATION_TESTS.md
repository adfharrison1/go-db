# API Integration Tests

This directory contains comprehensive integration tests for the go-db API layer. These tests spin up a real HTTP server and test the entire API stack with actual HTTP requests.

## Test Structure

### TestServer Helper

The `TestServer` struct provides a convenient way to create isolated test environments:

```go
ts := NewTestServer(t)
defer ts.Close(t)

// Make HTTP requests
resp, err := ts.POST("/collections/users/insert", userDocument)
```

**Features:**

- Automatic temporary directory management
- Configurable storage options
- Built-in HTTP client methods (GET, POST, PUT, DELETE)
- Proper cleanup of resources

### Test Coverage

#### Basic CRUD Operations (`TestAPI_Integration_BasicCRUD`)

- **Insert Document**: Tests document creation via POST
- **Get Document by ID**: Tests document retrieval via GET
- **Update Document**: Tests document modification via PUT
- **Find All Documents**: Tests collection querying
- **Delete Document**: Tests document removal via DELETE

#### Transaction Save Testing (`TestAPI_Integration_TransactionSaves`)

- **File Creation**: Verifies files are created immediately after write operations
- **Timestamp Verification**: Ensures file modification times update correctly
- **Disabled Mode**: Tests behavior when transaction saves are disabled

#### Error Handling (`TestAPI_Integration_ErrorHandling`)

- **Non-existent Documents**: Tests 404 responses for missing documents
- **Invalid JSON**: Tests 400 responses for malformed request bodies
- **Collection Errors**: Tests error scenarios for missing collections

#### Concurrent Operations (`TestAPI_Integration_ConcurrentRequests`)

- **Concurrent Inserts**: Tests multiple simultaneous document insertions
- **Read/Write Mix**: Tests mixed read and write operations under load
- **Thread Safety**: Verifies no race conditions or data corruption

#### Index Operations (`TestAPI_Integration_IndexOperations`)

- **Index Creation**: Tests creating indexes on collection fields
- **Invalid Fields**: Tests error handling for invalid index operations

#### Pagination (`TestAPI_Integration_Pagination`)

- **Limit Parameters**: Tests result limiting with query parameters
- **Metadata**: Verifies pagination metadata (has_next, cursors)

## Usage Examples

### Basic Test

```go
func TestAPI_MyFeature(t *testing.T) {
    ts := NewTestServer(t)
    defer ts.Close(t)

    // Insert a document
    doc := map[string]interface{}{"name": "Test", "value": 42}
    resp, err := ts.POST("/collections/test/insert", doc)
    require.NoError(t, err)
    assert.Equal(t, http.StatusCreated, resp.StatusCode)

    // Retrieve the document
    resp, err = ts.GET("/collections/test/documents/1")
    require.NoError(t, err)
    assert.Equal(t, http.StatusOK, resp.StatusCode)
}
```

### Custom Storage Configuration

```go
func TestAPI_WithCustomStorage(t *testing.T) {
    ts := NewTestServer(t,
        storage.WithTransactionSave(false),
        storage.WithMaxMemory(512),
    )
    defer ts.Close(t)

    // Test with custom storage settings
}
```

### Reading Response Bodies

```go
resp, err := ts.GET("/collections/users/find")
require.NoError(t, err)

body, err := ReadResponseBody(resp)
require.NoError(t, err)

var result map[string]interface{}
err = json.Unmarshal([]byte(body), &result)
require.NoError(t, err)

documents := result["documents"].([]interface{})
assert.Len(t, documents, expectedCount)
```

## Running Integration Tests

```bash
# Run all integration tests
go test ./pkg/api -v -run "TestAPI_Integration"

# Run specific test
go test ./pkg/api -v -run "TestAPI_Integration_BasicCRUD"

# Run with timeout
go test ./pkg/api -v -run "TestAPI_Integration" -timeout 60s
```

## Test Isolation

Each test uses:

- **Temporary directories**: Automatic cleanup prevents test interference
- **Unique collections**: Different collection names per test avoid conflicts
- **Fresh storage engines**: Each TestServer gets its own isolated storage
- **Proper cleanup**: `defer ts.Close(t)` ensures resources are released

## Performance Considerations

- **Concurrent tests** use reduced goroutine counts to avoid resource contention
- **Transaction saves** are disabled in concurrent tests to prevent lock contention
- **Timeouts** are set appropriately for CI/CD environments

## Best Practices

1. **Always use `defer ts.Close(t)`** to ensure cleanup
2. **Use unique collection names** to avoid test interference
3. **Check both status codes and response bodies** for complete validation
4. **Use `require.NoError`** for critical operations that must succeed
5. **Use `assert.*`** for validation that should continue on failure
6. **Test both success and error scenarios** for comprehensive coverage

## Integration with CI/CD

These tests are designed to:

- Run quickly (< 5 seconds for full suite)
- Be deterministic and reproducible
- Clean up after themselves
- Provide clear failure messages
- Work in containerized environments
