# go-db TypeScript SDK

A comprehensive TypeScript SDK for the go-db document database API. This SDK provides type-safe access to all go-db features including document CRUD operations, batch operations, indexing, and pagination.

## Features

- 🚀 **Type-Safe**: Full TypeScript support with comprehensive type definitions
- 📦 **Zero Dependencies**: Built on top of axios for HTTP requests
- 🔄 **Batch Operations**: Efficient bulk insert and update operations
- 📄 **Pagination**: Support for both offset-based and cursor-based pagination
- 🔍 **Indexing**: Create and manage indexes for improved query performance
- 🌊 **Streaming**: Support for streaming large result sets
- ⚡ **Performance**: Optimized for high-throughput operations

## Installation

```bash
yarn add @go-db/typescript-sdk
```

Or with npm:

```bash
npm install @go-db/typescript-sdk
```

## Quick Start

```typescript
import { GoDBClient } from '@go-db/typescript-sdk';

// Create a client instance
const client = new GoDBClient({
  baseURL: 'http://localhost:8080',
  timeout: 30000,
});

// Check if the database is healthy
const health = await client.health();
console.log(health.status); // 'healthy'

// Insert a document
const user = await client.insert('users', {
  name: 'John Doe',
  email: 'john@example.com',
  age: 30,
  active: true,
});

console.log(user._id); // Auto-generated document ID

// Find documents
const results = await client.find('users', {
  age: 30,
  limit: 10,
});

console.log(results.documents); // Array of matching documents
```

## API Reference

### Client Configuration

```typescript
interface GoDBClientConfig {
  baseURL?: string; // Default: 'http://localhost:8080'
  timeout?: number; // Default: 30000ms
  headers?: Record<string, string>; // Additional headers
}
```

### Document Operations

#### Insert Document

```typescript
// Insert a single document
const document = await client.insert('collection', {
  name: 'John Doe',
  email: 'john@example.com',
});
```

#### Batch Insert

```typescript
// Insert multiple documents
const response = await client.batchInsert('collection', [
  { name: 'John Doe', email: 'john@example.com' },
  { name: 'Jane Smith', email: 'jane@example.com' },
]);

console.log(response.inserted_count); // 2
```

#### Get Document by ID

```typescript
const document = await client.getById('collection', 'document_id');
```

#### Update Document

```typescript
// Partial update
const updated = await client.updateById('collection', 'document_id', {
  age: 31,
  last_login: new Date().toISOString(),
});

// Complete replacement
const replaced = await client.replaceById('collection', 'document_id', {
  name: 'John Updated',
  email: 'john.updated@example.com',
});
```

#### Delete Document

```typescript
await client.deleteById('collection', 'document_id');
```

### Query Operations

#### Find Documents

```typescript
// Basic find with pagination
const results = await client.find('collection', {
  limit: 10,
  offset: 0,
});

// Find with filters
const activeUsers = await client.find('users', {
  active: true,
  age: { $gte: 18 },
  limit: 20,
});

// Cursor-based pagination
const nextPage = await client.find('collection', {
  after: 'eyJpZCI6InVzZXJfMTIzIn0=',
  limit: 10,
});
```

#### Streaming Results

```typescript
// For large result sets
const stream = await client.findWithStream('collection', {
  active: true,
});

const reader = stream.getReader();
while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  console.log(value); // Process each document
}
```

### Index Operations

#### List Indexes

```typescript
const indexes = await client.getIndexes('collection');
console.log(indexes.indexes); // ['_id', 'email', 'name']
```

#### Create Index

```typescript
const response = await client.createIndex('collection', 'email');
console.log(response.message); // 'Index created successfully'
```

### Batch Operations

#### Batch Update

```typescript
const response = await client.batchUpdate('collection', [
  {
    id: 'doc1',
    updates: { status: 'active' },
  },
  {
    id: 'doc2',
    updates: { last_login: new Date().toISOString() },
  },
]);

console.log(response.updated_count); // 2
```

## Error Handling

The SDK provides comprehensive error handling:

```typescript
try {
  const document = await client.getById('collection', 'nonexistent');
} catch (error) {
  if (error.message.includes('API Error 404')) {
    console.log('Document not found');
  } else if (error.message.includes('Network Error')) {
    console.log('Connection failed');
  } else {
    console.log('Unexpected error:', error.message);
  }
}
```

## Advanced Usage

### Custom HTTP Client

```typescript
const httpClient = client.getHttpClient();

// Add custom interceptors
httpClient.interceptors.request.use((config) => {
  config.headers.Authorization = `Bearer ${token}`;
  return config;
});
```

### Custom Requests

```typescript
// Make custom API requests
const response = await client.request('GET', '/custom/endpoint');
console.log(response.data);
```

## Type Definitions

The SDK exports comprehensive TypeScript types:

```typescript
import {
  Document,
  HealthResponse,
  BatchInsertRequest,
  BatchInsertResponse,
  PaginationResult,
  FindOptions,
  // ... and more
} from '@go-db/typescript-sdk';
```

## Examples

### Complete CRUD Example

```typescript
import { GoDBClient } from '@go-db/typescript-sdk';

const client = new GoDBClient({ baseURL: 'http://localhost:8080' });

async function crudExample() {
  // Create
  const user = await client.insert('users', {
    name: 'John Doe',
    email: 'john@example.com',
    age: 30,
  });

  // Read
  const found = await client.getById('users', user._id!);

  // Update
  const updated = await client.updateById('users', user._id!, {
    age: 31,
    last_login: new Date().toISOString(),
  });

  // Query
  const results = await client.find('users', {
    age: { $gte: 30 },
    limit: 10,
  });

  // Delete
  await client.deleteById('users', user._id!);
}
```

### Batch Operations Example

```typescript
async function batchExample() {
  // Batch insert
  const insertResponse = await client.batchInsert('users', [
    { name: 'User 1', email: 'user1@example.com' },
    { name: 'User 2', email: 'user2@example.com' },
    { name: 'User 3', email: 'user3@example.com' },
  ]);

  // Batch update
  const updateResponse = await client.batchUpdate('users', [
    { id: insertResponse.documents[0]._id!, updates: { status: 'active' } },
    { id: insertResponse.documents[1]._id!, updates: { status: 'inactive' } },
  ]);

  console.log(`Inserted: ${insertResponse.inserted_count}`);
  console.log(`Updated: ${updateResponse.updated_count}`);
}
```

## Development

### Building

```bash
yarn build
```

### Development Mode

```bash
yarn dev
```

### Cleaning

```bash
yarn clean
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please see the main project repository for contribution guidelines.

## Support

For issues and questions, please visit the [GitHub repository](https://github.com/adfharrison1/go-db).
