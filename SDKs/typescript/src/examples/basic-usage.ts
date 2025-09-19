/**
 * Basic usage examples for the go-db TypeScript SDK
 */

import { GoDBClient } from '../index';

// Create a client instance
const client = new GoDBClient({
  baseURL: 'http://localhost:8080',
  timeout: 30000,
});

async function basicExamples() {
  try {
    // 1. Health check
    console.log('Checking database health...');
    const health = await client.health();
    console.log('Health status:', health.status);
    console.log('Health message:', health.message);

    // 2. Insert a single document
    console.log('\nInserting a user document...');
    const user = await client.insert('users', {
      name: 'John Doe',
      email: 'john@example.com',
      age: 30,
      active: true,
      created_at: new Date().toISOString(),
    });
    console.log('Created user:', user);

    // 3. Get document by ID
    console.log('\nRetrieving user by ID...');
    const retrievedUser = await client.getById('users', user._id!);
    console.log('Retrieved user:', retrievedUser);

    // 4. Update document
    console.log('\nUpdating user...');
    const updatedUser = await client.updateById('users', user._id!, {
      age: 31,
      last_login: new Date().toISOString(),
    });
    console.log('Updated user:', updatedUser);

    // 5. Find documents with filters
    console.log('\nFinding active users...');
    const activeUsers = await client.find('users', {
      active: true,
      limit: 10,
    });
    console.log('Active users:', activeUsers.documents.length);

    // 6. Batch insert
    console.log('\nBatch inserting multiple users...');
    const batchResponse = await client.batchInsert('users', [
      { name: 'Jane Smith', email: 'jane@example.com', age: 25, active: true },
      { name: 'Bob Johnson', email: 'bob@example.com', age: 35, active: false },
      {
        name: 'Alice Brown',
        email: 'alice@example.com',
        age: 28,
        active: true,
      },
    ]);
    console.log(
      'Batch insert result:',
      batchResponse.inserted_count,
      'documents inserted'
    );

    // 7. Batch update
    console.log('\nBatch updating users...');
    const updateResponse = await client.batchUpdate('users', [
      {
        id: batchResponse.documents[0]._id!,
        updates: { last_login: new Date().toISOString() },
      },
      {
        id: batchResponse.documents[1]._id!,
        updates: { status: 'inactive', updated_at: new Date().toISOString() },
      },
    ]);
    console.log(
      'Batch update result:',
      updateResponse.updated_count,
      'documents updated'
    );

    // 8. Create an index
    console.log('\nCreating index on email field...');
    const indexResponse = await client.createIndex('users', 'email');
    console.log('Index creation result:', indexResponse.message);

    // 9. List indexes
    console.log('\nListing indexes...');
    const indexes = await client.getIndexes('users');
    console.log('Available indexes:', indexes.indexes);

    // 10. Find with pagination
    console.log('\nFinding users with pagination...');
    const paginatedResults = await client.find('users', {
      limit: 2,
      offset: 0,
    });
    console.log('Paginated results:', {
      documents: paginatedResults.documents.length,
      has_next: paginatedResults.has_next,
      has_prev: paginatedResults.has_prev,
      total: paginatedResults.total,
    });

    // 11. Clean up - delete the test user
    console.log('\nCleaning up test data...');
    await client.deleteById('users', user._id!);
    console.log('Test user deleted');
  } catch (error) {
    console.error('Error in basic examples:', error);
  }
}

// Run the examples
if (require.main === module) {
  basicExamples().catch(console.error);
}

export { basicExamples };
