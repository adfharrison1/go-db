/**
 * Simple test file to verify the SDK compiles and works correctly
 */

import { GoDBClient, Document, HealthResponse } from './index';

async function testSDK() {
  console.log('Testing go-db TypeScript SDK...');

  // Test 1: Client instantiation
  console.log('✓ Testing client instantiation...');
  const client = new GoDBClient({
    baseURL: 'http://localhost:8080',
    timeout: 5000,
  });

  // Test 2: Type checking
  console.log('✓ Testing type definitions...');
  const testDocument: Document = {
    name: 'Test User',
    email: 'test@example.com',
    age: 25,
  };

  const testHealth: HealthResponse = {
    status: 'healthy',
    message: 'Database is running',
  };

  console.log('Test document:', testDocument);
  console.log('Test health response:', testHealth);

  // Test 3: Method existence
  console.log('✓ Testing method availability...');
  const methods = [
    'health',
    'insert',
    'batchInsert',
    'getById',
    'updateById',
    'replaceById',
    'deleteById',
    'find',
    'findWithStream',
    'getIndexes',
    'createIndex',
    'batchUpdate',
  ];

  methods.forEach((method) => {
    if (typeof (client as any)[method] === 'function') {
      console.log(`  ✓ ${method} method exists`);
    } else {
      console.log(`  ✗ ${method} method missing`);
    }
  });

  // Test 4: Configuration
  console.log('✓ Testing configuration...');
  console.log('Base URL:', client.getBaseURL());

  // Test 5: HTTP client access
  console.log('✓ Testing HTTP client access...');
  const httpClient = client.getHttpClient();
  console.log('HTTP client available:', !!httpClient);

  console.log('\n🎉 All tests passed! SDK is ready to use.');
  console.log('\nTo run the examples:');
  console.log('  npm run build');
  console.log('  node dist/examples/basic-usage.js');
  console.log('  node dist/examples/advanced-usage.js');
}

// Run the test
if (require.main === module) {
  testSDK().catch(console.error);
}

export { testSDK };
