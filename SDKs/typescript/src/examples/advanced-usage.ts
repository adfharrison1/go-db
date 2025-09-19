/**
 * Advanced usage examples for the go-db TypeScript SDK
 */

import { GoDBClient, FindOptions, Document } from '../index';

const client = new GoDBClient({
  baseURL: 'http://localhost:8080',
  timeout: 30000,
});

async function advancedExamples() {
  try {
    // 1. Streaming large result sets
    console.log('Demonstrating streaming...');
    await demonstrateStreaming();

    // 2. Complex queries with multiple filters
    console.log('\nDemonstrating complex queries...');
    await demonstrateComplexQueries();

    // 3. Error handling patterns
    console.log('\nDemonstrating error handling...');
    await demonstrateErrorHandling();

    // 4. Custom HTTP client usage
    console.log('\nDemonstrating custom HTTP client...');
    await demonstrateCustomHttpClient();

    // 5. Performance testing
    console.log('\nDemonstrating performance testing...');
    await demonstratePerformanceTesting();
  } catch (error) {
    console.error('Error in advanced examples:', error);
  }
}

async function demonstrateStreaming() {
  // Create some test data first
  const testData = Array.from({ length: 100 }, (_, i) => ({
    name: `User ${i}`,
    email: `user${i}@example.com`,
    age: 20 + (i % 50),
    active: i % 3 === 0,
    category: ['premium', 'standard', 'basic'][i % 3],
  }));

  await client.batchInsert('streaming_test', testData);

  // Stream the results
  const stream = await client.findWithStream('streaming_test', {
    active: true,
    age: { $gte: 25 },
  });

  const reader = stream.getReader();
  let count = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      count++;
      if (count <= 5) {
        // Show first 5 results
        console.log(`Streamed document ${count}:`, value.name);
      }
    }
    console.log(`Total documents streamed: ${count}`);
  } finally {
    reader.releaseLock();
  }
}

async function demonstrateComplexQueries() {
  // Create test data with various attributes
  const complexData = [
    {
      name: 'Alice',
      age: 25,
      salary: 50000,
      department: 'Engineering',
      level: 'Senior',
    },
    {
      name: 'Bob',
      age: 30,
      salary: 60000,
      department: 'Engineering',
      level: 'Lead',
    },
    {
      name: 'Charlie',
      age: 35,
      salary: 70000,
      department: 'Management',
      level: 'Director',
    },
    {
      name: 'Diana',
      age: 28,
      salary: 55000,
      department: 'Engineering',
      level: 'Mid',
    },
    {
      name: 'Eve',
      age: 32,
      salary: 65000,
      department: 'Sales',
      level: 'Senior',
    },
  ];

  await client.batchInsert('employees', complexData);

  // Complex query with multiple filters
  const seniorEngineers = await client.find('employees', {
    department: 'Engineering',
    level: 'Senior',
    age: { $gte: 25, $lte: 35 },
    salary: { $gte: 50000 },
    limit: 10,
  });

  console.log('Senior engineers found:', seniorEngineers.documents.length);
  seniorEngineers.documents.forEach((emp) => {
    console.log(`- ${emp.name}: ${emp.age} years old, $${emp.salary}`);
  });
}

async function demonstrateErrorHandling() {
  try {
    // Try to get a non-existent document
    await client.getById('employees', 'non-existent-id');
  } catch (error) {
    console.log('Expected error for non-existent document:', error.message);
  }

  try {
    // Try to create an index on _id (should fail)
    await client.createIndex('employees', '_id');
  } catch (error) {
    console.log('Expected error for _id index:', error.message);
  }

  try {
    // Try to insert invalid data
    await client.insert('employees', {
      // Missing required fields or invalid data
    });
  } catch (error) {
    console.log('Expected error for invalid data:', error.message);
  }
}

async function demonstrateCustomHttpClient() {
  const httpClient = client.getHttpClient();

  // Add request interceptor for logging
  httpClient.interceptors.request.use((config) => {
    console.log(
      `Making ${config.method?.toUpperCase()} request to ${config.url}`
    );
    return config;
  });

  // Add response interceptor for timing
  httpClient.interceptors.response.use(
    (response) => {
      console.log(
        `Request completed in ${
          Date.now() - response.config.metadata?.startTime
        }ms`
      );
      return response;
    },
    (error) => {
      console.log(`Request failed: ${error.message}`);
      return Promise.reject(error);
    }
  );

  // Add timing metadata
  httpClient.interceptors.request.use((config) => {
    config.metadata = { startTime: Date.now() };
    return config;
  });

  // Make a request with custom logging
  await client.health();
}

async function demonstratePerformanceTesting() {
  console.log('Running performance test...');

  const startTime = Date.now();
  const batchSize = 100;
  const totalDocuments = 1000;

  // Create test data
  const testData = Array.from({ length: totalDocuments }, (_, i) => ({
    id: i,
    name: `Performance Test User ${i}`,
    email: `perf${i}@example.com`,
    value: Math.random() * 1000,
    timestamp: new Date().toISOString(),
  }));

  // Batch insert in chunks
  const insertStart = Date.now();
  for (let i = 0; i < totalDocuments; i += batchSize) {
    const chunk = testData.slice(i, i + batchSize);
    await client.batchInsert('performance_test', chunk);
  }
  const insertTime = Date.now() - insertStart;

  // Query performance
  const queryStart = Date.now();
  const results = await client.find('performance_test', {
    limit: 100,
    value: { $gte: 500 },
  });
  const queryTime = Date.now() - queryStart;

  const totalTime = Date.now() - startTime;

  console.log('Performance Results:');
  console.log(`- Total documents: ${totalDocuments}`);
  console.log(
    `- Insert time: ${insertTime}ms (${(
      (totalDocuments / insertTime) *
      1000
    ).toFixed(2)} docs/sec)`
  );
  console.log(`- Query time: ${queryTime}ms`);
  console.log(`- Total time: ${totalTime}ms`);
  console.log(`- Documents found: ${results.documents.length}`);

  // Clean up
  console.log('Cleaning up performance test data...');
  // Note: In a real scenario, you might want to delete the test collection
  // This is just for demonstration
}

// Run the examples
if (require.main === module) {
  advancedExamples().catch(console.error);
}

export { advancedExamples };
