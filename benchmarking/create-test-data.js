#!/usr/bin/env node

/**
 * Create Test Data for V2 Persistence Test
 * Creates 1000 documents using batch insert for testing data persistence
 */

const http = require('http');

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'persistence_test';
const DOCUMENT_COUNT = 1000;
const BATCH_SIZE = 50; // Insert 50 documents per batch

// Colors for console output
const colors = {
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  reset: '\x1b[0m',
};

function log(message, color = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

function makeRequest(method, path, data = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 8080,
      path: path,
      method: method,
      headers: {
        'Content-Type': 'application/json',
      },
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => {
        body += chunk;
      });
      res.on('end', () => {
        try {
          const parsed = body ? JSON.parse(body) : {};
          resolve({ status: res.statusCode, data: parsed, body });
        } catch (e) {
          resolve({ status: res.statusCode, data: null, body });
        }
      });
    });

    req.on('error', (err) => {
      reject(err);
    });

    if (data) {
      req.write(JSON.stringify(data));
    }
    req.end();
  });
}

function generateDocument(id) {
  return {
    id: id,
    name: `Test Document ${id}`,
    value: Math.floor(Math.random() * 10000),
    timestamp: new Date().toISOString(),
    metadata: {
      created_by: 'persistence-test',
      version: '1.0',
      tags: ['test', 'persistence', `batch-${Math.floor(id / BATCH_SIZE)}`],
    },
    data: {
      random_string: `random_${Math.random().toString(36).substr(2, 9)}`,
      random_number: Math.floor(Math.random() * 1000000),
      random_boolean: Math.random() > 0.5,
    },
  };
}

async function createTestData() {
  log(`🚀 Starting test data creation...`, 'blue');
  log(
    `📊 Target: ${DOCUMENT_COUNT} documents in batches of ${BATCH_SIZE}`,
    'blue'
  );

  const startTime = Date.now();
  let totalCreated = 0;
  let batchNumber = 0;

  try {
    // Create collection first (by inserting first document)
    log(`📝 Creating collection '${COLLECTION}'...`, 'yellow');

    for (let i = 0; i < DOCUMENT_COUNT; i += BATCH_SIZE) {
      batchNumber++;
      const batchEnd = Math.min(i + BATCH_SIZE, DOCUMENT_COUNT);
      const batchSize = batchEnd - i;

      log(
        `📦 Creating batch ${batchNumber} (documents ${i + 1}-${batchEnd})...`,
        'yellow'
      );

      // Generate batch of documents
      const documents = [];
      for (let j = i; j < batchEnd; j++) {
        documents.push(generateDocument(j + 1));
      }

      // Insert batch
      const batchRequest = {
        documents: documents,
      };
      const response = await makeRequest(
        'POST',
        `/collections/${COLLECTION}/batch`,
        batchRequest
      );

      if (response.status !== 201) {
        throw new Error(
          `Batch insert failed: ${response.status} - ${JSON.stringify(
            response.data
          )}`
        );
      }

      totalCreated += batchSize;
      log(
        `✅ Batch ${batchNumber} created successfully (${batchSize} documents)`,
        'green'
      );

      // Small delay between batches to avoid overwhelming the server
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    const duration = Date.now() - startTime;
    log(
      `🎉 Successfully created ${totalCreated} documents in ${duration}ms`,
      'green'
    );
    log(
      `📈 Average: ${(totalCreated / (duration / 1000)).toFixed(
        2
      )} documents/second`,
      'green'
    );

    // Verify the count
    log(`🔍 Verifying document count...`, 'blue');
    const countResponse = await makeRequest(
      'GET',
      `/collections/${COLLECTION}/find?limit=1`
    );

    if (countResponse.status !== 200) {
      throw new Error(`Failed to verify count: ${countResponse.status}`);
    }

    const totalCount = countResponse.data.total || 0;
    log(`📊 Total documents in collection: ${totalCount}`, 'blue');

    if (totalCount !== DOCUMENT_COUNT) {
      throw new Error(
        `Document count mismatch: expected ${DOCUMENT_COUNT}, got ${totalCount}`
      );
    }

    log(`✅ Document count verification passed!`, 'green');

    // Save expected data for validation
    const expectedData = {
      collection: COLLECTION,
      totalCount: DOCUMENT_COUNT,
      createdAt: new Date().toISOString(),
      batches: Math.ceil(DOCUMENT_COUNT / BATCH_SIZE),
    };

    const fs = require('fs');
    fs.writeFileSync(
      './expected-data.json',
      JSON.stringify(expectedData, null, 2)
    );
    log(`💾 Expected data saved to expected-data.json`, 'blue');
  } catch (error) {
    log(`❌ Error creating test data: ${error.message}`, 'red');
    process.exit(1);
  }
}

// Run the test
createTestData().catch((error) => {
  log(`❌ Unexpected error: ${error.message}`, 'red');
  process.exit(1);
});
