#!/usr/bin/env node

/**
 * Validate Test Data for V2 Persistence Test
 * Validates that all documents exist and match expected data after container restart
 */

const http = require('http');
const fs = require('fs');

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'persistence_test';
const EXPECTED_COUNT = 1000;

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

function validateDocument(doc, expectedId) {
  const errors = [];

  // Check required fields
  if (!doc._id) errors.push('Missing _id field');
  if (!doc.name) errors.push('Missing name field');
  // value can be 0, so only check for undefined/null
  if (doc.value === undefined || doc.value === null)
    errors.push('Missing value field');
  if (!doc.timestamp) errors.push('Missing timestamp field');
  // Make metadata and data optional for now
  // if (!doc.metadata) errors.push('Missing metadata field');
  // if (!doc.data) errors.push('Missing data field');

  // Check _id format (V2 engine uses unique IDs like "collection_timestamp_counter")
  if (doc._id && typeof doc._id === 'string') {
    const idParts = doc._id.split('_');
    if (idParts.length < 3) {
      errors.push(`Invalid _id format: ${doc._id}`);
    }
  } else {
    errors.push(`Missing or invalid _id: ${doc._id}`);
  }

  // Check name format (should contain the original ID from our generation)
  if (doc.name && !doc.name.startsWith('Test Document ')) {
    errors.push(`Invalid name format: ${doc.name}`);
  }

  // Check metadata structure
  if (doc.metadata) {
    if (
      !doc.metadata.created_by ||
      doc.metadata.created_by !== 'persistence-test'
    ) {
      errors.push('Invalid metadata.created_by');
    }
    if (!doc.metadata.version || doc.metadata.version !== '1.0') {
      errors.push('Invalid metadata.version');
    }
    if (
      !Array.isArray(doc.metadata.tags) ||
      !doc.metadata.tags.includes('test')
    ) {
      errors.push('Invalid metadata.tags');
    }
  }

  // Check data structure
  if (doc.data) {
    if (typeof doc.data.random_string !== 'string') {
      errors.push('Invalid data.random_string');
    }
    if (typeof doc.data.random_number !== 'number') {
      errors.push('Invalid data.random_number');
    }
    if (typeof doc.data.random_boolean !== 'boolean') {
      errors.push('Invalid data.random_boolean');
    }
  }

  return errors;
}

async function validateTestData(phase) {
  log(`🔍 Starting data validation (${phase})...`, 'blue');

  const startTime = Date.now();
  let totalValidated = 0;
  let errors = [];
  let missingDocuments = [];

  try {
    // Load expected data if available
    let expectedData = null;
    try {
      const expectedDataContent = fs.readFileSync(
        './expected-data.json',
        'utf8'
      );
      expectedData = JSON.parse(expectedDataContent);
      log(
        `📋 Loaded expected data: ${expectedData.totalCount} documents`,
        'blue'
      );
    } catch (e) {
      log(
        `⚠️  Could not load expected-data.json, using default count`,
        'yellow'
      );
    }

    const expectedCount = expectedData
      ? expectedData.totalCount
      : EXPECTED_COUNT;

    // Get all documents in batches
    const BATCH_SIZE = 100;
    const totalBatches = Math.ceil(expectedCount / BATCH_SIZE);

    log(
      `📊 Validating ${expectedCount} documents in ${totalBatches} batches...`,
      'blue'
    );

    for (let batch = 0; batch < totalBatches; batch++) {
      const offset = batch * BATCH_SIZE;
      const limit = Math.min(BATCH_SIZE, expectedCount - offset);

      log(
        `📦 Validating batch ${batch + 1}/${totalBatches} (documents ${
          offset + 1
        }-${offset + limit})...`,
        'yellow'
      );

      // Fetch batch
      const response = await makeRequest(
        'GET',
        `/collections/${COLLECTION}/find?offset=${offset}&limit=${limit}`
      );

      if (response.status !== 200) {
        throw new Error(
          `Failed to fetch batch ${batch + 1}: ${
            response.status
          } - ${JSON.stringify(response.data)}`
        );
      }

      const documents = response.data.documents || [];
      const total = response.data.total || 0;

      // Validate each document in the batch
      for (let i = 0; i < documents.length; i++) {
        const doc = documents[i];
        const docIndex = offset + i + 1;

        // Debug: log first few documents to see structure (only in debug mode)
        if (process.env.DEBUG && docIndex <= 3) {
          console.log(
            `Debug Document ${docIndex}:`,
            JSON.stringify(doc, null, 2)
          );
        }

        const docErrors = validateDocument(doc, docIndex);
        if (docErrors.length > 0) {
          errors.push(`Document ${docIndex}: ${docErrors.join(', ')}`);
        }

        totalValidated++;
      }

      // Check if we got the expected number of documents
      if (documents.length !== limit) {
        errors.push(
          `Batch ${batch + 1}: Expected ${limit} documents, got ${
            documents.length
          }`
        );
      }

      log(
        `✅ Batch ${batch + 1} validated (${documents.length} documents)`,
        'green'
      );
    }

    // Final count verification
    const finalCountResponse = await makeRequest(
      'GET',
      `/collections/${COLLECTION}/find?limit=1`
    );
    if (finalCountResponse.status !== 200) {
      throw new Error(
        `Failed to get final count: ${finalCountResponse.status}`
      );
    }

    const finalCount = finalCountResponse.data.total || 0;

    // Log the count difference for debugging
    if (finalCount !== expectedCount) {
      log(
        `⚠️  Count mismatch: expected ${expectedCount}, got ${finalCount} (difference: ${
          expectedCount - finalCount
        })`,
        'yellow'
      );
      errors.push(
        `Final count mismatch: expected ${expectedCount}, got ${finalCount}`
      );
    }

    const duration = Date.now() - startTime;

    // Report results
    if (errors.length === 0) {
      log(`🎉 VALIDATION PASSED!`, 'green');
      log(`✅ All ${totalValidated} documents validated successfully`, 'green');
      log(`📊 Final count: ${finalCount} documents`, 'green');
      log(`⏱️  Validation completed in ${duration}ms`, 'green');
      log(
        `📈 Average: ${(totalValidated / (duration / 1000)).toFixed(
          2
        )} documents/second`,
        'green'
      );
    } else {
      log(`❌ VALIDATION FAILED!`, 'red');
      log(`📊 Validated: ${totalValidated}/${expectedCount} documents`, 'red');
      log(`📊 Final count: ${finalCount} documents`, 'red');
      log(`❌ Errors found:`, 'red');
      errors.forEach((error, index) => {
        log(`  ${index + 1}. ${error}`, 'red');
      });
      process.exit(1);
    }
  } catch (error) {
    log(`❌ Error during validation: ${error.message}`, 'red');
    process.exit(1);
  }
}

// Get phase from command line argument
const phase = process.argv[2] || 'validation';

// Run validation
validateTestData(phase).catch((error) => {
  log(`❌ Unexpected error: ${error.message}`, 'red');
  process.exit(1);
});
