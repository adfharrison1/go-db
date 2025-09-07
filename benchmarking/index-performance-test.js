import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '15s', target: 15 }, // Ramp up to 15 users
    { duration: '30s', target: 15 }, // Stay at 15 users
    { duration: '15s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'], // 95% of requests under 200ms (increased for large dataset)
    http_req_failed: ['rate<0.1'], // Error rate under 10%
    errors: ['rate<0.1'], // Error rate under 10%
  },
  setupTimeout: '5m', // Increase setup timeout to 5 minutes for large dataset insertion
};

const BASE_URL = 'http://localhost:8080';
const INDEXED_COLLECTION = 'test_indexed';
const NON_INDEXED_COLLECTION = 'test_non_indexed';

// Global counters for progress tracking
let totalQueries = 0;
let validQueries = 0;
let invalidQueries = 0;
let indexedWins = 0;
let nonIndexedWins = 0;

export function setup() {
  console.log('Setting up proper index comparison test with large dataset...');

  // Create large dataset for meaningful comparison
  const totalDocs = 100000; // 100,000 documents for significant performance difference
  const batchSize = 1000; // API limit per batch
  const numBatches = Math.ceil(totalDocs / batchSize);

  console.log(
    `🌱 SEEDING DATASET: Creating ${totalDocs.toLocaleString()} documents in ${numBatches} batches of ${batchSize}...`
  );
  console.log(
    `🌱 This will take approximately 3-5 minutes. Progress updates every 10 batches.`
  );

  // Insert data into both collections in batches
  let indexedSuccess = 0;
  let nonIndexedSuccess = 0;

  for (let batch = 0; batch < numBatches; batch++) {
    const startIdx = batch * batchSize;
    const endIdx = Math.min(startIdx + batchSize, totalDocs);
    const batchData = [];

    for (let i = startIdx; i < endIdx; i++) {
      batchData.push({
        name: `User_${i}`,
        age: Math.floor(Math.random() * 50) + 18, // Ages 18-67
        email: `user_${i}@example.com`,
        city: ['New York', 'London', 'Tokyo', 'Paris'][
          Math.floor(Math.random() * 4)
        ],
        salary: Math.floor(Math.random() * 100000) + 30000, // Salary 30k-130k
        department: ['Engineering', 'Sales', 'Marketing', 'HR'][
          Math.floor(Math.random() * 4)
        ],
      });
    }

    const batchInsertPayload = JSON.stringify({ documents: batchData });

    // Insert into indexed collection
    const indexedInsertResponse = http.post(
      `${BASE_URL}/collections/${INDEXED_COLLECTION}/batch`,
      batchInsertPayload,
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );

    if (indexedInsertResponse.status === 201) {
      indexedSuccess += batchData.length;
    } else {
      console.log(
        `Failed to insert batch ${batch + 1} into indexed collection: ${
          indexedInsertResponse.status
        }`
      );
    }

    // Insert into non-indexed collection
    const nonIndexedInsertResponse = http.post(
      `${BASE_URL}/collections/${NON_INDEXED_COLLECTION}/batch`,
      batchInsertPayload,
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );

    if (nonIndexedInsertResponse.status === 201) {
      nonIndexedSuccess += batchData.length;
    } else {
      console.log(
        `Failed to insert batch ${batch + 1} into non-indexed collection: ${
          nonIndexedInsertResponse.status
        }`
      );
    }

    // Progress update every 10 batches with more detail
    if ((batch + 1) % 10 === 0) {
      const progressPercent = (((batch + 1) / numBatches) * 100).toFixed(1);
      const docsInserted = (batch + 1) * batchSize;
      console.log(
        `🌱 SEEDING PROGRESS: ${
          batch + 1
        }/${numBatches} batches (${progressPercent}%) - ${docsInserted.toLocaleString()} documents inserted`
      );
    }
  }

  console.log(
    `🌱 SEEDING COMPLETE: Inserted ${indexedSuccess.toLocaleString()} documents into indexed collection`
  );
  console.log(
    `🌱 SEEDING COMPLETE: Inserted ${nonIndexedSuccess.toLocaleString()} documents into non-indexed collection`
  );

  // Create index ONLY on the indexed collection
  console.log('🌱 Creating age index on indexed collection...');
  const createIndexResponse = http.post(
    `${BASE_URL}/collections/${INDEXED_COLLECTION}/indexes/age`
  );

  if (createIndexResponse.status !== 201) {
    console.log('Failed to create age index');
  } else {
    console.log('Successfully created age index');
  }

  // Verify setup
  const indexedIndexes = http.get(
    `${BASE_URL}/collections/${INDEXED_COLLECTION}/indexes`
  );
  const nonIndexedIndexes = http.get(
    `${BASE_URL}/collections/${NON_INDEXED_COLLECTION}/indexes`
  );

  console.log(
    'Indexed collection indexes:',
    JSON.parse(indexedIndexes.body).indexes
  );
  console.log(
    'Non-indexed collection indexes:',
    JSON.parse(nonIndexedIndexes.body).indexes
  );
  console.log('🌱 Setup complete - ready for performance comparison');

  return {};
}

export default function () {
  // Test the SAME query on BOTH collections for proper comparison
  const testAge = Math.floor(Math.random() * 50) + 18;
  totalQueries++;

  // Query indexed collection (should be fast)
  const indexedQueryResponse = http.get(
    `${BASE_URL}/collections/${INDEXED_COLLECTION}/find?age=${testAge}`
  );

  check(indexedQueryResponse, {
    'indexed query status is 200': (r) => r.status === 200,
    'indexed query returns valid response': (r) => {
      if (r.status !== 200) return false;
      try {
        const parsed = JSON.parse(r.body);
        return Array.isArray(parsed.documents);
      } catch (e) {
        console.log(`Indexed query JSON parse error: ${e.message}`);
        return false;
      }
    },
  });

  if (indexedQueryResponse.status !== 200) {
    console.log(
      `Indexed query failed with status ${indexedQueryResponse.status}`
    );
    errorRate.add(1);
  }

  // Query non-indexed collection (should be slower)
  const nonIndexedQueryResponse = http.get(
    `${BASE_URL}/collections/${NON_INDEXED_COLLECTION}/find?age=${testAge}`
  );

  check(nonIndexedQueryResponse, {
    'non-indexed query status is 200': (r) => r.status === 200,
    'non-indexed query returns valid response': (r) => {
      if (r.status !== 200) return false;
      try {
        const parsed = JSON.parse(r.body);
        return Array.isArray(parsed.documents);
      } catch (e) {
        console.log(`Non-indexed query JSON parse error: ${e.message}`);
        return false;
      }
    },
  });

  if (nonIndexedQueryResponse.status !== 200) {
    console.log(
      `Non-indexed query failed with status ${nonIndexedQueryResponse.status}`
    );
    errorRate.add(1);
  }

  // Log performance comparison for this iteration
  if (
    indexedQueryResponse.status === 200 &&
    nonIndexedQueryResponse.status === 200
  ) {
    const indexedDuration = indexedQueryResponse.timings.duration;
    const nonIndexedDuration = nonIndexedQueryResponse.timings.duration;
    const speedup = nonIndexedDuration / indexedDuration;

    // Parse results and validate data consistency
    const indexedData = JSON.parse(indexedQueryResponse.body).documents;
    const nonIndexedData = JSON.parse(nonIndexedQueryResponse.body).documents;

    const indexedResults = indexedData.length;
    const nonIndexedResults = nonIndexedData.length;

    // CRITICAL: Validate that both queries return exactly the same data
    if (indexedResults !== nonIndexedResults) {
      console.log(
        `❌ DATA MISMATCH: Age ${testAge} - Indexed: ${indexedResults} results, Non-indexed: ${nonIndexedResults} results`
      );
      invalidQueries++;
      errorRate.add(1);
      return; // Skip this iteration as data is inconsistent
    }

    // Sort both result sets by _id to ensure consistent comparison
    const sortedIndexed = indexedData.sort((a, b) =>
      a._id.localeCompare(b._id)
    );
    const sortedNonIndexed = nonIndexedData.sort((a, b) =>
      a._id.localeCompare(b._id)
    );

    // Validate that all documents match exactly
    let dataMatches = true;
    for (let i = 0; i < sortedIndexed.length; i++) {
      if (
        sortedIndexed[i]._id !== sortedNonIndexed[i]._id ||
        sortedIndexed[i].age !== sortedNonIndexed[i].age
      ) {
        console.log(
          `❌ DOCUMENT MISMATCH: Age ${testAge} - Document ${i} differs between indexed and non-indexed results`
        );
        invalidQueries++;
        dataMatches = false;
        break;
      }
    }

    if (!dataMatches) {
      errorRate.add(1);
      return; // Skip this iteration as data is inconsistent
    }

    // Track valid queries and wins
    validQueries++;
    if (speedup > 1) {
      indexedWins++;
    } else if (speedup < 1) {
      nonIndexedWins++;
    }

    // Show progress every 10 queries
    if (validQueries % 10 === 0) {
      const winRate = ((indexedWins / validQueries) * 100).toFixed(1);
      console.log(
        `📊 Progress: ${validQueries} valid queries, ${winRate}% indexed wins, ${invalidQueries} invalid`
      );
    }

    console.log(
      `✅ Age ${testAge}: Indexed=${indexedDuration.toFixed(
        2
      )}ms (${indexedResults} results), Non-indexed=${nonIndexedDuration.toFixed(
        2
      )}ms (${nonIndexedResults} results), Speedup=${speedup.toFixed(2)}x`
    );
  }

  sleep(0.1);
}

// Add a teardown function to show final summary
export function teardown() {
  console.log('\n🏁 TEST COMPLETED - FINAL SUMMARY:');
  console.log(`   Total queries attempted: ${totalQueries}`);
  console.log(`   Valid queries: ${validQueries}`);
  console.log(`   Invalid queries: ${invalidQueries}`);
  if (validQueries > 0) {
    const winRate = ((indexedWins / validQueries) * 100).toFixed(1);
    console.log(`   Indexed wins: ${indexedWins} (${winRate}%)`);
    console.log(`   Non-indexed wins: ${nonIndexedWins}`);
    console.log(`   Ties: ${validQueries - indexedWins - nonIndexedWins}`);
  }
  console.log('');
}
