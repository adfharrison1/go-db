import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '15s', target: 15 }, // Ramp up to 15 users
    { duration: '30m', target: 15 }, // Stay at 15 users
    { duration: '15s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<50'], // 95% of requests under 50ms
    http_req_failed: ['rate<0.1'], // Error rate under 10%
    errors: ['rate<0.1'],
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_indexed';

export function setup() {
  // Setup: Create index and insert test data
  console.log('Setting up index performance test...');

  // Create index on age field
  const createIndexResponse = http.post(
    `${BASE_URL}/collections/${COLLECTION}/indexes/age`
  );
  if (createIndexResponse.status !== 201) {
    console.log('Failed to create age index');
  }

  // Insert some test data
  const testData = [];
  for (let i = 0; i < 100; i++) {
    testData.push({
      name: `IndexUser_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `index_${i}@example.com`,
      city: ['New York', 'London', 'Tokyo', 'Paris'][
        Math.floor(Math.random() * 4)
      ],
    });
  }

  const batchInsertPayload = JSON.stringify({ documents: testData });
  const batchInsertResponse = http.post(
    `${BASE_URL}/collections/${COLLECTION}/batch`,
    batchInsertPayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  if (batchInsertResponse.status !== 201) {
    console.log('Failed to insert test data');
  }

  console.log('Setup complete');
  return {};
}

export default function (data) {
  // Test indexed queries
  const testAge = Math.floor(Math.random() * 50) + 18;

  const indexedQueryResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/find?age=${testAge}`
  );

  const indexedQuerySuccess = check(indexedQueryResponse, {
    'indexed query status is 200': (r) => r.status === 200,
    'indexed query returns results': (r) => {
      if (r.status !== 200) return false;
      try {
        const parsed = JSON.parse(r.body);
        return Array.isArray(parsed.documents);
      } catch (e) {
        console.log(
          `Indexed query JSON parse error: ${e.message}, body: ${r.body}`
        );
        return false;
      }
    },
  });

  if (!indexedQuerySuccess) {
    errorRate.add(1);
  }

  // Test non-indexed queries (city field)
  const testCity = ['New York', 'London', 'Tokyo', 'Paris'][
    Math.floor(Math.random() * 4)
  ];

  const nonIndexedQueryResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/find?city=${testCity}`
  );

  const nonIndexedQuerySuccess = check(nonIndexedQueryResponse, {
    'non-indexed query status is 200': (r) => r.status === 200,
    'non-indexed query returns results': (r) => {
      if (r.status !== 200) return false;
      try {
        const parsed = JSON.parse(r.body);
        return Array.isArray(parsed.documents);
      } catch (e) {
        console.log(
          `Non-indexed query JSON parse error: ${e.message}, body: ${r.body}`
        );
        return false;
      }
    },
  });

  if (!nonIndexedQuerySuccess) {
    errorRate.add(1);
  }

  // Test compound queries
  const compoundQueryResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/find?age=${testAge}&city=${testCity}`
  );

  const compoundQuerySuccess = check(compoundQueryResponse, {
    'compound query status is 200': (r) => r.status === 200,
    'compound query returns results': (r) => {
      if (r.status !== 200) return false;
      try {
        const parsed = JSON.parse(r.body);
        return Array.isArray(parsed.documents);
      } catch (e) {
        console.log(
          `Compound query JSON parse error: ${e.message}, body: ${r.body}`
        );
        return false;
      }
    },
  });

  if (!compoundQuerySuccess) {
    errorRate.add(1);
  }

  sleep(0.1);
}
