import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '15s', target: 20 }, // Ramp up to 20 users
    { duration: '15s', target: 50 }, // Ramp up to 50 users
    { duration: '30s', target: 100 }, // Ramp up to 100 users
    { duration: '30s', target: 100 }, // Stay at 100 users
    { duration: '15s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // More aggressive threshold for optimized config
    http_req_failed: ['rate<0.01'], // Error rate under 1%
    errors: ['rate<0.01'],
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_stress_optimized';

export function setup() {
  // Setup: Insert some initial data
  console.log('Setting up optimized stress test...');
  console.log(
    'Note: This test assumes go-db is configured with -transaction-save=false'
  );

  const initialData = [];
  for (let i = 0; i < 100; i++) {
    initialData.push({
      name: `StressUser_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `stress_${i}@example.com`,
      city: ['New York', 'London', 'Tokyo', 'Paris'][
        Math.floor(Math.random() * 4)
      ],
      department: ['Engineering', 'Sales', 'Marketing', 'HR'][
        Math.floor(Math.random() * 4)
      ],
    });
  }

  const batchInsertPayload = JSON.stringify({ documents: initialData });
  const batchInsertResponse = http.post(
    `${BASE_URL}/collections/${COLLECTION}/batch`,
    batchInsertPayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  if (batchInsertResponse.status !== 201) {
    console.log('Failed to insert initial data');
  }

  // Create some indexes for performance testing
  http.post(`${BASE_URL}/collections/${COLLECTION}/indexes/age`);
  http.post(`${BASE_URL}/collections/${COLLECTION}/indexes/department`);

  console.log('Optimized stress test setup complete');
  return {};
}

export default function (data) {
  const operation = Math.floor(Math.random() * 6); // 0-5 for different operations

  switch (operation) {
    case 0:
    case 1: // Weight read operations more heavily (realistic workload)
      // Find operation with indexed field
      const ageFilter = Math.floor(Math.random() * 50) + 18;
      const findResponse = http.get(
        `${BASE_URL}/collections/${COLLECTION}/find?age=${ageFilter}&limit=20`
      );

      const findSuccess = check(findResponse, {
        'stress find status is 200': (r) => r.status === 200,
      });

      if (!findSuccess) {
        errorRate.add(1);
      }
      break;

    case 2:
      // Insert operation
      const insertPayload = JSON.stringify({
        name: `StressUser_${__VU}_${__ITER}`,
        age: Math.floor(Math.random() * 50) + 18,
        email: `stress_${__VU}_${__ITER}@example.com`,
        city: ['New York', 'London', 'Tokyo', 'Paris'][
          Math.floor(Math.random() * 4)
        ],
        department: ['Engineering', 'Sales', 'Marketing', 'HR'][
          Math.floor(Math.random() * 4)
        ],
      });

      const insertResponse = http.post(
        `${BASE_URL}/collections/${COLLECTION}`,
        insertPayload,
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );

      const insertSuccess = check(insertResponse, {
        'stress insert status is 201': (r) => r.status === 201,
      });

      if (!insertSuccess) {
        errorRate.add(1);
      }
      break;

    case 3:
      // Batch insert operation (smaller batches for better concurrency)
      const batchSize = Math.floor(Math.random() * 10) + 5; // 5-15 documents
      const documents = [];

      for (let i = 0; i < batchSize; i++) {
        documents.push({
          name: `BatchStressUser_${__VU}_${__ITER}_${i}`,
          age: Math.floor(Math.random() * 50) + 18,
          email: `batch_stress_${__VU}_${__ITER}_${i}@example.com`,
          department: ['Engineering', 'Sales', 'Marketing', 'HR'][
            Math.floor(Math.random() * 4)
          ],
        });
      }

      const batchInsertPayload = JSON.stringify({ documents });
      const batchInsertResponse = http.post(
        `${BASE_URL}/collections/${COLLECTION}/batch`,
        batchInsertPayload,
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );

      const batchInsertSuccess = check(batchInsertResponse, {
        'stress batch insert status is 201': (r) => r.status === 201,
      });

      if (!batchInsertSuccess) {
        errorRate.add(1);
      }
      break;

    case 4:
      // Get by ID operation (if we have IDs)
      const randomId = Math.floor(Math.random() * 100) + 1;
      const getByIdResponse = http.get(
        `${BASE_URL}/collections/${COLLECTION}/documents/${randomId}`
      );

      const getByIdSuccess = check(getByIdResponse, {
        'stress get by id status is 200 or 404': (r) =>
          r.status === 200 || r.status === 404, // 404 is acceptable if document doesn't exist
      });

      if (!getByIdSuccess) {
        errorRate.add(1);
      }
      break;

    case 5:
      // Update operation (if document exists)
      const updateId = Math.floor(Math.random() * 100) + 1;
      const updatePayload = JSON.stringify({
        age: Math.floor(Math.random() * 50) + 18,
        last_updated: new Date().toISOString(),
      });

      const updateResponse = http.patch(
        `${BASE_URL}/collections/${COLLECTION}/documents/${updateId}`,
        updatePayload,
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );

      const updateSuccess = check(updateResponse, {
        'stress update status is 200 or 404': (r) =>
          r.status === 200 || r.status === 404, // 404 is acceptable if document doesn't exist
      });

      if (!updateSuccess) {
        errorRate.add(1);
      }
      break;
  }

  sleep(Math.random() * 0.2); // Shorter delay for more aggressive testing
}

export function teardown(data) {
  console.log('Optimized stress test teardown complete');
}
