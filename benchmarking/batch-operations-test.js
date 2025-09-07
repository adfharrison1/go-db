import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '30s', target: 5 }, // Ramp up to 5 users
    { duration: '2m', target: 5 }, // Stay at 5 users
    { duration: '30s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'], // 95% of requests under 200ms
    http_req_failed: ['rate<0.05'], // Error rate under 5%
    errors: ['rate<0.05'],
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_batch';

export default function () {
  // 1. Batch Insert
  const batchSize = Math.floor(Math.random() * 50) + 10; // 10-60 documents
  const documents = [];

  for (let i = 0; i < batchSize; i++) {
    documents.push({
      name: `BatchUser_${__VU}_${__ITER}_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `batch_${__VU}_${__ITER}_${i}@example.com`,
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
    'batch insert status is 201': (r) => r.status === 201,
    'batch insert returns documents': (r) =>
      JSON.parse(r.body).documents.length === batchSize,
  });

  if (!batchInsertSuccess) {
    errorRate.add(1);
    return;
  }

  const insertedDocs = JSON.parse(batchInsertResponse.body).documents;

  // 2. Batch Update
  const updateOperations = insertedDocs
    .slice(0, Math.min(10, insertedDocs.length))
    .map((doc) => ({
      id: doc._id,
      updates: {
        age: Math.floor(Math.random() * 50) + 18,
        salary: Math.floor(Math.random() * 50000) + 30000,
        status: ['active', 'inactive', 'pending'][
          Math.floor(Math.random() * 3)
        ],
      },
    }));

  const batchUpdatePayload = JSON.stringify({ operations: updateOperations });

  const batchUpdateResponse = http.patch(
    `${BASE_URL}/collections/${COLLECTION}/batch`,
    batchUpdatePayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  const batchUpdateSuccess = check(batchUpdateResponse, {
    'batch update status is 200': (r) => r.status === 200,
    'batch update returns documents': (r) =>
      JSON.parse(r.body).documents.length === updateOperations.length,
  });

  if (!batchUpdateSuccess) {
    errorRate.add(1);
  }

  sleep(0.5); // Longer delay for batch operations
}
