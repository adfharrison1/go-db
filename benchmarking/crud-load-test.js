import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

// Custom metrics
const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '15s', target: 10 }, // Ramp up to 10 users
    { duration: '30s', target: 10 }, // Stay at 10 users
    { duration: '15s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<100'], // 95% of requests under 100ms
    http_req_failed: ['rate<0.1'], // Error rate under 10%
    errors: ['rate<0.1'],
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_users';

export default function () {
  // 1. Insert a document
  const insertPayload = JSON.stringify({
    name: `User_${__VU}_${__ITER}`,
    age: Math.floor(Math.random() * 50) + 18,
    email: `user_${__VU}_${__ITER}@example.com`,
    city: ['New York', 'London', 'Tokyo', 'Paris'][
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
    'insert status is 201': (r) => r.status === 201,
    'insert has _id': (r) => JSON.parse(r.body)._id !== undefined,
  });

  if (!insertSuccess) {
    errorRate.add(1);
    return;
  }

  const docId = JSON.parse(insertResponse.body)._id;

  // 2. Get document by ID
  const getResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/documents/${docId}`
  );

  const getSuccess = check(getResponse, {
    'get status is 200': (r) => r.status === 200,
    'get returns document': (r) => JSON.parse(r.body)._id === docId,
  });

  if (!getSuccess) {
    errorRate.add(1);
  }

  // 3. Update document (PATCH)
  const updatePayload = JSON.stringify({
    age: Math.floor(Math.random() * 50) + 18,
    city: ['New York', 'London', 'Tokyo', 'Paris'][
      Math.floor(Math.random() * 4)
    ],
  });

  const updateResponse = http.patch(
    `${BASE_URL}/collections/${COLLECTION}/documents/${docId}`,
    updatePayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  const updateSuccess = check(updateResponse, {
    'update status is 200': (r) => r.status === 200,
    'update returns document': (r) => JSON.parse(r.body)._id === docId,
  });

  if (!updateSuccess) {
    errorRate.add(1);
  }

  // 4. Find documents
  const findResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/find?limit=10`
  );

  const findSuccess = check(findResponse, {
    'find status is 200': (r) => r.status === 200,
    'find returns array': (r) => Array.isArray(JSON.parse(r.body).documents),
  });

  if (!findSuccess) {
    errorRate.add(1);
  }

  // 5. Delete document
  const deleteResponse = http.del(
    `${BASE_URL}/collections/${COLLECTION}/documents/${docId}`
  );

  const deleteSuccess = check(deleteResponse, {
    'delete status is 204': (r) => r.status === 204,
  });

  if (!deleteSuccess) {
    errorRate.add(1);
  }

  sleep(0.1); // Small delay between iterations
}
