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
    http_req_duration: ['p(95)<1000'], // 95% of requests under 1000ms
    http_req_failed: ['rate<0.01'], // Error rate under 1%
    errors: ['rate<0.01'],
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_stress';

export function setup() {
  // Setup: Insert some initial data
  console.log('Setting up stress test...');

  const initialData = [];
  for (let i = 0; i < 50; i++) {
    initialData.push({
      name: `StressUser_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `stress_${i}@example.com`,
      city: ['New York', 'London', 'Tokyo', 'Paris'][
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

  console.log('Stress test setup complete');
  return {};
}

export default function (data) {
  const operation = Math.floor(Math.random() * 4); // 0-3 for different operations

  switch (operation) {
    case 0:
      // Insert operation
      const insertPayload = JSON.stringify({
        name: `StressUser_${__VU}_${__ITER}`,
        age: Math.floor(Math.random() * 50) + 18,
        email: `stress_${__VU}_${__ITER}@example.com`,
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
        'stress insert status is 201': (r) => r.status === 201,
      });

      if (!insertSuccess) {
        errorRate.add(1);
      }
      break;

    case 1:
      // Find operation
      const findResponse = http.get(
        `${BASE_URL}/collections/${COLLECTION}/find?limit=20`
      );

      const findSuccess = check(findResponse, {
        'stress find status is 200': (r) => r.status === 200,
      });

      if (!findSuccess) {
        errorRate.add(1);
      }
      break;

    case 2:
      // Batch insert operation
      const batchSize = Math.floor(Math.random() * 20) + 5; // 5-25 documents
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

    case 3:
      // Streaming operation
      const streamingResponse = http.get(
        `${BASE_URL}/collections/${COLLECTION}/find_with_stream`
      );

      const streamingSuccess = check(streamingResponse, {
        'stress streaming status is 200': (r) => r.status === 200,
      });

      if (!streamingSuccess) {
        errorRate.add(1);
      }
      break;
  }

  sleep(Math.random() * 0.5); // Random delay between 0-0.5 seconds
}
