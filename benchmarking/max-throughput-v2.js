import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '15s', target: 100 }, // Warm up
    { duration: '15s', target: 500 }, // Ramp to 500 VUs
    { duration: '15s', target: 1000 }, // Ramp to 1000 VUs
    { duration: '15s', target: 1500 }, // Ramp to 1500 VUs
    { duration: '15s', target: 2000 }, // Ramp to 2000 VUs
    { duration: '15s', target: 2500 }, // Ramp to 2500 VUs
    { duration: '15s', target: 0 }, // Ramp down
  ],
  thresholds: {
    http_req_failed: ['rate<0.10'], // Allow 10% error rate for max throughput testing
    http_req_duration: ['p(95)<2000'], // Allow higher latency for max throughput
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_max_throughput_v2';

export function setup() {
  console.log('Setting up V2 max throughput test...');
  console.log(
    'Testing maximum possible throughput with pure insert operations'
  );
  console.log('Note: V2 engine uses unique timestamp-based IDs');

  // Create collection with some initial data
  const initialData = [];
  for (let i = 0; i < 10; i++) {
    initialData.push({
      name: `InitialUser_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `initial_${i}@example.com`,
    });
  }

  const batchInsertPayload = JSON.stringify({ documents: initialData });
  const response = http.post(
    `${BASE_URL}/collections/${COLLECTION}/batch`,
    batchInsertPayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  if (response.status !== 201) {
    console.log('Failed to insert initial data');
  }

  console.log('V2 max throughput test setup complete');
  return {};
}

export default function (data) {
  // Pure insert operations only - no artificial delays
  const payload = JSON.stringify({
    name: `User_${__VU}_${__ITER}`,
    age: Math.floor(Math.random() * 50) + 18,
    email: `user_${__VU}_${__ITER}@example.com`,
    city: ['New York', 'London', 'Tokyo', 'Paris'][
      Math.floor(Math.random() * 4)
    ],
    department: ['Engineering', 'Sales', 'Marketing', 'HR'][
      Math.floor(Math.random() * 4)
    ],
    timestamp: Date.now(),
    random_data: Math.random().toString(36).substring(7),
  });

  const response = http.post(`${BASE_URL}/collections/${COLLECTION}`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  const success = check(response, {
    'insert status is 201': (r) => r.status === 201,
    'response time < 5s': (r) => r.timings.duration < 5000,
  });

  if (!success) {
    errorRate.add(1);
  }

  // No sleep() - maximum throughput testing
}

export function teardown(data) {
  console.log('V2 max throughput test teardown complete');
}
