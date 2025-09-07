import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '30s', target: 8 }, // Ramp up to 8 users
    { duration: '1m', target: 8 }, // Stay at 8 users
    { duration: '30s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests under 500ms
    http_req_failed: ['rate<0.1'], // Error rate under 10%
    errors: ['rate<0.1'],
  },
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = 'test_streaming';

export function setup() {
  // Setup: Insert large dataset for streaming
  console.log('Setting up streaming test...');

  const largeDataset = [];
  for (let i = 0; i < 1000; i++) {
    largeDataset.push({
      name: `StreamUser_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `stream_${i}@example.com`,
      city: ['New York', 'London', 'Tokyo', 'Paris'][
        Math.floor(Math.random() * 4)
      ],
      data: 'x'.repeat(100), // 100 character string to make documents larger
    });
  }

  const batchInsertPayload = JSON.stringify({ documents: largeDataset });
  const batchInsertResponse = http.post(
    `${BASE_URL}/collections/${COLLECTION}/batch`,
    batchInsertPayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  if (batchInsertResponse.status !== 201) {
    console.log('Failed to insert large dataset');
  }

  console.log('Streaming setup complete');
  return {};
}

export default function (data) {
  // Test streaming endpoint
  const streamingResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/find_with_stream`
  );

  const streamingSuccess = check(streamingResponse, {
    'streaming status is 200': (r) => r.status === 200,
    'streaming returns data': (r) => r.body.length > 0,
  });

  if (!streamingSuccess) {
    errorRate.add(1);
  }

  // Test streaming with filters
  const testAge = Math.floor(Math.random() * 50) + 18;
  const filteredStreamingResponse = http.get(
    `${BASE_URL}/collections/${COLLECTION}/find_with_stream?age=${testAge}`
  );

  const filteredStreamingSuccess = check(filteredStreamingResponse, {
    'filtered streaming status is 200': (r) => r.status === 200,
    'filtered streaming returns data': (r) => r.body.length > 0,
  });

  if (!filteredStreamingSuccess) {
    errorRate.add(1);
  }

  sleep(0.2); // Longer delay for streaming operations
}
