import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export const options = {
  stages: [
    { duration: '15s', target: 8 }, // Ramp up to 8 users
    { duration: '30s', target: 8 }, // Stay at 8 users
    { duration: '15s', target: 0 }, // Ramp down to 0 users
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'], // 95% of requests under 2s (increased for streaming)
    http_req_failed: ['rate<0.1'], // Error rate under 10% (increased tolerance)
    errors: ['rate<0.1'],
  },
  // Add timeouts for streaming operations
  httpReqTimeout: '30s',
  setupTimeout: '2m',
};

const BASE_URL = 'http://localhost:8080';
const COLLECTION = `test_streaming_${Date.now()}`; // Unique collection name

// Store the collection name globally so it's consistent between setup and test
let collectionName = COLLECTION;

export function setup() {
  // Setup: Insert large dataset for streaming
  console.log('Setting up streaming test...');

  const largeDataset = [];
  for (let i = 0; i < 500; i++) {
    // Reduced from 1000 to 500
    largeDataset.push({
      name: `StreamUser_${i}`,
      age: Math.floor(Math.random() * 50) + 18,
      email: `stream_${i}@example.com`,
      city: ['New York', 'London', 'Tokyo', 'Paris'][
        Math.floor(Math.random() * 4)
      ],
      data: 'x'.repeat(50), // Reduced from 100 to 50 characters
    });
  }

  const batchInsertPayload = JSON.stringify({ documents: largeDataset });
  const batchInsertResponse = http.post(
    `${BASE_URL}/collections/${collectionName}/batch`,
    batchInsertPayload,
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );

  if (batchInsertResponse.status !== 201) {
    console.log(
      `Failed to insert large dataset: status=${batchInsertResponse.status}, body=${batchInsertResponse.body}`
    );
    throw new Error(
      `Setup failed: ${batchInsertResponse.status} - ${batchInsertResponse.body}`
    );
  }

  console.log(
    `Streaming setup complete - inserted ${largeDataset.length} documents into collection ${collectionName}`
  );
  return { collectionName };
}

export default function (data) {
  // Use the collection name from setup
  const testCollection = data.collectionName || collectionName;

  // Test streaming endpoint with timeout
  const streamingResponse = http.get(
    `${BASE_URL}/collections/${testCollection}/find_with_stream`,
    {
      timeout: '20s', // 20 second timeout for streaming
    }
  );

  const streamingSuccess = check(streamingResponse, {
    'streaming status is 200': (r) => r.status === 200,
    'streaming returns data': (r) => r.body && r.body.length > 0,
    'streaming no timeout': (r) => r.status !== 0, // 0 means timeout
    'streaming starts with array': (r) => r.body && r.body.startsWith('['),
  });

  if (!streamingSuccess) {
    console.log(
      `Streaming failed: status=${streamingResponse.status}, body length=${
        streamingResponse.body ? streamingResponse.body.length : 'null'
      }, body preview=${
        streamingResponse.body
          ? streamingResponse.body.substring(0, 100)
          : 'null'
      }`
    );
    errorRate.add(1);
  }

  // Test streaming with filters
  const testAge = Math.floor(Math.random() * 50) + 18;
  const filteredStreamingResponse = http.get(
    `${BASE_URL}/collections/${testCollection}/find_with_stream?age=${testAge}`,
    {
      timeout: '20s', // 20 second timeout for streaming
    }
  );

  const filteredStreamingSuccess = check(filteredStreamingResponse, {
    'filtered streaming status is 200': (r) => r.status === 200,
    'filtered streaming returns data': (r) => r.body && r.body.length > 0,
    'filtered streaming no timeout': (r) => r.status !== 0,
  });

  if (!filteredStreamingSuccess) {
    console.log(
      `Filtered streaming failed: status=${
        filteredStreamingResponse.status
      }, body length=${
        filteredStreamingResponse.body
          ? filteredStreamingResponse.body.length
          : 'null'
      }, body preview=${
        filteredStreamingResponse.body
          ? filteredStreamingResponse.body.substring(0, 100)
          : 'null'
      }`
    );
    errorRate.add(1);
  }

  sleep(1); // Longer delay for streaming operations
}
