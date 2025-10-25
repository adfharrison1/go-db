/**
 * Simple unit tests for the go-db TypeScript SDK
 * Tests the SDK without requiring a running database
 */

import { GoDBClient } from '../index';

// Test schema definition
type TestUser = {
  _id?: string;
  name: string;
  age: number;
  email: string;
  active: boolean;
};

type TestSchema = {
  users: TestUser;
};

describe('GoDB TypeScript SDK Unit Tests', () => {
  let client: GoDBClient<TestSchema>;

  beforeEach(() => {
    client = new GoDBClient<TestSchema>({
      baseURL: 'http://localhost:8080',
      timeout: 5000,
    });
  });

  describe('Client Initialization', () => {
    it('should create a client with default configuration', () => {
      const defaultClient = new GoDBClient<TestSchema>();
      expect(defaultClient).toBeDefined();
      expect(defaultClient.getBaseURL()).toBe('http://localhost:8080');
    });

    it('should create a client with custom configuration', () => {
      const customClient = new GoDBClient<TestSchema>({
        baseURL: 'http://custom:9090',
        timeout: 10000,
      });
      expect(customClient).toBeDefined();
      expect(customClient.getBaseURL()).toBe('http://custom:9090');
    });
  });

  describe('Type Safety', () => {
    it('should enforce correct types for user data', () => {
      // This test verifies that TypeScript types are working correctly
      const validUser: Omit<TestUser, '_id'> = {
        name: 'John Doe',
        age: 30,
        email: 'john@example.com',
        active: true,
      };

      // These should compile without TypeScript errors
      expect(validUser.name).toBe('John Doe');
      expect(validUser.age).toBe(30);
      expect(validUser.email).toBe('john@example.com');
      expect(validUser.active).toBe(true);
    });

    it('should handle optional fields correctly', () => {
      const userWithOptional: Omit<TestUser, '_id'> = {
        name: 'Jane Doe',
        age: 25,
        email: 'jane@example.com',
        active: false,
      };

      expect(userWithOptional).toBeDefined();
      expect(userWithOptional.name).toBe('Jane Doe');
    });
  });

  describe('Error Handling', () => {
    it('should handle network errors gracefully', async () => {
      // This test will fail because there's no server running
      // But it tests that the error handling works
      try {
        await client.health();
        fail('Expected an error to be thrown');
      } catch (error: any) {
        expect(error).toBeDefined();
        expect(error.message).toContain('Network Error');
      }
    });

    it('should handle invalid URLs', async () => {
      const invalidClient = new GoDBClient<TestSchema>({
        baseURL: 'http://invalid-url-that-does-not-exist:9999',
        timeout: 1000,
      });

      try {
        await invalidClient.health();
        fail('Expected an error to be thrown');
      } catch (error: any) {
        expect(error).toBeDefined();
      }
    });
  });

  describe('HTTP Client Access', () => {
    it('should provide access to the underlying HTTP client', () => {
      const httpClient = client.getHttpClient();
      expect(httpClient).toBeDefined();
      expect(httpClient.defaults.baseURL).toBe('http://localhost:8080');
    });
  });
});
