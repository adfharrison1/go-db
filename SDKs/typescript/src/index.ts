/**
 * go-db TypeScript SDK
 *
 * A comprehensive TypeScript SDK for the go-db document database API.
 *
 * @example
 * ```typescript
 * import { GoDBClient } from '@go-db/typescript-sdk';
 *
 * const client = new GoDBClient({
 *   baseURL: 'http://localhost:8080'
 * });
 *
 * // Insert a document
 * const document = await client.insert('users', {
 *   name: 'John Doe',
 *   email: 'john@example.com',
 *   age: 30
 * });
 *
 * // Find documents
 * const results = await client.find('users', {
 *   age: { $gte: 18 },
 *   limit: 10
 * });
 * ```
 */

// Export the main client class
export { GoDBClient } from './client';

// Export all types
export * from './types';

// Import for internal use
import { GoDBClient } from './client';
import { GoDBClientConfig } from './types';

// Export a default instance factory
export function createClient<S extends import('./types').CollectionsSchema>(
  config?: GoDBClientConfig
): import('./types').TypedGoDBClient<S> {
  return new GoDBClient<S>(config);
}

// Version information
export const VERSION = '1.0.0';
