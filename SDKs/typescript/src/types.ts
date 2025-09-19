/**
 * Core types for the go-db TypeScript SDK
 */

/**
 * A document in the database
 */
export interface Document {
  _id?: string;
  [key: string]: any;
}

/**
 * Health check response
 */
export interface HealthResponse {
  status: 'healthy' | 'unhealthy';
  message: string;
}

/**
 * Standard error response
 */
export interface ErrorResponse {
  error: string;
  message: string;
  code: number;
}

/**
 * Batch insert request
 */
export interface BatchInsertRequest {
  documents: Document[];
}

/**
 * Batch insert response
 */
export interface BatchInsertResponse {
  success: boolean;
  message: string;
  inserted_count: number;
  collection: string;
  documents: Document[];
}

/**
 * Batch update operation
 */
export interface BatchUpdateOperation {
  id: string;
  updates: Document;
}

/**
 * Batch update request
 */
export interface BatchUpdateRequest {
  operations: BatchUpdateOperation[];
}

/**
 * Batch update response
 */
export interface BatchUpdateResponse {
  success: boolean;
  message: string;
  updated_count: number;
  failed_count: number;
  collection: string;
  documents: Document[];
  errors?: string[];
}

/**
 * Pagination options
 */
export interface PaginationOptions {
  limit?: number;
  offset?: number;
  after?: string;
  before?: string;
}

/**
 * Pagination result
 */
export interface PaginationResult {
  documents: Document[];
  has_next: boolean;
  has_prev: boolean;
  next_cursor?: string;
  prev_cursor?: string;
  total?: number;
}

/**
 * Index list response
 */
export interface IndexListResponse {
  success: boolean;
  collection: string;
  indexes: string[];
  index_count: number;
}

/**
 * Index create response
 */
export interface IndexCreateResponse {
  success: boolean;
  message: string;
  collection: string;
  field: string;
}

/**
 * Client configuration options
 */
export interface GoDBClientConfig {
  baseURL?: string;
  timeout?: number;
  headers?: Record<string, string>;
}

/**
 * Query parameters for find operations
 */
export interface FindOptions extends PaginationOptions {
  [key: string]: any; // Allow arbitrary filter parameters
}

/**
 * HTTP methods supported by the API
 */
export type HttpMethod = 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE';

/**
 * API response wrapper
 */
export interface ApiResponse<T = any> {
  data: T;
  status: number;
  statusText: string;
  headers: Record<string, string>;
}

/**
 * Collection name type (for type safety)
 */
export type CollectionName = string;

/**
 * Document ID type (for type safety)
 */
export type DocumentId = string;

/**
 * Field name type (for type safety)
 */
export type FieldName = string;
