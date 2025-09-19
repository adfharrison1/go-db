/**
 * Core types for the go-db TypeScript SDK
 */

/**
 * A document in the database (generic version)
 */
export interface Document {
  _id?: string;
  [key: string]: any;
}

/**
 * Collections schema - user-defined shape of all collections
 */
export type CollectionsSchema = Record<string, unknown>;

/**
 * Utility type for selecting specific fields from a document
 */
export type Select<
  T,
  K extends readonly (keyof T)[] | undefined
> = K extends readonly (keyof T)[] ? Pick<T, K[number]> : T;

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

/**
 * Type-safe client interface that users can implement
 */
export interface TypedClient<S extends CollectionsSchema> {
  /**
   * Get a single document by ID
   */
  getById<K extends keyof S>(collection: K, id: string): Promise<S[K] | null>;

  /**
   * Find documents with optional filtering and field selection
   */
  find<
    K extends keyof S,
    Sel extends readonly (keyof S[K])[] | undefined = undefined
  >(
    collection: K,
    opts?: {
      where?: Partial<S[K]>;
      select?: Sel;
      limit?: number;
      offset?: number;
      after?: string;
      before?: string;
    }
  ): Promise<Array<Select<S[K], Sel>>>;

  /**
   * Find documents with streaming support
   */
  findWithStream<K extends keyof S>(
    collection: K,
    filters: Partial<S[K]>
  ): Promise<ReadableStream<S[K]>>;

  /**
   * Insert a document into a collection
   */
  insert<K extends keyof S>(
    collection: K,
    doc: Omit<S[K], '_id'>
  ): Promise<S[K]>;

  /**
   * Update a document by ID with partial data
   */
  updateById<K extends keyof S>(
    collection: K,
    id: string,
    updates: Partial<Omit<S[K], '_id'>>
  ): Promise<S[K]>;

  /**
   * Replace a document by ID with complete data
   */
  replaceById<K extends keyof S>(
    collection: K,
    id: string,
    document: Omit<S[K], '_id'>
  ): Promise<S[K]>;

  /**
   * Delete a document by ID
   */
  deleteById<K extends keyof S>(collection: K, id: string): Promise<void>;

  /**
   * Batch insert multiple documents
   */
  batchInsert<K extends keyof S>(
    collection: K,
    docs: Array<Omit<S[K], '_id'>>
  ): Promise<{
    success: boolean;
    message: string;
    inserted_count: number;
    collection: string;
    documents: S[K][];
  }>;

  /**
   * Batch update multiple documents
   */
  batchUpdate<K extends keyof S>(
    collection: K,
    operations: Array<{
      id: string;
      updates: Partial<Omit<S[K], '_id'>>;
    }>
  ): Promise<{
    success: boolean;
    message: string;
    updated_count: number;
    failed_count: number;
    collection: string;
    documents: S[K][];
    errors?: string[];
  }>;

  /**
   * Create an index on a field
   */
  createIndex<K extends keyof S>(
    collection: K,
    field: keyof S[K]
  ): Promise<{
    success: boolean;
    message: string;
    collection: string;
    field: string;
  }>;

  /**
   * Get all indexes for a collection
   */
  getIndexes<K extends keyof S>(
    collection: K
  ): Promise<{
    success: boolean;
    collection: string;
    indexes: string[];
    index_count: number;
  }>;

  /**
   * Health check
   */
  health(): Promise<HealthResponse>;
}
