import axios, { AxiosInstance, AxiosResponse } from 'axios';
import {
  HealthResponse,
  PaginationResult,
  GoDBClientConfig,
  ApiResponse,
  ErrorResponse,
  CollectionsSchema,
  TypedClient,
  Select,
} from './types';

/**
 * GoDBClient - Main client class for interacting with the go-db API
 */
export class GoDBClient<S extends CollectionsSchema> implements TypedClient<S> {
  private client: AxiosInstance;
  private baseURL: string;

  constructor(config: GoDBClientConfig = {}) {
    this.baseURL = config.baseURL || 'http://localhost:8080';

    this.client = axios.create({
      baseURL: this.baseURL,
      timeout: config.timeout || 30000,
      headers: {
        'Content-Type': 'application/json',
        ...config.headers,
      },
    });

    // Add response interceptor for error handling
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response) {
          // Server responded with error status
          const errorResponse: ErrorResponse = {
            error: error.response.data?.error || 'Unknown Error',
            message: error.response.data?.message || error.message,
            code: error.response.status,
          };
          throw new Error(
            `API Error ${errorResponse.code}: ${errorResponse.message}`
          );
        } else if (error.request) {
          // Request was made but no response received
          throw new Error('Network Error: No response from server');
        } else {
          // Something else happened
          throw new Error(`Request Error: ${error.message}`);
        }
      }
    );
  }

  /**
   * Get the base URL of the client
   */
  getBaseURL(): string {
    return this.baseURL;
  }

  /**
   * Check if the database service is healthy
   */
  async health(): Promise<HealthResponse> {
    const response = await this.client.get<HealthResponse>('/health');
    return response.data;
  }

  /**
   * Insert a single document into a collection
   * @param collection - The name of the collection to insert the document into
   * @param document - The document to insert
   * @returns The inserted document
   * @throws {Error}
   */

  async insert<K extends keyof S>(
    collection: K,
    doc: Omit<S[K], '_id'>
  ): Promise<S[K]> {
    const response = await this.client.post<S[K]>(
      `/collections/${String(collection)}`,
      doc
    );
    return response.data;
  }

  /**
   * Insert multiple documents into a collection
   */
  async batchInsert<K extends keyof S>(
    collection: K,
    docs: Array<Omit<S[K], '_id'>>
  ): Promise<{
    success: boolean;
    message: string;
    inserted_count: number;
    collection: string;
    documents: S[K][];
  }> {
    const request = { documents: docs };
    const response = await this.client.post(
      `/collections/${String(collection)}/batch`,
      request
    );
    return response.data;
  }

  /**
   * Update multiple documents in a collection
   */
  async batchUpdate<K extends keyof S>(
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
  }> {
    const request = { operations };
    const response = await this.client.patch(
      `/collections/${String(collection)}/batch`,
      request
    );
    return response.data;
  }

  /**
   * Get a document by its ID
   */
  async getById<K extends keyof S>(
    collection: K,
    id: string
  ): Promise<S[K] | null> {
    try {
      const response = await this.client.get<S[K]>(
        `/collections/${String(collection)}/documents/${id}`
      );
      return response.data;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * Update a document by its ID (partial update)
   */
  async updateById<K extends keyof S>(
    collection: K,
    id: string,
    updates: Partial<Omit<S[K], '_id'>>
  ): Promise<S[K]> {
    const response = await this.client.patch<S[K]>(
      `/collections/${String(collection)}/documents/${id}`,
      updates
    );
    return response.data;
  }

  /**
   * Replace a document by its ID (complete replacement)
   */
  async replaceById<K extends keyof S>(
    collection: K,
    id: string,
    document: Omit<S[K], '_id'>
  ): Promise<S[K]> {
    const response = await this.client.put<S[K]>(
      `/collections/${String(collection)}/documents/${id}`,
      document
    );
    return response.data;
  }

  /**
   * Delete a document by its ID
   */
  async deleteById<K extends keyof S>(
    collection: K,
    id: string
  ): Promise<void> {
    await this.client.delete(
      `/collections/${String(collection)}/documents/${id}`
    );
  }

  /**
   * Find documents in a collection with optional filtering and pagination
   */
  async find<
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
  ): Promise<Array<Select<S[K], Sel>>> {
    const params = new URLSearchParams();

    // Add pagination parameters
    if (opts?.limit !== undefined) {
      params.append('limit', opts.limit.toString());
    }
    if (opts?.offset !== undefined) {
      params.append('offset', opts.offset.toString());
    }
    if (opts?.after) {
      params.append('after', opts.after);
    }
    if (opts?.before) {
      params.append('before', opts.before);
    }

    // Add filter parameters
    if (opts?.where) {
      Object.entries(opts.where).forEach(([key, value]) => {
        if (value !== undefined) {
          params.append(key, String(value));
        }
      });
    }

    const response = await this.client.get<PaginationResult>(
      `/collections/${String(collection)}/find?${params.toString()}`
    );

    // If select is specified, filter the fields
    if (opts?.select) {
      const selectFields = opts.select as readonly string[];
      return response.data.documents.map((doc) => {
        const selected: any = {};
        selectFields.forEach((field) => {
          if (field in doc) {
            selected[field] = doc[field];
          }
        });
        return selected;
      });
    }

    return response.data.documents as Array<Select<S[K], Sel>>;
  }

  /**
   * Find documents with streaming support (returns a ReadableStream)
   */
  async findWithStream<K extends keyof S>(
    collection: K,
    filters: Partial<S[K]> = {}
  ): Promise<ReadableStream<S[K]>> {
    const params = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== undefined) {
        params.append(key, String(value));
      }
    });

    const response = await this.client.get(
      `/collections/${String(
        collection
      )}/find_with_stream?${params.toString()}`,
      {
        responseType: 'stream',
      }
    );

    // Convert Node.js stream to ReadableStream
    return new ReadableStream({
      start(controller) {
        response.data.on('data', (chunk: Buffer) => {
          const lines = chunk.toString().split('\n');
          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const data = line.slice(6);
              if (data === '[DONE]') {
                controller.close();
                return;
              }
              try {
                const document = JSON.parse(data);
                controller.enqueue(document);
              } catch (e) {
                // Skip invalid JSON
              }
            }
          }
        });

        response.data.on('end', () => {
          controller.close();
        });

        response.data.on('error', (error: Error) => {
          controller.error(error);
        });
      },
    });
  }

  /**
   * Get all indexes for a collection
   */
  async getIndexes<K extends keyof S>(
    collection: K
  ): Promise<{
    success: boolean;
    collection: string;
    indexes: string[];
    index_count: number;
  }> {
    const response = await this.client.get(
      `/collections/${String(collection)}/indexes`
    );
    return response.data;
  }

  /**
   * Create an index on a field in a collection
   */
  async createIndex<K extends keyof S>(
    collection: K,
    field: keyof S[K]
  ): Promise<{
    success: boolean;
    message: string;
    collection: string;
    field: string;
  }> {
    const response = await this.client.post(
      `/collections/${String(collection)}/indexes/${String(field)}`
    );
    return response.data;
  }

  /**
   * Get raw HTTP client for advanced usage
   */
  getHttpClient(): AxiosInstance {
    return this.client;
  }

  /**
   * Make a custom request to the API
   */
  async request<T = any>(
    method: string,
    url: string,
    data?: any
  ): Promise<ApiResponse<T>> {
    const response: AxiosResponse<T> = await this.client.request({
      method: method as any,
      url,
      data,
    });

    return {
      data: response.data,
      status: response.status,
      statusText: response.statusText,
      headers: response.headers as Record<string, string>,
    };
  }
}
