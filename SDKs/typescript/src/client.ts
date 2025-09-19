import axios, { AxiosInstance, AxiosResponse } from 'axios';
import {
  Document,
  HealthResponse,
  BatchInsertRequest,
  BatchInsertResponse,
  BatchUpdateRequest,
  BatchUpdateResponse,
  PaginationResult,
  IndexListResponse,
  IndexCreateResponse,
  GoDBClientConfig,
  FindOptions,
  ApiResponse,
  CollectionName,
  DocumentId,
  FieldName,
  ErrorResponse,
} from './types';

/**
 * GoDBClient - Main client class for interacting with the go-db API
 */
export class GoDBClient {
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
   */
  async insert(
    collection: CollectionName,
    document: Document
  ): Promise<Document> {
    const response = await this.client.post<Document>(
      `/collections/${collection}`,
      document
    );
    return response.data;
  }

  /**
   * Insert multiple documents into a collection
   */
  async batchInsert(
    collection: CollectionName,
    documents: Document[]
  ): Promise<BatchInsertResponse> {
    const request: BatchInsertRequest = { documents };
    const response = await this.client.post<BatchInsertResponse>(
      `/collections/${collection}/batch`,
      request
    );
    return response.data;
  }

  /**
   * Update multiple documents in a collection
   */
  async batchUpdate(
    collection: CollectionName,
    operations: BatchUpdateRequest['operations']
  ): Promise<BatchUpdateResponse> {
    const request: BatchUpdateRequest = { operations };
    const response = await this.client.patch<BatchUpdateResponse>(
      `/collections/${collection}/batch`,
      request
    );
    return response.data;
  }

  /**
   * Get a document by its ID
   */
  async getById(collection: CollectionName, id: DocumentId): Promise<Document> {
    const response = await this.client.get<Document>(
      `/collections/${collection}/documents/${id}`
    );
    return response.data;
  }

  /**
   * Update a document by its ID (partial update)
   */
  async updateById(
    collection: CollectionName,
    id: DocumentId,
    updates: Document
  ): Promise<Document> {
    const response = await this.client.patch<Document>(
      `/collections/${collection}/documents/${id}`,
      updates
    );
    return response.data;
  }

  /**
   * Replace a document by its ID (complete replacement)
   */
  async replaceById(
    collection: CollectionName,
    id: DocumentId,
    document: Document
  ): Promise<Document> {
    const response = await this.client.put<Document>(
      `/collections/${collection}/documents/${id}`,
      document
    );
    return response.data;
  }

  /**
   * Delete a document by its ID
   */
  async deleteById(collection: CollectionName, id: DocumentId): Promise<void> {
    await this.client.delete(`/collections/${collection}/documents/${id}`);
  }

  /**
   * Find documents in a collection with optional filtering and pagination
   */
  async find(
    collection: CollectionName,
    options: FindOptions = {}
  ): Promise<PaginationResult> {
    const params = new URLSearchParams();

    // Add pagination parameters
    if (options.limit !== undefined)
      params.append('limit', options.limit.toString());
    if (options.offset !== undefined)
      params.append('offset', options.offset.toString());
    if (options.after) params.append('after', options.after);
    if (options.before) params.append('before', options.before);

    // Add filter parameters
    Object.entries(options).forEach(([key, value]) => {
      if (
        key !== 'limit' &&
        key !== 'offset' &&
        key !== 'after' &&
        key !== 'before' &&
        value !== undefined
      ) {
        params.append(key, String(value));
      }
    });

    const response = await this.client.get<PaginationResult>(
      `/collections/${collection}/find?${params.toString()}`
    );
    return response.data;
  }

  /**
   * Find documents with streaming support (returns a ReadableStream)
   */
  async findWithStream(
    collection: CollectionName,
    filters: Record<string, any> = {}
  ): Promise<ReadableStream<Document>> {
    const params = new URLSearchParams();
    Object.entries(filters).forEach(([key, value]) => {
      if (value !== undefined) {
        params.append(key, String(value));
      }
    });

    const response = await this.client.get(
      `/collections/${collection}/find_with_stream?${params.toString()}`,
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
  async getIndexes(collection: CollectionName): Promise<IndexListResponse> {
    const response = await this.client.get<IndexListResponse>(
      `/collections/${collection}/indexes`
    );
    return response.data;
  }

  /**
   * Create an index on a field in a collection
   */
  async createIndex(
    collection: CollectionName,
    field: FieldName
  ): Promise<IndexCreateResponse> {
    const response = await this.client.post<IndexCreateResponse>(
      `/collections/${collection}/indexes/${field}`
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
