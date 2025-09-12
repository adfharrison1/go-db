package storage

import (
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/indexing"
)

// CollectionLock provides per-collection concurrency control
type CollectionLock struct {
	mu     sync.RWMutex
	saving bool // Track if collection is being saved
}

// DiskWriteRequest represents a failed disk write that needs retry
type DiskWriteRequest struct {
	Collection string
	DocumentID string
	Document   domain.Document
	RetryCount int
	Timestamp  time.Time
}

// StorageEngine provides memory management with LRU caching and lazy loading
type StorageEngine struct {
	mu          sync.RWMutex
	cache       *LRUCache
	collections map[string]*CollectionInfo // Collection metadata (always in memory)
	indexEngine *indexing.IndexEngine
	metadata    map[string]interface{}

	// Per-collection locks for better concurrency
	collectionLocks map[string]*CollectionLock
	locksMu         sync.RWMutex

	// Per-document locks for maximum concurrency
	documentLocks map[string]*sync.RWMutex // "collection:docID" -> lock
	docLocksMu    sync.RWMutex             // protects documentLocks map

	// Configuration
	maxMemoryMB int
	dataDir     string
	dataFile    string // Current data file for single-file persistence
	noSaves     bool   // If true, only save on shutdown

	// Background workers
	backgroundWg sync.WaitGroup
	stopChan     chan struct{}
	stopOnce     sync.Once

	// Disk write queue for failed immediate writes
	diskWriteQueue chan DiskWriteRequest
	diskWriteWg    sync.WaitGroup

	// Per-collection ID counters for thread-safe ID generation
	idCounters   map[string]*int64
	idCountersMu sync.RWMutex
}

// NewStorageEngine creates a new storage engine
func NewStorageEngine(options ...StorageOption) *StorageEngine {
	engine := &StorageEngine{
		collections:     make(map[string]*CollectionInfo),
		indexEngine:     indexing.NewIndexEngine(),
		metadata:        make(map[string]interface{}),
		collectionLocks: make(map[string]*CollectionLock),
		documentLocks:   make(map[string]*sync.RWMutex),
		idCounters:      make(map[string]*int64),
		maxMemoryMB:     1024, // 1GB default
		dataDir:         ".",
		noSaves:         false, // Default to dual-write mode
		stopChan:        make(chan struct{}),
		diskWriteQueue:  make(chan DiskWriteRequest, 1000), // Buffer for failed writes
	}

	// Apply options
	for _, option := range options {
		option(engine)
	}

	// Initialize cache with capacity based on max memory
	engine.cache = NewLRUCache(engine.maxMemoryMB / 100) // Rough estimate: 100MB per collection

	// Start disk write queue processing
	engine.startDiskWriteQueue()

	return engine
}

// getOrCreateCollectionLock gets or creates a lock for a collection
func (se *StorageEngine) getOrCreateCollectionLock(collName string) *CollectionLock {
	se.locksMu.RLock()
	if lock, exists := se.collectionLocks[collName]; exists {
		se.locksMu.RUnlock()
		return lock
	}
	se.locksMu.RUnlock()

	// Need to create the lock
	se.locksMu.Lock()
	defer se.locksMu.Unlock()

	// Double-check in case another goroutine created it
	if lock, exists := se.collectionLocks[collName]; exists {
		return lock
	}

	lock := &CollectionLock{}
	se.collectionLocks[collName] = lock
	return lock
}

// withCollectionReadLock executes a function with a read lock on the specified collection
func (se *StorageEngine) withCollectionReadLock(collName string, fn func() error) error {
	lock := se.getOrCreateCollectionLock(collName)
	lock.mu.RLock()
	defer lock.mu.RUnlock()
	return fn()
}

// withCollectionWriteLock executes a function with a write lock on the specified collection
func (se *StorageEngine) withCollectionWriteLock(collName string, fn func() error) error {
	lock := se.getOrCreateCollectionLock(collName)
	lock.mu.Lock()
	defer lock.mu.Unlock()
	return fn()
}

// getOrCreateDocumentLock gets or creates a lock for a specific document
func (se *StorageEngine) getOrCreateDocumentLock(collName, docID string) *sync.RWMutex {
	lockKey := collName + ":" + docID

	se.docLocksMu.RLock()
	if lock, exists := se.documentLocks[lockKey]; exists {
		se.docLocksMu.RUnlock()
		return lock
	}
	se.docLocksMu.RUnlock()

	// Need to create the lock
	se.docLocksMu.Lock()
	defer se.docLocksMu.Unlock()

	// Double-check in case another goroutine created it
	if lock, exists := se.documentLocks[lockKey]; exists {
		return lock
	}

	lock := &sync.RWMutex{}
	se.documentLocks[lockKey] = lock
	return lock
}

// withDocumentReadLock executes a function with a read lock on the specified document
func (se *StorageEngine) withDocumentReadLock(collName, docID string, fn func() error) error {
	lock := se.getOrCreateDocumentLock(collName, docID)
	lock.RLock()
	defer lock.RUnlock()
	return fn()
}

// withDocumentWriteLock executes a function with a write lock on the specified document
func (se *StorageEngine) withDocumentWriteLock(collName, docID string, fn func() error) error {
	lock := se.getOrCreateDocumentLock(collName, docID)
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

// SaveCollectionAfterTransaction saves a specific collection to disk
func (se *StorageEngine) SaveCollectionAfterTransaction(collName string) error {
	if se.noSaves {
		return nil // No-saves mode enabled
	}

	// Use collection write lock to prevent concurrent modifications during save
	return se.withCollectionWriteLock(collName, func() error {
		// Only save if the collection is dirty
		collInfo, exists := se.collections[collName]
		if !exists || collInfo.State != CollectionStateDirty {
			return nil // Collection doesn't exist or isn't dirty
		}

		return se.saveCollectionToFileUnsafe(collName)
	})
}

// startDiskWriteQueue starts the background goroutine to process failed disk writes
func (se *StorageEngine) startDiskWriteQueue() {
	se.diskWriteWg.Add(1)
	go func() {
		defer se.diskWriteWg.Done()
		for req := range se.diskWriteQueue {
			se.processDiskWriteRequest(req)
		}
	}()
}

// processDiskWriteRequest processes a failed disk write with retry logic
func (se *StorageEngine) processDiskWriteRequest(req DiskWriteRequest) {
	maxRetries := 3
	baseDelay := time.Second

	if req.RetryCount >= maxRetries {
		// Log final failure and give up
		// In a real implementation, you might want to persist this to a dead letter queue
		return
	}

	// Exponential backoff with interruptible sleep
	delay := time.Duration(req.RetryCount+1) * baseDelay
	select {
	case <-time.After(delay):
		// Delay completed
	case <-se.stopChan:
		// Stop requested, exit
		return
	}

	// Retry the disk write
	if err := se.saveDocumentToDisk(req.Collection, req.DocumentID, req.Document); err != nil {
		// Still failed, increment retry count and requeue
		req.RetryCount++
		select {
		case se.diskWriteQueue <- req:
			// Successfully requeued
		case <-se.stopChan:
			// Stop requested, exit
			return
		default:
			// Queue is full, log error
			// In a real implementation, you might want to persist this to a dead letter queue
		}
	}
	// If successful, the request is automatically removed from the queue
}

// queueDiskWrite queues a failed disk write for background retry
func (se *StorageEngine) queueDiskWrite(collection, docID string, doc domain.Document) {
	req := DiskWriteRequest{
		Collection: collection,
		DocumentID: docID,
		Document:   doc,
		RetryCount: 0,
		Timestamp:  time.Now(),
	}

	select {
	case se.diskWriteQueue <- req:
		// Successfully queued
	default:
		// Queue is full, log error
		// In a real implementation, you might want to persist this to a dead letter queue
	}
}

// IsNoSavesEnabled returns whether no-saves mode is enabled
func (se *StorageEngine) IsNoSavesEnabled() bool {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.noSaves
}

// GetIndexEngine returns the index engine instance
func (se *StorageEngine) GetIndexEngine() domain.IndexEngine {
	return se.indexEngine
}
