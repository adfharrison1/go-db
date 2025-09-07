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

	// Configuration
	maxMemoryMB     int
	dataDir         string
	dataFile        string // Current data file for single-file persistence
	backgroundSave  bool
	transactionSave bool
	saveInterval    time.Duration

	// Background workers
	backgroundWg sync.WaitGroup
	stopChan     chan struct{}

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
		idCounters:      make(map[string]*int64),
		maxMemoryMB:     1024, // 1GB default
		dataDir:         ".",
		backgroundSave:  false,
		transactionSave: true, // Default to transaction-based saves
		saveInterval:    5 * time.Minute,
		stopChan:        make(chan struct{}),
	}

	// Apply options
	for _, option := range options {
		option(engine)
	}

	// Initialize cache with capacity based on max memory
	engine.cache = NewLRUCache(engine.maxMemoryMB / 100) // Rough estimate: 100MB per collection

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

// SaveCollectionAfterTransaction saves a specific collection to disk if transaction saves are enabled
func (se *StorageEngine) SaveCollectionAfterTransaction(collName string) error {
	if !se.transactionSave {
		return nil // Transaction saves disabled
	}

	// Only save if the collection is dirty
	se.mu.RLock()
	collInfo, exists := se.collections[collName]
	if !exists || collInfo.State != CollectionStateDirty {
		se.mu.RUnlock()
		return nil // Collection doesn't exist or isn't dirty
	}
	se.mu.RUnlock()

	return se.saveCollectionToFile(collName)
}

// IsTransactionSaveEnabled returns whether transaction-based saves are enabled
func (se *StorageEngine) IsTransactionSaveEnabled() bool {
	return se.transactionSave
}

// GetIndexEngine returns the index engine instance
func (se *StorageEngine) GetIndexEngine() domain.IndexEngine {
	return se.indexEngine
}
