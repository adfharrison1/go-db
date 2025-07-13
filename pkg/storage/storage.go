package storage

import (
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/indexing"
)

// StorageEngine provides memory management with LRU caching and lazy loading
type StorageEngine struct {
	mu          sync.RWMutex
	cache       *LRUCache
	collections map[string]*CollectionInfo // Collection metadata (always in memory)
	indexEngine *indexing.IndexEngine
	metadata    map[string]interface{}

	// Configuration
	maxMemoryMB    int
	dataDir        string
	backgroundSave bool
	saveInterval   time.Duration

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
		collections:    make(map[string]*CollectionInfo),
		indexEngine:    indexing.NewIndexEngine(),
		metadata:       make(map[string]interface{}),
		idCounters:     make(map[string]*int64),
		maxMemoryMB:    1024, // 1GB default
		dataDir:        ".",
		backgroundSave: false,
		saveInterval:   5 * time.Minute,
		stopChan:       make(chan struct{}),
	}

	// Apply options
	for _, option := range options {
		option(engine)
	}

	// Initialize cache with capacity based on max memory
	engine.cache = NewLRUCache(engine.maxMemoryMB / 100) // Rough estimate: 100MB per collection

	return engine
}
