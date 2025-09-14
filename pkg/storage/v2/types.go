package v2

import (
	"os"
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/indexing"
)

// DurabilityLevel represents the level of durability guarantee
type DurabilityLevel int

const (
	DurabilityNone   DurabilityLevel = iota // No durability guarantees
	DurabilityMemory                        // Durability to memory only
	DurabilityOS                            // Durability to OS page cache (default)
	DurabilityFull                          // Full durability with fsync
)

// WALEntryType represents the type of WAL entry
type WALEntryType uint8

const (
	WALEntryInsert WALEntryType = iota + 1
	WALEntryUpdate
	WALEntryReplace
	WALEntryDelete
	WALEntryBatchInsert
	WALEntryBatchUpdate
	WALEntryCheckpoint
	WALEntryCommit
)

// WALEntry represents a single entry in the write-ahead log
type WALEntry struct {
	Type       WALEntryType                  `json:"type"`
	Timestamp  int64                         `json:"timestamp"`
	Collection string                        `json:"collection"`
	DocumentID string                        `json:"document_id,omitempty"`
	Document   domain.Document               `json:"document,omitempty"`
	Updates    domain.Document               `json:"updates,omitempty"`
	BatchOps   []domain.BatchUpdateOperation `json:"batch_ops,omitempty"`
	LSN        int64                         `json:"lsn"` // Log Sequence Number
	Checksum   uint32                        `json:"checksum"`
}

// CollectionState represents the state of a collection
type CollectionState int

const (
	CollectionStateUnloaded CollectionState = iota
	CollectionStateLoading
	CollectionStateLoaded
	CollectionStateDirty
)

// CollectionInfo holds metadata about a collection
type CollectionInfo struct {
	Name          string
	State         CollectionState
	DocumentCount int64
	LastModified  time.Time
	Indexes       []string
	mu            sync.RWMutex
}

// StorageEngine is the v2 storage engine implementation
type StorageEngine struct {
	// Core components
	walEngine     *WALEngine
	checkpointMgr *CheckpointManager
	recoveryMgr   *RecoveryManager
	memoryMgr     *MemoryManager
	indexEngine   *indexing.IndexEngine

	// Configuration
	walDir              string
	dataDir             string
	checkpointDir       string
	maxMemoryMB         int
	checkpointInterval  time.Duration
	durabilityLevel     DurabilityLevel
	maxWALSize          int64
	checkpointThreshold int
	compressionEnabled  bool

	// State management
	collections   map[string]*CollectionInfo
	collectionsMu sync.RWMutex

	// Background workers
	backgroundWg sync.WaitGroup
	stopChan     chan struct{}
	stopOnce     sync.Once

	// Statistics
	stats   *StorageStats
	statsMu sync.RWMutex

	// ID generation
	idCounter int64
}

// StorageStats holds performance and health statistics
type StorageStats struct {
	WALEntriesWritten    int64
	WALBytesWritten      int64
	CheckpointsPerformed int64
	RecoveryTime         time.Duration
	MemoryUsageMB        int64
	CollectionCount      int64
	LastCheckpoint       time.Time
}

// WALEngine manages the write-ahead log
type WALEngine struct {
	walDir             string
	durabilityLevel    DurabilityLevel
	compressionEnabled bool
	currentLSN         int64
	walFile            *WALFile
	mu                 sync.RWMutex
}

// WALFile represents an open WAL file
type WALFile struct {
	Path     string
	File     *os.File
	Position int64
	Entries  int64
}

// CheckpointManager handles periodic checkpointing
type CheckpointManager struct {
	engine         *StorageEngine
	interval       time.Duration
	threshold      int
	maxWALSize     int64
	lastCheckpoint time.Time
	mu             sync.RWMutex
}

// RecoveryManager handles startup recovery
type RecoveryManager struct {
	engine *StorageEngine
}

// MemoryManager handles in-memory collections and caching
type MemoryManager struct {
	engine      *StorageEngine
	cache       *LRUCache
	maxMemoryMB int
	collections map[string]*Collection
	mu          sync.RWMutex
}

// LRUCache implements a thread-safe LRU cache
type LRUCache struct {
	capacity int
	cache    map[string]*CacheEntry
	head     *CacheEntry
	tail     *CacheEntry
	mu       sync.RWMutex
}

// CacheEntry represents a single cache entry
type CacheEntry struct {
	key        string
	value      interface{}
	collection string
	lastAccess time.Time
	prev       *CacheEntry
	next       *CacheEntry
}

// Collection represents an in-memory collection
type Collection struct {
	Name      string
	Documents map[string]domain.Document
	CreatedAt time.Time
}
