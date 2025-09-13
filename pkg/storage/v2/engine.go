package v2

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/indexing"
)

// NewStorageEngine creates a new v2 storage engine with WAL
func NewStorageEngine(options ...StorageOption) *StorageEngine {
	engine := &StorageEngine{
		collections:         make(map[string]*CollectionInfo),
		indexEngine:         indexing.NewIndexEngine(),
		walDir:              "./wal",
		dataDir:             ".",
		maxMemoryMB:         1024,
		checkpointInterval:  30 * time.Second,
		durabilityLevel:     DurabilityOS,
		maxWALSize:          100 * 1024 * 1024, // 100MB
		checkpointThreshold: 1000,
		compressionEnabled:  false,
		stopChan:            make(chan struct{}),
		stats:               &StorageStats{},
	}

	// Apply options
	for _, option := range options {
		option(engine)
	}

	// Initialize components
	engine.walEngine = NewWALEngine(engine.walDir, engine.durabilityLevel, engine.compressionEnabled)
	engine.checkpointMgr = NewCheckpointManager(engine)
	engine.recoveryMgr = NewRecoveryManager(engine)
	engine.memoryMgr = NewMemoryManager(engine)

	// Ensure directories exist
	if err := os.MkdirAll(engine.walDir, 0755); err != nil {
		log.Fatalf("Failed to create WAL directory: %v", err)
	}
	if err := os.MkdirAll(engine.dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Perform recovery on startup
	if err := engine.recoveryMgr.Recover(); err != nil {
		log.Fatalf("Recovery failed: %v", err)
	}

	return engine
}

// Insert implements domain.StorageEngine
func (se *StorageEngine) Insert(collName string, doc domain.Document) (domain.Document, error) {
	se.collectionsMu.RLock()
	_, exists := se.collections[collName]
	se.collectionsMu.RUnlock()

	if !exists {
		if err := se.CreateCollection(collName); err != nil {
			return nil, err
		}
	}

	// Generate ID if not provided
	if doc["_id"] == nil {
		doc["_id"] = se.generateDocumentID(collName)
	}

	// Create WAL entry
	entry := &WALEntry{
		Type:       WALEntryInsert,
		Timestamp:  time.Now().UnixNano(),
		Collection: collName,
		DocumentID: doc["_id"].(string),
		Document:   doc,
	}

	// Write to WAL
	if err := se.walEngine.WriteEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update in-memory collection
	if err := se.memoryMgr.InsertDocument(collName, doc); err != nil {
		return nil, fmt.Errorf("failed to insert document in memory: %w", err)
	}

	// Update collection metadata
	se.updateCollectionMetadata(collName, 1)

	se.updateStats(func(s *StorageStats) {
		s.WALEntriesWritten++
		s.WALBytesWritten += int64(len(fmt.Sprintf("%+v", doc)))
	})

	return doc, nil
}

// BatchInsert implements domain.StorageEngine
func (se *StorageEngine) BatchInsert(collName string, docs []domain.Document) ([]domain.Document, error) {
	se.collectionsMu.RLock()
	_, exists := se.collections[collName]
	se.collectionsMu.RUnlock()

	if !exists {
		if err := se.CreateCollection(collName); err != nil {
			return nil, err
		}
	}

	// Generate IDs for documents that don't have them
	for i, doc := range docs {
		if doc["_id"] == nil {
			docs[i]["_id"] = se.generateDocumentID(collName)
		}
	}

	// Create WAL entry for batch
	entry := &WALEntry{
		Type:       WALEntryBatchInsert,
		Timestamp:  time.Now().UnixNano(),
		Collection: collName,
		Document:   domain.Document{"_batch": docs},
	}

	// Write to WAL
	if err := se.walEngine.WriteEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update in-memory collection
	if err := se.memoryMgr.BatchInsertDocuments(collName, docs); err != nil {
		return nil, fmt.Errorf("failed to batch insert documents in memory: %w", err)
	}

	// Update collection metadata
	se.updateCollectionMetadata(collName, int64(len(docs)))

	se.updateStats(func(s *StorageStats) {
		s.WALEntriesWritten++
		s.WALBytesWritten += int64(len(fmt.Sprintf("%+v", docs)))
	})

	return docs, nil
}

// FindAll implements domain.StorageEngine
func (se *StorageEngine) FindAll(collName string, filter map[string]interface{}, options *domain.PaginationOptions) (*domain.PaginationResult, error) {
	return se.memoryMgr.FindAll(collName, filter, options)
}

// FindAllStream implements domain.StorageEngine
func (se *StorageEngine) FindAllStream(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	return se.memoryMgr.FindAllStream(collName, filter)
}

// GetById implements domain.StorageEngine
func (se *StorageEngine) GetById(collName, docId string) (domain.Document, error) {
	return se.memoryMgr.GetById(collName, docId)
}

// UpdateById implements domain.StorageEngine
func (se *StorageEngine) UpdateById(collName, docId string, updates domain.Document) (domain.Document, error) {
	// Get existing document
	existing, err := se.memoryMgr.GetById(collName, docId)
	if err != nil {
		return nil, err
	}

	// Merge updates
	updated := se.mergeDocuments(existing, updates)

	// Create WAL entry
	entry := &WALEntry{
		Type:       WALEntryUpdate,
		Timestamp:  time.Now().UnixNano(),
		Collection: collName,
		DocumentID: docId,
		Updates:    updates,
	}

	// Write to WAL
	if err := se.walEngine.WriteEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update in-memory collection
	if err := se.memoryMgr.UpdateDocument(collName, docId, updated); err != nil {
		return nil, fmt.Errorf("failed to update document in memory: %w", err)
	}

	se.updateStats(func(s *StorageStats) {
		s.WALEntriesWritten++
		s.WALBytesWritten += int64(len(fmt.Sprintf("%+v", updates)))
	})

	return updated, nil
}

// ReplaceById implements domain.StorageEngine
func (se *StorageEngine) ReplaceById(collName, docId string, newDoc domain.Document) (domain.Document, error) {
	// Ensure the document has the correct ID
	newDoc["_id"] = docId

	// Create WAL entry
	entry := &WALEntry{
		Type:       WALEntryReplace,
		Timestamp:  time.Now().UnixNano(),
		Collection: collName,
		DocumentID: docId,
		Document:   newDoc,
	}

	// Write to WAL
	if err := se.walEngine.WriteEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Update in-memory collection
	if err := se.memoryMgr.ReplaceDocument(collName, docId, newDoc); err != nil {
		return nil, fmt.Errorf("failed to replace document in memory: %w", err)
	}

	se.updateStats(func(s *StorageStats) {
		s.WALEntriesWritten++
		s.WALBytesWritten += int64(len(fmt.Sprintf("%+v", newDoc)))
	})

	return newDoc, nil
}

// BatchUpdate implements domain.StorageEngine
func (se *StorageEngine) BatchUpdate(collName string, updates []domain.BatchUpdateOperation) ([]domain.Document, error) {
	// Create WAL entry for batch
	entry := &WALEntry{
		Type:       WALEntryBatchUpdate,
		Timestamp:  time.Now().UnixNano(),
		Collection: collName,
		BatchOps:   updates,
	}

	// Write to WAL
	if err := se.walEngine.WriteEntry(entry); err != nil {
		return nil, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Process batch updates in memory
	results, err := se.memoryMgr.BatchUpdateDocuments(collName, updates)
	if err != nil {
		return nil, fmt.Errorf("failed to batch update documents in memory: %w", err)
	}

	se.updateStats(func(s *StorageStats) {
		s.WALEntriesWritten++
		s.WALBytesWritten += int64(len(fmt.Sprintf("%+v", updates)))
	})

	return results, nil
}

// DeleteById implements domain.StorageEngine
func (se *StorageEngine) DeleteById(collName, docId string) error {
	// Create WAL entry
	entry := &WALEntry{
		Type:       WALEntryDelete,
		Timestamp:  time.Now().UnixNano(),
		Collection: collName,
		DocumentID: docId,
	}

	// Write to WAL
	if err := se.walEngine.WriteEntry(entry); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Delete from in-memory collection
	if err := se.memoryMgr.DeleteDocument(collName, docId); err != nil {
		return fmt.Errorf("failed to delete document in memory: %w", err)
	}

	// Update collection metadata
	se.updateCollectionMetadata(collName, -1)

	se.updateStats(func(s *StorageStats) {
		s.WALEntriesWritten++
		s.WALBytesWritten += int64(len(docId))
	})

	return nil
}

// CreateCollection implements domain.StorageEngine
func (se *StorageEngine) CreateCollection(collName string) error {
	se.collectionsMu.Lock()
	defer se.collectionsMu.Unlock()

	if _, exists := se.collections[collName]; exists {
		return nil // Collection already exists
	}

	se.collections[collName] = &CollectionInfo{
		Name:          collName,
		State:         CollectionStateLoaded,
		DocumentCount: 0,
		LastModified:  time.Now(),
		Indexes:       []string{},
	}

	// Also create the collection in the memory manager
	se.memoryMgr.mu.Lock()
	defer se.memoryMgr.mu.Unlock()

	if _, exists := se.memoryMgr.collections[collName]; !exists {
		se.memoryMgr.collections[collName] = &Collection{
			Name:      collName,
			Documents: make(map[string]domain.Document),
			CreatedAt: time.Now(),
		}
	}

	return nil
}

// GetCollection implements domain.StorageEngine
func (se *StorageEngine) GetCollection(collName string) (*domain.Collection, error) {
	se.collectionsMu.RLock()
	_, exists := se.collections[collName]
	se.collectionsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("collection %s not found", collName)
	}

	return &domain.Collection{
		Name:      collName,
		Documents: make(map[string]domain.Document), // We don't store documents in the collection metadata
	}, nil
}

// LoadCollectionMetadata implements domain.StorageEngine
func (se *StorageEngine) LoadCollectionMetadata(filename string) error {
	// This would load collection metadata from disk
	// For now, we'll implement a basic version
	return nil
}

// SaveToFile implements domain.StorageEngine
func (se *StorageEngine) SaveToFile(filename string) error {
	// Trigger a checkpoint to save all data to disk
	return se.checkpointMgr.Checkpoint()
}

// GetMemoryStats implements domain.StorageEngine
func (se *StorageEngine) GetMemoryStats() map[string]interface{} {
	se.statsMu.RLock()
	defer se.statsMu.RUnlock()

	return map[string]interface{}{
		"wal_entries_written":   se.stats.WALEntriesWritten,
		"wal_bytes_written":     se.stats.WALBytesWritten,
		"checkpoints_performed": se.stats.CheckpointsPerformed,
		"recovery_time_ms":      se.stats.RecoveryTime.Milliseconds(),
		"memory_usage_mb":       se.stats.MemoryUsageMB,
		"collection_count":      se.stats.CollectionCount,
		"last_checkpoint":       se.stats.LastCheckpoint,
	}
}

// StartBackgroundWorkers implements domain.StorageEngine
func (se *StorageEngine) StartBackgroundWorkers() {
	se.stopOnce.Do(func() {
		se.stopChan = make(chan struct{})
		se.backgroundWg.Add(1)
		go se.checkpointMgr.Run()
	})
}

// StopBackgroundWorkers implements domain.StorageEngine
func (se *StorageEngine) StopBackgroundWorkers() {
	se.stopOnce.Do(func() {
		close(se.stopChan)
		se.backgroundWg.Wait()
	})
}

// SaveCollectionAfterTransaction implements domain.StorageEngine
func (se *StorageEngine) SaveCollectionAfterTransaction(collName string) error {
	// In WAL mode, we don't need to save after each transaction
	// The WAL ensures durability, and checkpoints handle periodic saves
	return nil
}

// GetIndexes implements domain.StorageEngine
func (se *StorageEngine) GetIndexes(collName string) ([]string, error) {
	se.collectionsMu.RLock()
	collInfo, exists := se.collections[collName]
	se.collectionsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("collection %s not found", collName)
	}

	return collInfo.Indexes, nil
}

// CreateIndex implements domain.StorageEngine
func (se *StorageEngine) CreateIndex(collName, fieldName string) error {
	se.collectionsMu.Lock()
	defer se.collectionsMu.Unlock()

	collInfo, exists := se.collections[collName]
	if !exists {
		return fmt.Errorf("collection %s not found", collName)
	}

	// Add to collection indexes
	collInfo.Indexes = append(collInfo.Indexes, fieldName)

	// Create index in index engine
	return se.indexEngine.CreateIndex(collName, fieldName)
}

// Helper methods

func (se *StorageEngine) generateDocumentID(collName string) string {
	// Simple ID generation - in production, use UUID or similar
	return fmt.Sprintf("%s_%d", collName, time.Now().UnixNano())
}

func (se *StorageEngine) updateCollectionMetadata(collName string, delta int64) {
	se.collectionsMu.Lock()
	defer se.collectionsMu.Unlock()

	if collInfo, exists := se.collections[collName]; exists {
		collInfo.DocumentCount += delta
		collInfo.LastModified = time.Now()
		if delta > 0 {
			collInfo.State = CollectionStateDirty
		}
	}
}

func (se *StorageEngine) mergeDocuments(existing, updates domain.Document) domain.Document {
	merged := make(domain.Document)

	// Copy existing document
	for k, v := range existing {
		merged[k] = v
	}

	// Apply updates
	for k, v := range updates {
		merged[k] = v
	}

	return merged
}

func (se *StorageEngine) updateStats(updater func(*StorageStats)) {
	se.statsMu.Lock()
	defer se.statsMu.Unlock()
	updater(se.stats)
}
