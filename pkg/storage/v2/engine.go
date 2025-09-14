package v2

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync/atomic"
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

	// Update indexes
	se.updateIndexesForDocument(collName, doc["_id"].(string), nil, doc)

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

	// Update indexes for each document
	for _, doc := range docs {
		se.updateIndexesForDocument(collName, doc["_id"].(string), nil, doc)
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

	// Update indexes
	se.updateIndexesForDocument(collName, docId, existing, updated)

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

	// Get existing document for index updates
	existing, _ := se.memoryMgr.GetById(collName, docId)

	// Update in-memory collection
	if err := se.memoryMgr.ReplaceDocument(collName, docId, newDoc); err != nil {
		return nil, fmt.Errorf("failed to replace document in memory: %w", err)
	}

	// Update indexes
	se.updateIndexesForDocument(collName, docId, existing, newDoc)

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

	// Update indexes for each updated document
	for i, result := range results {
		se.updateIndexesForDocument(collName, updates[i].ID, nil, result)
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

	// Get existing document for index updates
	existing, _ := se.memoryMgr.GetById(collName, docId)

	// Delete from in-memory collection
	if err := se.memoryMgr.DeleteDocument(collName, docId); err != nil {
		return fmt.Errorf("failed to delete document in memory: %w", err)
	}

	// Update indexes (remove document from all indexes)
	se.updateIndexesForDocument(collName, docId, existing, nil)

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

	// Create _id index automatically (like v1 engine)
	if err := se.indexEngine.CreateIndex(collName, "_id"); err != nil {
		return fmt.Errorf("failed to create _id index: %w", err)
	}

	se.collections[collName] = &CollectionInfo{
		Name:          collName,
		State:         CollectionStateLoaded,
		DocumentCount: 0,
		LastModified:  time.Now(),
		Indexes:       []string{"_id"}, // Include _id index
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
	// Load checkpoint data from the specified file
	return se.loadFromCheckpoint(filename)
}

// SaveToFile implements domain.StorageEngine
func (se *StorageEngine) SaveToFile(filename string) error {
	// Create a custom checkpoint to the specified filename
	return se.saveToSpecificFile(filename)
}

// loadFromCheckpoint loads data from a checkpoint file
func (se *StorageEngine) loadFromCheckpoint(filename string) error {
	// Read checkpoint file
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	// Parse checkpoint data
	var checkpointData CheckpointData
	if err := json.Unmarshal(data, &checkpointData); err != nil {
		return fmt.Errorf("failed to parse checkpoint data: %w", err)
	}

	// Load collections into memory
	se.collectionsMu.Lock()
	defer se.collectionsMu.Unlock()

	for collName, collData := range checkpointData.Collections {
		// Create collection info
		se.collections[collName] = &CollectionInfo{
			Name:          collData.Name,
			State:         CollectionStateLoaded,
			DocumentCount: collData.DocumentCount,
			LastModified:  collData.LastModified,
			Indexes:       collData.Indexes,
		}

		// Load documents into memory manager
		se.memoryMgr.mu.Lock()
		// Convert interface{} to domain.Document
		documents := make(map[string]domain.Document)
		for docID, doc := range collData.Documents {
			if docMap, ok := doc.(map[string]interface{}); ok {
				documents[docID] = domain.Document(docMap)
			}
		}
		se.memoryMgr.collections[collName] = &Collection{
			Name:      collData.Name,
			Documents: documents,
			CreatedAt: collData.LastModified,
		}
		se.memoryMgr.mu.Unlock()

		// Rebuild indexes
		for _, fieldName := range collData.Indexes {
			if err := se.indexEngine.CreateIndex(collName, fieldName); err != nil {
				// Log error but continue
				fmt.Printf("Failed to recreate index %s on collection %s: %v\n", fieldName, collName, err)
			}
		}
	}

	return nil
}

// saveToSpecificFile saves data to a specific filename
func (se *StorageEngine) saveToSpecificFile(filename string) error {
	// Get all collections data
	se.collectionsMu.RLock()
	collections := make(map[string]*CollectionData)

	for name, collInfo := range se.collections {
		// Get documents from memory manager
		documents, err := se.memoryMgr.GetAllDocuments(name)
		if err != nil {
			se.collectionsMu.RUnlock()
			return fmt.Errorf("failed to get documents for collection %s: %w", name, err)
		}

		collections[name] = &CollectionData{
			Name:          name,
			DocumentCount: collInfo.DocumentCount,
			LastModified:  collInfo.LastModified,
			Indexes:       collInfo.Indexes,
			Documents:     documents,
		}
	}
	se.collectionsMu.RUnlock()

	// Create checkpoint data
	checkpointData := &CheckpointData{
		Timestamp:   time.Now(),
		Collections: collections,
		Indexes:     se.indexEngine.ExportIndexes(),
		LSN:         se.walEngine.GetCurrentLSN(),
	}

	// Serialize and write to file
	jsonData, err := json.MarshalIndent(checkpointData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint data: %w", err)
	}

	// Write to temporary file first
	tempFile := filename + ".tmp"
	if err := os.WriteFile(tempFile, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, filename); err != nil {
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
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
	// Get indexes from the index engine (returns empty slice for non-existent collections)
	return se.indexEngine.GetIndexes(collName)
}

// IsNoSavesEnabled implements domain.StorageEngine (for compatibility)
func (se *StorageEngine) IsNoSavesEnabled() bool {
	// v2 storage engine doesn't have no-saves mode, always returns false
	return false
}

// GetIndexEngine returns the index engine instance
func (se *StorageEngine) GetIndexEngine() domain.IndexEngine {
	return se.indexEngine
}

// CreateIndex implements domain.StorageEngine
func (se *StorageEngine) CreateIndex(collName, fieldName string) error {
	// Create index in index engine
	if err := se.indexEngine.CreateIndex(collName, fieldName); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Build index for existing documents
	if err := se.buildIndexForCollection(collName, fieldName); err != nil {
		// If building fails, clean up the index
		se.indexEngine.DropIndex(collName, fieldName)
		return fmt.Errorf("failed to build index: %w", err)
	}

	// Update collection metadata
	se.collectionsMu.Lock()
	if collInfo, exists := se.collections[collName]; exists {
		collInfo.Indexes = append(collInfo.Indexes, fieldName)
	}
	se.collectionsMu.Unlock()

	return nil
}

// DropIndex removes an index from a collection
func (se *StorageEngine) DropIndex(collName, fieldName string) error {
	// Drop index from index engine
	if err := se.indexEngine.DropIndex(collName, fieldName); err != nil {
		return fmt.Errorf("failed to drop index: %w", err)
	}

	// Update collection metadata
	se.collectionsMu.Lock()
	if collInfo, exists := se.collections[collName]; exists {
		// Remove fieldName from indexes slice
		for i, idx := range collInfo.Indexes {
			if idx == fieldName {
				collInfo.Indexes = append(collInfo.Indexes[:i], collInfo.Indexes[i+1:]...)
				break
			}
		}
	}
	se.collectionsMu.Unlock()

	return nil
}

// FindByIndex finds documents using an index
func (se *StorageEngine) FindByIndex(collName, fieldName string, value interface{}) ([]domain.Document, error) {
	// Get the index
	index, exists := se.indexEngine.GetIndex(collName, fieldName)
	if !exists {
		return nil, fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collName)
	}

	// Query the index to get document IDs
	docIDs := index.Query(value)
	if len(docIDs) == 0 {
		return []domain.Document{}, nil
	}

	// Retrieve documents from memory manager
	var results []domain.Document
	for _, docID := range docIDs {
		doc, err := se.memoryMgr.GetById(collName, docID)
		if err != nil {
			// Skip documents that no longer exist
			continue
		}
		results = append(results, doc)
	}

	return results, nil
}

// UpdateIndex rebuilds an index for a collection
func (se *StorageEngine) UpdateIndex(collName, fieldName string) error {
	// Check if index exists
	_, exists := se.indexEngine.GetIndex(collName, fieldName)
	if !exists {
		return fmt.Errorf("index on field %s does not exist in collection %s", fieldName, collName)
	}

	// Rebuild the index
	return se.buildIndexForCollection(collName, fieldName)
}

// Helper methods

func (se *StorageEngine) generateDocumentID(collName string) string {
	// Simple ID generation - in production, use UUID or similar
	// Use atomic counter to ensure uniqueness even in rapid succession
	counter := atomic.AddInt64(&se.idCounter, 1)
	return fmt.Sprintf("%s_%d_%d", collName, time.Now().UnixNano(), counter)
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

// buildIndexForCollection builds an index for all documents in a collection
func (se *StorageEngine) buildIndexForCollection(collName, fieldName string) error {
	// Get all documents from memory manager
	documents, err := se.memoryMgr.GetAllDocuments(collName)
	if err != nil {
		return fmt.Errorf("failed to get documents: %w", err)
	}

	// Create a domain.Collection for the index engine
	collection := &domain.Collection{
		Name:      collName,
		Documents: make(map[string]domain.Document),
	}

	// Convert documents to domain.Document format
	for docID, docData := range documents {
		if doc, ok := docData.(domain.Document); ok {
			collection.Documents[docID] = doc
		}
	}

	// Build the index
	return se.indexEngine.BuildIndexForCollection(collName, fieldName, collection)
}

// updateIndexesForDocument updates all indexes when a document changes
func (se *StorageEngine) updateIndexesForDocument(collName, docID string, oldDoc, newDoc domain.Document) {
	// Ensure _id index exists
	if err := se.indexEngine.CreateIndex(collName, "_id"); err != nil {
		// Index might already exist, which is fine
	}

	// Update all indexes
	se.indexEngine.UpdateIndexForDocument(collName, docID, oldDoc, newDoc)
}
