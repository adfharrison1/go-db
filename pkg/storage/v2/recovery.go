package v2

import (
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(engine *StorageEngine) *RecoveryManager {
	return &RecoveryManager{
		engine: engine,
	}
}

// Recover performs recovery from WAL and checkpoint files
func (rm *RecoveryManager) Recover() error {
	start := time.Now()
	defer func() {
		rm.engine.updateStats(func(s *StorageStats) {
			s.RecoveryTime = time.Since(start)
		})
	}()

	log.Println("Starting recovery process...")

	// Load latest checkpoint
	checkpoint, err := rm.engine.checkpointMgr.LoadCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Restore from checkpoint if available
	if checkpoint != nil {
		if err := rm.restoreFromCheckpoint(checkpoint); err != nil {
			return fmt.Errorf("failed to restore from checkpoint: %w", err)
		}
		log.Printf("Restored from checkpoint at LSN %d", checkpoint.LSN)
	}

	// Replay WAL entries since checkpoint
	if err := rm.replayWALEntries(checkpoint); err != nil {
		return fmt.Errorf("failed to replay WAL entries: %w", err)
	}

	log.Printf("Recovery completed in %v", time.Since(start))
	return nil
}

// restoreFromCheckpoint restores the database state from a checkpoint
func (rm *RecoveryManager) restoreFromCheckpoint(checkpoint *CheckpointData) error {
	// Restore collections
	for name, collData := range checkpoint.Collections {
		// Create collection
		if err := rm.engine.CreateCollection(name); err != nil {
			return fmt.Errorf("failed to create collection %s: %w", name, err)
		}

		// Restore collection metadata
		rm.engine.collectionsMu.Lock()
		if collInfo, exists := rm.engine.collections[name]; exists {
			collInfo.DocumentCount = collData.DocumentCount
			collInfo.LastModified = collData.LastModified
			collInfo.Indexes = collData.Indexes
			collInfo.State = CollectionStateLoaded
		}
		rm.engine.collectionsMu.Unlock()

		// Restore documents to memory
		for docID, docData := range collData.Documents {
			if doc, ok := docData.(map[string]interface{}); ok {
				// Convert to domain.Document
				domainDoc := make(map[string]interface{})
				for k, v := range doc {
					domainDoc[k] = v
				}

				// Insert into memory manager
				if err := rm.engine.memoryMgr.InsertDocument(name, domainDoc); err != nil {
					return fmt.Errorf("failed to restore document %s in collection %s: %w", docID, name, err)
				}
			}
		}

		// Restore indexes
		for _, indexName := range collData.Indexes {
			if err := rm.engine.indexEngine.CreateIndex(name, indexName); err != nil {
				return fmt.Errorf("failed to restore index %s for collection %s: %w", indexName, name, err)
			}
		}
	}

	return nil
}

// replayWALEntries replays WAL entries since the last checkpoint
func (rm *RecoveryManager) replayWALEntries(checkpoint *CheckpointData) error {
	// Get all WAL files
	walFiles, err := rm.engine.walEngine.GetWALFiles()
	if err != nil {
		return fmt.Errorf("failed to get WAL files: %w", err)
	}

	if len(walFiles) == 0 {
		return nil // No WAL files to replay
	}

	// Sort WAL files by name (which includes timestamp)
	sort.Strings(walFiles)

	// Determine starting LSN
	startLSN := int64(0)
	if checkpoint != nil {
		startLSN = checkpoint.LSN
	}

	// Replay entries from each WAL file
	for _, walFile := range walFiles {
		if err := rm.replayWALFile(walFile, startLSN); err != nil {
			return fmt.Errorf("failed to replay WAL file %s: %w", walFile, err)
		}
	}

	return nil
}

// replayWALFile replays entries from a single WAL file
func (rm *RecoveryManager) replayWALFile(filename string, startLSN int64) error {
	entries, err := rm.engine.walEngine.ReadEntries(filename)
	if err != nil {
		return fmt.Errorf("failed to read WAL entries: %w", err)
	}

	// Filter entries by LSN
	var entriesToReplay []*WALEntry
	for _, entry := range entries {
		if entry.LSN > startLSN {
			entriesToReplay = append(entriesToReplay, entry)
		}
	}

	// Replay entries in order
	for _, entry := range entriesToReplay {
		if err := rm.replayWALEntry(entry); err != nil {
			return fmt.Errorf("failed to replay WAL entry LSN %d: %w", entry.LSN, err)
		}
	}

	return nil
}

// replayWALEntry replays a single WAL entry
func (rm *RecoveryManager) replayWALEntry(entry *WALEntry) error {
	switch entry.Type {
	case WALEntryInsert:
		return rm.replayInsert(entry)
	case WALEntryUpdate:
		return rm.replayUpdate(entry)
	case WALEntryReplace:
		return rm.replayReplace(entry)
	case WALEntryDelete:
		return rm.replayDelete(entry)
	case WALEntryBatchInsert:
		return rm.replayBatchInsert(entry)
	case WALEntryBatchUpdate:
		return rm.replayBatchUpdate(entry)
	case WALEntryCheckpoint:
		// Checkpoint entries are handled separately
		return nil
	case WALEntryCommit:
		// Commit entries are handled separately
		return nil
	default:
		return fmt.Errorf("unknown WAL entry type: %d", entry.Type)
	}
}

// replayInsert replays an insert operation
func (rm *RecoveryManager) replayInsert(entry *WALEntry) error {
	// Ensure collection exists
	if err := rm.engine.CreateCollection(entry.Collection); err != nil {
		return fmt.Errorf("failed to create collection %s: %w", entry.Collection, err)
	}

	// Insert document
	return rm.engine.memoryMgr.InsertDocument(entry.Collection, entry.Document)
}

// replayUpdate replays an update operation
func (rm *RecoveryManager) replayUpdate(entry *WALEntry) error {
	// Ensure collection exists
	if err := rm.engine.CreateCollection(entry.Collection); err != nil {
		return fmt.Errorf("failed to create collection %s: %w", entry.Collection, err)
	}

	// Get existing document
	existing, err := rm.engine.memoryMgr.GetById(entry.Collection, entry.DocumentID)
	if err != nil {
		// If document doesn't exist, skip the update
		return nil
	}

	// Merge updates
	updated := rm.engine.mergeDocuments(existing, entry.Updates)

	// Update document
	return rm.engine.memoryMgr.UpdateDocument(entry.Collection, entry.DocumentID, updated)
}

// replayReplace replays a replace operation
func (rm *RecoveryManager) replayReplace(entry *WALEntry) error {
	// Ensure collection exists
	if err := rm.engine.CreateCollection(entry.Collection); err != nil {
		return fmt.Errorf("failed to create collection %s: %w", entry.Collection, err)
	}

	return rm.engine.memoryMgr.ReplaceDocument(entry.Collection, entry.DocumentID, entry.Document)
}

// replayDelete replays a delete operation
func (rm *RecoveryManager) replayDelete(entry *WALEntry) error {
	// Ensure collection exists
	if err := rm.engine.CreateCollection(entry.Collection); err != nil {
		return fmt.Errorf("failed to create collection %s: %w", entry.Collection, err)
	}

	return rm.engine.memoryMgr.DeleteDocument(entry.Collection, entry.DocumentID)
}

// replayBatchInsert replays a batch insert operation
func (rm *RecoveryManager) replayBatchInsert(entry *WALEntry) error {
	// Ensure collection exists
	if err := rm.engine.CreateCollection(entry.Collection); err != nil {
		return fmt.Errorf("failed to create collection %s: %w", entry.Collection, err)
	}

	// Extract batch documents from the special _batch field
	if batchData, ok := entry.Document["_batch"]; ok {
		if batchDocs, ok := batchData.([]interface{}); ok {
			// Convert to domain.Document slice
			docs := make([]domain.Document, len(batchDocs))
			for i, docData := range batchDocs {
				if doc, ok := docData.(map[string]interface{}); ok {
					docs[i] = domain.Document(doc)
				}
			}
			return rm.engine.memoryMgr.BatchInsertDocuments(entry.Collection, docs)
		}
	}
	return fmt.Errorf("invalid batch insert entry format")
}

// replayBatchUpdate replays a batch update operation
func (rm *RecoveryManager) replayBatchUpdate(entry *WALEntry) error {
	// Ensure collection exists
	if err := rm.engine.CreateCollection(entry.Collection); err != nil {
		return fmt.Errorf("failed to create collection %s: %w", entry.Collection, err)
	}

	_, err := rm.engine.memoryMgr.BatchUpdateDocuments(entry.Collection, entry.BatchOps)
	return err
}

// GetRecoveryStats returns recovery statistics
func (rm *RecoveryManager) GetRecoveryStats() map[string]interface{} {
	rm.engine.statsMu.RLock()
	defer rm.engine.statsMu.RUnlock()

	return map[string]interface{}{
		"recovery_time_ms": rm.engine.stats.RecoveryTime.Milliseconds(),
		"last_checkpoint":  rm.engine.stats.LastCheckpoint,
	}
}
