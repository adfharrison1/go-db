package v2

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// NewCheckpointManager creates a new checkpoint manager
func NewCheckpointManager(engine *StorageEngine) *CheckpointManager {
	return &CheckpointManager{
		engine:         engine,
		interval:       engine.checkpointInterval,
		threshold:      engine.checkpointThreshold,
		maxWALSize:     engine.maxWALSize,
		lastCheckpoint: time.Now(),
	}
}

// Run starts the checkpoint manager background worker
func (cm *CheckpointManager) Run() {
	defer cm.engine.backgroundWg.Done()

	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := cm.Checkpoint(); err != nil {
				// Log error but continue running
				fmt.Printf("Checkpoint failed: %v\n", err)
			}
		case <-cm.engine.stopChan:
			// Perform final checkpoint before shutdown
			if err := cm.Checkpoint(); err != nil {
				fmt.Printf("Final checkpoint failed: %v\n", err)
			}
			return
		}
	}
}

// Checkpoint performs a checkpoint operation
func (cm *CheckpointManager) Checkpoint() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check if checkpoint is needed
	if !cm.shouldCheckpoint() {
		return nil
	}

	start := time.Now()
	defer func() {
		cm.lastCheckpoint = time.Now()
		cm.engine.updateStats(func(s *StorageStats) {
			s.CheckpointsPerformed++
			s.LastCheckpoint = cm.lastCheckpoint
		})
	}()

	// Get all collections to checkpoint
	collections := cm.getCollectionsToCheckpoint()

	// Export indexes
	indexes := cm.engine.indexEngine.ExportIndexes()

	// Create checkpoint data
	checkpointData := &CheckpointData{
		Timestamp:   time.Now(),
		Collections: collections,
		Indexes:     indexes,
		LSN:         cm.engine.walEngine.GetCurrentLSN(),
	}

	// Write checkpoint to disk
	if err := cm.writeCheckpoint(checkpointData); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	// Clean up old WAL files
	if err := cm.cleanupOldWALFiles(); err != nil {
		// Log but don't fail checkpoint
		fmt.Printf("Failed to cleanup old WAL files: %v\n", err)
	}

	// Clean up old checkpoint files
	if err := cm.cleanupOldCheckpointFiles(); err != nil {
		// Log but don't fail checkpoint
		fmt.Printf("Failed to cleanup old checkpoint files: %v\n", err)
	}

	// Rotate WAL file
	if err := cm.engine.walEngine.RotateWALFile(); err != nil {
		return fmt.Errorf("failed to rotate WAL file: %w", err)
	}

	duration := time.Since(start)
	fmt.Printf("Checkpoint completed in %v\n", duration)

	return nil
}

// CheckpointData represents the data written during a checkpoint
type CheckpointData struct {
	Timestamp   time.Time                      `json:"timestamp"`
	Collections map[string]*CollectionData     `json:"collections"`
	Indexes     map[string]map[string][]string `json:"indexes"` // collection -> field -> docIDs
	LSN         int64                          `json:"lsn"`
}

// CollectionData represents collection data in a checkpoint
type CollectionData struct {
	Name          string                 `json:"name"`
	DocumentCount int64                  `json:"document_count"`
	LastModified  time.Time              `json:"last_modified"`
	Indexes       []string               `json:"indexes"`
	Documents     map[string]interface{} `json:"documents"`
}

// Private methods

func (cm *CheckpointManager) shouldCheckpoint() bool {
	// Check time-based checkpoint
	if time.Since(cm.lastCheckpoint) >= cm.interval {
		return true
	}

	// Check WAL size-based checkpoint
	walFiles, err := cm.engine.walEngine.GetWALFiles()
	if err != nil {
		return false
	}

	totalSize := int64(0)
	for _, file := range walFiles {
		if info, err := os.Stat(file); err == nil {
			totalSize += info.Size()
		}
	}

	if totalSize >= cm.maxWALSize {
		return true
	}

	// Check dirty collections threshold
	dirtyCount := cm.getDirtyCollectionCount()
	if dirtyCount >= cm.threshold {
		return true
	}

	return false
}

func (cm *CheckpointManager) getDirtyCollectionCount() int {
	cm.engine.collectionsMu.RLock()
	defer cm.engine.collectionsMu.RUnlock()

	count := 0
	for _, coll := range cm.engine.collections {
		if coll.State == CollectionStateDirty {
			count++
		}
	}
	return count
}

func (cm *CheckpointManager) getCollectionsToCheckpoint() map[string]*CollectionData {
	cm.engine.collectionsMu.RLock()
	defer cm.engine.collectionsMu.RUnlock()

	collections := make(map[string]*CollectionData)

	for name, collInfo := range cm.engine.collections {
		// Only checkpoint dirty collections
		if collInfo.State != CollectionStateDirty {
			continue
		}

		// Get documents from memory manager
		documents, err := cm.engine.memoryMgr.GetAllDocuments(name)
		if err != nil {
			fmt.Printf("Failed to get documents for collection %s: %v\n", name, err)
			continue
		}

		collections[name] = &CollectionData{
			Name:          name,
			DocumentCount: collInfo.DocumentCount,
			LastModified:  collInfo.LastModified,
			Indexes:       collInfo.Indexes,
			Documents:     documents,
		}

		// Mark collection as clean
		collInfo.State = CollectionStateLoaded
	}

	return collections
}

func (cm *CheckpointManager) writeCheckpoint(data *CheckpointData) error {
	// Create checkpoint filename
	filename := fmt.Sprintf("checkpoint_%d.json", data.Timestamp.Unix())
	filePath := filepath.Join(cm.engine.checkpointDir, filename)

	// Serialize checkpoint data
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint data: %w", err)
	}

	// Write to temporary file first
	tempFile := filePath + ".tmp"
	if err := os.WriteFile(tempFile, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write temporary checkpoint file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, filePath); err != nil {
		return fmt.Errorf("failed to rename checkpoint file: %w", err)
	}

	// Update latest checkpoint symlink
	latestFile := filepath.Join(cm.engine.checkpointDir, "latest_checkpoint.json")
	os.Remove(latestFile) // Ignore error if file doesn't exist
	if err := os.Symlink(filename, latestFile); err != nil {
		// Log but don't fail
		fmt.Printf("Failed to create latest checkpoint symlink: %v\n", err)
	}

	return nil
}

func (cm *CheckpointManager) cleanupOldWALFiles() error {
	walFiles, err := cm.engine.walEngine.GetWALFiles()
	if err != nil {
		return err
	}

	// Don't cleanup if we have fewer files than retention count
	if len(walFiles) <= cm.engine.walRetentionCount {
		return nil
	}

	// Get current checkpoint LSN to determine which WAL files are safe to delete
	checkpoint, err := cm.LoadCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint for WAL cleanup: %w", err)
	}

	// If no checkpoint exists, don't delete any WAL files
	if checkpoint == nil {
		return nil
	}

	// Sort WAL files by modification time (newest first)
	sort.Slice(walFiles, func(i, j int) bool {
		infoI, errI := os.Stat(walFiles[i])
		infoJ, errJ := os.Stat(walFiles[j])
		if errI != nil || errJ != nil {
			return false
		}
		return infoI.ModTime().After(infoJ.ModTime())
	})

	// Keep the most recent files up to retention count
	filesToDelete := walFiles[cm.engine.walRetentionCount:]

	// Only delete files that are older than the checkpoint
	for _, file := range filesToDelete {
		// Check if this WAL file is safe to delete
		if cm.isWALFileSafeToDelete(file, checkpoint.LSN) {
			if err := os.Remove(file); err != nil {
				fmt.Printf("Failed to delete WAL file %s: %v\n", file, err)
			} else {
				fmt.Printf("Deleted old WAL file: %s\n", filepath.Base(file))
			}
		}
	}

	return nil
}

// isWALFileSafeToDelete checks if a WAL file is safe to delete
func (cm *CheckpointManager) isWALFileSafeToDelete(walFile string, checkpointLSN int64) bool {
	// Read the WAL file to find its max LSN
	entries, err := cm.engine.walEngine.ReadEntries(walFile)
	if err != nil {
		fmt.Printf("Failed to read WAL file %s: %v\n", walFile, err)
		return false
	}

	// Find the maximum LSN in this WAL file
	maxLSN := int64(0)
	for _, entry := range entries {
		if entry.LSN > maxLSN {
			maxLSN = entry.LSN
		}
	}

	// WAL file is safe to delete if its max LSN is less than or equal to checkpoint LSN
	return maxLSN <= checkpointLSN
}

// LoadCheckpoint loads the latest checkpoint
func (cm *CheckpointManager) LoadCheckpoint() (*CheckpointData, error) {
	latestFile := filepath.Join(cm.engine.checkpointDir, "latest_checkpoint.json")

	// Check if latest checkpoint exists
	if _, err := os.Stat(latestFile); os.IsNotExist(err) {
		return nil, nil // No checkpoint found
	}

	// Read checkpoint file
	data, err := os.ReadFile(latestFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint file: %w", err)
	}

	var checkpoint CheckpointData
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint data: %w", err)
	}

	return &checkpoint, nil
}

// cleanupOldCheckpointFiles removes old checkpoint files based on retention policy
func (cm *CheckpointManager) cleanupOldCheckpointFiles() error {
	// Get all checkpoint files
	pattern := filepath.Join(cm.engine.checkpointDir, "checkpoint_*.json")
	checkpointFiles, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list checkpoint files: %w", err)
	}

	// Don't cleanup if we have fewer files than retention count
	if len(checkpointFiles) <= cm.engine.checkpointRetentionCount {
		return nil
	}

	// Sort checkpoint files by modification time (newest first)
	sort.Slice(checkpointFiles, func(i, j int) bool {
		infoI, errI := os.Stat(checkpointFiles[i])
		infoJ, errJ := os.Stat(checkpointFiles[j])
		if errI != nil || errJ != nil {
			return false
		}
		return infoI.ModTime().After(infoJ.ModTime())
	})

	// Keep the most recent files up to retention count
	filesToDelete := checkpointFiles[cm.engine.checkpointRetentionCount:]

	// Delete old checkpoint files (but never delete the latest_checkpoint.json symlink)
	for _, file := range filesToDelete {
		if filepath.Base(file) != "latest_checkpoint.json" {
			if err := os.Remove(file); err != nil {
				fmt.Printf("Failed to delete checkpoint file %s: %v\n", file, err)
			} else {
				fmt.Printf("Deleted old checkpoint file: %s\n", filepath.Base(file))
			}
		}
	}

	return nil
}
