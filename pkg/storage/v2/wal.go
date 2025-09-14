package v2

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

// NewWALEngine creates a new WAL engine
func NewWALEngine(walDir string, durabilityLevel DurabilityLevel, compressionEnabled bool) *WALEngine {
	return &WALEngine{
		walDir:             walDir,
		durabilityLevel:    durabilityLevel,
		compressionEnabled: compressionEnabled,
		currentLSN:         0,
	}
}

// WriteEntry writes a WAL entry to the log
func (w *WALEngine) WriteEntry(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Assign LSN
	entry.LSN = w.currentLSN
	w.currentLSN++

	// Calculate checksum
	entry.Checksum = w.calculateChecksum(entry)

	// Ensure WAL file is open
	if err := w.ensureWALFile(); err != nil {
		return fmt.Errorf("failed to ensure WAL file: %w", err)
	}

	// Serialize entry
	data, err := w.serializeEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize WAL entry: %w", err)
	}

	// Write to WAL file
	if err := w.writeToWALFile(data); err != nil {
		return fmt.Errorf("failed to write to WAL file: %w", err)
	}

	// Apply durability guarantees
	if err := w.applyDurability(); err != nil {
		return fmt.Errorf("failed to apply durability: %w", err)
	}

	return nil
}

// ReadEntries reads WAL entries from a file
func (w *WALEngine) ReadEntries(filename string) ([]*WALEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	var entries []*WALEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		entry, err := w.deserializeEntry(line)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize WAL entry: %w", err)
		}

		// Verify checksum
		if !w.verifyChecksum(entry) {
			return nil, fmt.Errorf("checksum verification failed for LSN %d", entry.LSN)
		}

		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL file: %w", err)
	}

	return entries, nil
}

// GetCurrentLSN returns the current log sequence number
func (w *WALEngine) GetCurrentLSN() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.currentLSN
}

// Close closes the WAL engine
func (w *WALEngine) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.walFile != nil {
		return w.walFile.File.Close()
	}
	return nil
}

// Private methods

func (w *WALEngine) ensureWALFile() error {
	if w.walFile != nil {
		return nil
	}

	// Create WAL filename with timestamp
	filename := fmt.Sprintf("wal_%d.log", time.Now().Unix())
	filepath := filepath.Join(w.walDir, filename)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create WAL file: %w", err)
	}

	w.walFile = &WALFile{
		Path:     filepath,
		File:     file,
		Position: 0,
		Entries:  0,
	}

	return nil
}

func (w *WALEngine) writeToWALFile(data []byte) error {
	if w.walFile == nil {
		return fmt.Errorf("WAL file not initialized")
	}

	n, err := w.walFile.File.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to WAL file: %w", err)
	}

	w.walFile.Position += int64(n)
	w.walFile.Entries++

	return nil
}

func (w *WALEngine) applyDurability() error {
	switch w.durabilityLevel {
	case DurabilityNone:
		// No durability guarantees
		return nil
	case DurabilityMemory:
		// Data is in memory, no additional action needed
		return nil
	case DurabilityOS:
		// Flush to OS page cache - no explicit sync needed
		// The OS will handle flushing to disk when appropriate
		return nil
	case DurabilityFull:
		// Full durability with fsync - force data to disk
		return w.walFile.File.Sync()
	default:
		return fmt.Errorf("unknown durability level: %d", w.durabilityLevel)
	}
}

func (w *WALEngine) serializeEntry(entry *WALEntry) ([]byte, error) {
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal WAL entry: %w", err)
	}

	// Add newline for line-based reading
	return append(data, '\n'), nil
}

func (w *WALEngine) deserializeEntry(data []byte) (*WALEntry, error) {
	var entry WALEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal WAL entry: %w", err)
	}
	return &entry, nil
}

func (w *WALEngine) calculateChecksum(entry *WALEntry) uint32 {
	// Create a copy without checksum for calculation
	entryCopy := *entry
	entryCopy.Checksum = 0

	data, err := json.Marshal(entryCopy)
	if err != nil {
		return 0
	}

	return crc32.ChecksumIEEE(data)
}

func (w *WALEngine) verifyChecksum(entry *WALEntry) bool {
	expectedChecksum := w.calculateChecksum(entry)
	return entry.Checksum == expectedChecksum
}

// GetWALFiles returns a list of WAL files in the WAL directory
func (w *WALEngine) GetWALFiles() ([]string, error) {
	files, err := filepath.Glob(filepath.Join(w.walDir, "wal_*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	return files, nil
}

// RotateWALFile creates a new WAL file and closes the current one
func (w *WALEngine) RotateWALFile() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.walFile != nil {
		if err := w.walFile.File.Close(); err != nil {
			return fmt.Errorf("failed to close current WAL file: %w", err)
		}
		w.walFile = nil
	}

	return w.ensureWALFile()
}
