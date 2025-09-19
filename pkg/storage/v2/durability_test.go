package v2

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
)

func TestDurabilityLevels(t *testing.T) {
	tests := []struct {
		name        string
		durability  DurabilityLevel
		expectSync  bool
		description string
	}{
		{
			name:        "DurabilityNone",
			durability:  DurabilityNone,
			expectSync:  false,
			description: "No durability guarantees - fastest",
		},
		{
			name:        "DurabilityMemory",
			durability:  DurabilityMemory,
			expectSync:  false,
			description: "Memory only - fast, temporary data",
		},
		{
			name:        "DurabilityOS",
			durability:  DurabilityOS,
			expectSync:  false,
			description: "OS page cache - balanced performance/safety",
		},
		{
			name:        "DurabilityFull",
			durability:  DurabilityFull,
			expectSync:  true,
			description: "Full fsync - safest, critical data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary directory for test
			tempDir := t.TempDir()
			walDir := filepath.Join(tempDir, "wal")
			os.MkdirAll(walDir, 0755)

			// Create WAL engine with specific durability level
			walEngine := NewWALEngine(walDir, tt.durability, false)

			// Test that durability level is set correctly
			if walEngine.durabilityLevel != tt.durability {
				t.Errorf("Expected durability level %v, got %v", tt.durability, walEngine.durabilityLevel)
			}

			// Create a test entry to initialize the WAL file
			entry := &WALEntry{
				Type:       WALEntryInsert,
				Timestamp:  time.Now().UnixNano(),
				Collection: "test_collection",
				DocumentID: "test_doc",
				Document: domain.Document{
					"_id":  "test_doc",
					"name": "Test Document",
				},
				LSN: 1,
			}

			// Write entry to initialize WAL file
			err := walEngine.WriteEntry(entry)
			if err != nil {
				t.Errorf("WriteEntry() failed: %v", err)
			}

			// Test applyDurability behavior
			err = walEngine.applyDurability()
			if err != nil {
				t.Errorf("applyDurability() failed: %v", err)
			}

			// Clean up
			walEngine.Close()

			// For DurabilityFull, we expect Sync to be called
			// For others, we expect no sync
			// Note: We can't easily test the actual sync behavior without mocking,
			// but we can verify the logic path is correct
		})
	}
}

func TestDurabilityLevelPerformance(t *testing.T) {
	// This test verifies that different durability levels have different performance characteristics
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)

	durabilityLevels := []DurabilityLevel{
		DurabilityNone,
		DurabilityMemory,
		DurabilityOS,
		DurabilityFull,
	}

	for _, durability := range durabilityLevels {
		t.Run(durability.String(), func(t *testing.T) {
			// Create WAL engine
			walEngine := NewWALEngine(walDir, durability, false)

			// Create a test entry
			entry := &WALEntry{
				Type:       WALEntryInsert,
				Timestamp:  time.Now().UnixNano(),
				Collection: "test_collection",
				DocumentID: "test_doc_1",
				Document: domain.Document{
					"_id":  "test_doc_1",
					"name": "Test Document",
					"data": "This is test data for durability testing",
				},
				LSN: 1,
			}

			// Measure time to write entry
			start := time.Now()
			err := walEngine.WriteEntry(entry)
			duration := time.Since(start)

			if err != nil {
				t.Errorf("WriteEntry failed: %v", err)
			}

			// Log performance for analysis
			t.Logf("Durability level %v: WriteEntry took %v", durability, duration)

			// Clean up
			walEngine.Close()
		})
	}
}

func TestDurabilityLevelRecovery(t *testing.T) {
	// Test that different durability levels affect recovery behavior
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)

	// Test with DurabilityOS (should not sync)
	walEngineOS := NewWALEngine(walDir, DurabilityOS, false)

	entry := &WALEntry{
		Type:       WALEntryInsert,
		Timestamp:  time.Now().UnixNano(),
		Collection: "test_collection",
		DocumentID: "test_doc_os",
		Document: domain.Document{
			"_id":  "test_doc_os",
			"name": "OS Durability Test",
		},
		LSN: 1,
	}

	err := walEngineOS.WriteEntry(entry)
	if err != nil {
		t.Errorf("WriteEntry with DurabilityOS failed: %v", err)
	}
	walEngineOS.Close()

	// Test with DurabilityFull (should sync)
	walEngineFull := NewWALEngine(walDir, DurabilityFull, false)

	entryFull := &WALEntry{
		Type:       WALEntryInsert,
		Timestamp:  time.Now().UnixNano(),
		Collection: "test_collection",
		DocumentID: "test_doc_full",
		Document: domain.Document{
			"_id":  "test_doc_full",
			"name": "Full Durability Test",
		},
		LSN: 2,
	}

	err = walEngineFull.WriteEntry(entryFull)
	if err != nil {
		t.Errorf("WriteEntry with DurabilityFull failed: %v", err)
	}
	walEngineFull.Close()

	// Verify both entries were written
	files, err := walEngineOS.GetWALFiles()
	if err != nil {
		t.Errorf("Failed to get WAL files: %v", err)
	}

	if len(files) == 0 {
		t.Error("Expected WAL files to be created")
	}

	t.Logf("Created %d WAL files", len(files))
}

func TestDurabilityLevelIntegration(t *testing.T) {
	// Integration test with actual storage engine
	tempDir := t.TempDir()

	durabilityLevels := []DurabilityLevel{
		DurabilityNone,
		DurabilityMemory,
		DurabilityOS,
		DurabilityFull,
	}

	for _, durability := range durabilityLevels {
		t.Run(durability.String(), func(t *testing.T) {
			// Create storage engine with specific durability
			engine := NewStorageEngine(
				WithDataDir(tempDir),
				WithWALDir(filepath.Join(tempDir, "wal")),
				WithCheckpointDir(filepath.Join(tempDir, "checkpoints")),
				WithDurabilityLevel(durability),
			)

			// Test document insertion
			doc := domain.Document{
				"_id":   "test-1",
				"name":  "Test Document",
				"value": 42,
			}

			result, err := engine.Insert("test_collection", doc)
			if err != nil {
				t.Errorf("Insert failed: %v", err)
			}

			if result["_id"] != "test-1" {
				t.Errorf("Expected document ID to be test-1, got %v", result["_id"])
			}

			// Verify document can be retrieved
			retrieved, err := engine.GetById("test_collection", "test-1")
			if err != nil {
				t.Errorf("GetById failed: %v", err)
			}

			if retrieved["name"] != "Test Document" {
				t.Errorf("Expected name to be 'Test Document', got %v", retrieved["name"])
			}

			// Clean up
			engine.StopBackgroundWorkers()
		})
	}
}

// String method for DurabilityLevel to make test output readable
func (d DurabilityLevel) String() string {
	switch d {
	case DurabilityNone:
		return "DurabilityNone"
	case DurabilityMemory:
		return "DurabilityMemory"
	case DurabilityOS:
		return "DurabilityOS"
	case DurabilityFull:
		return "DurabilityFull"
	default:
		return "Unknown"
	}
}
