package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationMode_String(t *testing.T) {
	tests := []struct {
		mode     OperationMode
		expected string
	}{
		{ModeDualWrite, "dual-write"},
		{ModeNoSaves, "no-saves"},
		{ModeMemoryMap, "memory-map"},
		{OperationMode(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

func TestStorageEngine_OperationModes(t *testing.T) {
	tests := []struct {
		name              string
		mode              OperationMode
		expectedNoSaves   bool
		expectedMemoryMap bool
	}{
		{
			name:              "dual-write mode",
			mode:              ModeDualWrite,
			expectedNoSaves:   false,
			expectedMemoryMap: false,
		},
		{
			name:              "no-saves mode",
			mode:              ModeNoSaves,
			expectedNoSaves:   true,
			expectedMemoryMap: false,
		},
		{
			name:              "memory-map mode",
			mode:              ModeMemoryMap,
			expectedNoSaves:   false,
			expectedMemoryMap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewStorageEngine(WithOperationMode(tt.mode))
			defer engine.StopBackgroundWorkers()

			assert.Equal(t, tt.mode, engine.GetOperationMode())
			assert.Equal(t, tt.expectedNoSaves, engine.IsNoSavesEnabled())
			assert.Equal(t, tt.expectedMemoryMap, engine.IsMemoryMapEnabled())
		})
	}
}

func TestStorageEngine_WithNoSaves_BackwardCompatibility(t *testing.T) {
	t.Run("no-saves enabled", func(t *testing.T) {
		engine := NewStorageEngine(WithNoSaves(true))
		defer engine.StopBackgroundWorkers()

		assert.Equal(t, ModeNoSaves, engine.GetOperationMode())
		assert.True(t, engine.IsNoSavesEnabled())
		assert.False(t, engine.IsMemoryMapEnabled())
	})

	t.Run("no-saves disabled", func(t *testing.T) {
		engine := NewStorageEngine(WithNoSaves(false))
		defer engine.StopBackgroundWorkers()

		assert.Equal(t, ModeDualWrite, engine.GetOperationMode())
		assert.False(t, engine.IsNoSavesEnabled())
		assert.False(t, engine.IsMemoryMapEnabled())
	})
}

func TestStorageEngine_DefaultMode(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Should default to dual-write mode
	assert.Equal(t, ModeDualWrite, engine.GetOperationMode())
	assert.False(t, engine.IsNoSavesEnabled())
	assert.False(t, engine.IsMemoryMapEnabled())
}

func TestStorageEngine_ModeSwitching(t *testing.T) {
	engine := NewStorageEngine()
	defer engine.StopBackgroundWorkers()

	// Start with default mode
	assert.Equal(t, ModeDualWrite, engine.GetOperationMode())

	// Test that we can't change mode after creation
	// (This would require a new engine instance)
	// This test documents the current behavior
}

func TestStorageEngine_AllModesBasicFunctionality(t *testing.T) {
	modes := []OperationMode{ModeDualWrite, ModeNoSaves, ModeMemoryMap}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			engine := NewStorageEngine(
				WithOperationMode(mode),
				WithDataDir("/tmp/go-db-test"),
			)
			defer engine.StopBackgroundWorkers()

			// Test basic functionality works in all modes
			doc := map[string]interface{}{
				"name": "test",
				"age":  30,
			}

			result, err := engine.Insert("test_collection", doc)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Contains(t, result, "_id")

			// Verify the document was inserted
			retrieved, err := engine.GetById("test_collection", result["_id"].(string))
			require.NoError(t, err)
			require.Equal(t, "test", retrieved["name"])
			require.Equal(t, 30, retrieved["age"])
		})
	}
}
