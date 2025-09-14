package v2

import "time"

// StorageOption configures the v2 storage engine
type StorageOption func(*StorageEngine)

// WithWALDir sets the directory for WAL files
func WithWALDir(dir string) StorageOption {
	return func(engine *StorageEngine) {
		engine.walDir = dir
	}
}

// WithDataDir sets the directory for data files
func WithDataDir(dir string) StorageOption {
	return func(engine *StorageEngine) {
		engine.dataDir = dir
	}
}

// WithCheckpointDir sets the directory for checkpoint files
func WithCheckpointDir(dir string) StorageOption {
	return func(engine *StorageEngine) {
		engine.checkpointDir = dir
	}
}

// WithMaxMemory sets the maximum memory usage in MB
func WithMaxMemory(mb int) StorageOption {
	return func(engine *StorageEngine) {
		engine.maxMemoryMB = mb
	}
}

// WithCheckpointInterval sets how often to perform checkpoints
func WithCheckpointInterval(interval time.Duration) StorageOption {
	return func(engine *StorageEngine) {
		engine.checkpointInterval = interval
	}
}

// WithDurabilityLevel sets the durability guarantee level
func WithDurabilityLevel(level DurabilityLevel) StorageOption {
	return func(engine *StorageEngine) {
		engine.durabilityLevel = level
	}
}

// WithMaxWALSize sets the maximum WAL size before forced checkpoint
func WithMaxWALSize(size int64) StorageOption {
	return func(engine *StorageEngine) {
		engine.maxWALSize = size
	}
}

// WithCheckpointThreshold sets the minimum dirty pages before checkpoint
func WithCheckpointThreshold(threshold int) StorageOption {
	return func(engine *StorageEngine) {
		engine.checkpointThreshold = threshold
	}
}

// WithCompression enables WAL entry compression
func WithCompression(enabled bool) StorageOption {
	return func(engine *StorageEngine) {
		engine.compressionEnabled = enabled
	}
}
