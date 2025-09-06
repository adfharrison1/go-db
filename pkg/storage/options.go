package storage

import "time"

type StorageOption func(*StorageEngine)

func WithMaxMemory(mb int) StorageOption {
	return func(engine *StorageEngine) {
		engine.maxMemoryMB = mb
	}
}

func WithDataDir(dir string) StorageOption {
	return func(engine *StorageEngine) {
		engine.dataDir = dir
	}
}

func WithBackgroundSave(interval time.Duration) StorageOption {
	return func(engine *StorageEngine) {
		engine.backgroundSave = true
		engine.saveInterval = interval
		engine.transactionSave = false // Disable transaction saves when background saves are enabled
	}
}

// WithTransactionSave enables saving after every write transaction (default: true)
func WithTransactionSave(enabled bool) StorageOption {
	return func(engine *StorageEngine) {
		engine.transactionSave = enabled
	}
}

// WithDataDirAndTransactionSave is a convenience option for setting both data directory and transaction saves
func WithDataDirAndTransactionSave(dir string, transactionSave bool) StorageOption {
	return func(engine *StorageEngine) {
		engine.dataDir = dir
		engine.transactionSave = transactionSave
	}
}
