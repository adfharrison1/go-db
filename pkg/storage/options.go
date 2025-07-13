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
	}
}
