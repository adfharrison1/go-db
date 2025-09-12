package storage

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

// WithNoSaves disables automatic disk writes (only saves on shutdown)
func WithNoSaves(enabled bool) StorageOption {
	return func(engine *StorageEngine) {
		engine.noSaves = enabled
	}
}
