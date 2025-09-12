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

// WithOperationMode sets the storage engine operation mode
func WithOperationMode(mode OperationMode) StorageOption {
	return func(engine *StorageEngine) {
		engine.operationMode = mode
		// Update derived flags
		engine.noSaves = (mode == ModeNoSaves)
		engine.useMemoryMap = (mode == ModeMemoryMap)
	}
}

// WithNoSaves disables automatic disk writes (only saves on shutdown)
// Deprecated: Use WithOperationMode(ModeNoSaves) instead
func WithNoSaves(enabled bool) StorageOption {
	return func(engine *StorageEngine) {
		if enabled {
			engine.operationMode = ModeNoSaves
			engine.noSaves = true
			engine.useMemoryMap = false
		} else {
			engine.operationMode = ModeDualWrite
			engine.noSaves = false
			engine.useMemoryMap = false
		}
	}
}
