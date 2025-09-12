package storage

import (
	"runtime"
)

// GetMemoryStats returns current memory usage statistics
func (se *StorageEngine) GetMemoryStats() map[string]interface{} {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return map[string]interface{}{
		"alloc_mb":       m.Alloc / 1024 / 1024,
		"total_alloc_mb": m.TotalAlloc / 1024 / 1024,
		"sys_mb":         m.Sys / 1024 / 1024,
		"num_goroutines": runtime.NumGoroutine(),
		"cache_size":     se.cache.list.Len(),
		"collections":    len(se.collections),
	}
}

// StartBackgroundWorkers starts background workers (disk write queue processing)
func (se *StorageEngine) StartBackgroundWorkers() {
	// Background workers are now started automatically in NewStorageEngine
	// This method is kept for compatibility but does nothing
}

// StopBackgroundWorkers stops background workers
func (se *StorageEngine) StopBackgroundWorkers() {
	se.stopOnce.Do(func() {
		close(se.stopChan)
		close(se.diskWriteQueue)
	})

	se.diskWriteWg.Wait()
	se.backgroundWg.Wait()
}
