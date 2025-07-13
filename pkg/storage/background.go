package storage

import (
	"runtime"
	"time"
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

// StartBackgroundWorkers starts background save workers
func (se *StorageEngine) StartBackgroundWorkers() {
	if !se.backgroundSave {
		return
	}

	se.backgroundWg.Add(1)
	go func() {
		defer se.backgroundWg.Done()
		ticker := time.NewTicker(se.saveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				se.saveDirtyCollections()
			case <-se.stopChan:
				return
			}
		}
	}()
}

// StopBackgroundWorkers stops background workers
func (se *StorageEngine) StopBackgroundWorkers() {
	select {
	case <-se.stopChan:
		// Channel already closed, do nothing
	default:
		close(se.stopChan)
	}
	se.backgroundWg.Wait()
}
