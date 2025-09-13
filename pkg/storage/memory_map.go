package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"
)

// MemoryMappedFile represents a memory-mapped file for efficient data access
type MemoryMappedFile struct {
	file     *os.File
	data     []byte
	size     int64
	path     string
	readOnly bool
	mu       sync.RWMutex
}

// MemoryMapManager manages memory-mapped files for collections
type MemoryMapManager struct {
	maps    map[string]*MemoryMappedFile
	mu      sync.RWMutex
	dataDir string
	fileExt string
}

// NewMemoryMapManager creates a new memory map manager
func NewMemoryMapManager(dataDir, fileExt string) *MemoryMapManager {
	return &MemoryMapManager{
		maps:    make(map[string]*MemoryMappedFile),
		dataDir: dataDir,
		fileExt: fileExt,
	}
}

// OpenCollection opens or creates a memory-mapped file for a collection
func (mm *MemoryMapManager) OpenCollection(collection string, readOnly bool) (*MemoryMappedFile, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Check if already open
	if mf, exists := mm.maps[collection]; exists {
		return mf, nil
	}

	// Create file path
	filePath := filepath.Join(mm.dataDir, collection+mm.fileExt)

	// Open or create file
	var file *os.File
	var err error

	if readOnly {
		file, err = os.OpenFile(filePath, os.O_RDONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open/create file %s: %w", filePath, err)
		}
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}
	fileSize := stat.Size()

	// If file is empty, initialize with minimum size
	if fileSize == 0 {
		fileSize = 4096 // 4KB minimum
		if !readOnly {
			// Write initial data to make file non-empty
			_, err = file.WriteAt(make([]byte, fileSize), 0)
			if err != nil {
				file.Close()
				return nil, fmt.Errorf("failed to initialize file %s: %w", filePath, err)
			}
		}
	}

	// Memory map the file
	data, err := mm.mmap(file, fileSize, readOnly)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to memory map file %s: %w", filePath, err)
	}

	// Create memory mapped file
	mf := &MemoryMappedFile{
		file:     file,
		data:     data,
		size:     fileSize,
		path:     filePath,
		readOnly: readOnly,
	}

	// Store in manager
	mm.maps[collection] = mf

	return mf, nil
}

// CloseCollection closes a memory-mapped file
func (mm *MemoryMapManager) CloseCollection(collection string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mf, exists := mm.maps[collection]
	if !exists {
		return nil // Already closed
	}

	// Unmap memory
	err := mm.munmap(mf.data)
	if err != nil {
		return fmt.Errorf("failed to unmap memory for %s: %w", collection, err)
	}

	// Close file
	err = mf.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close file %s: %w", mf.path, err)
	}

	// Remove from manager
	delete(mm.maps, collection)

	return nil
}

// GetCollection returns an existing memory-mapped file for a collection
func (mm *MemoryMapManager) GetCollection(collection string) (*MemoryMappedFile, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	mf, exists := mm.maps[collection]
	return mf, exists
}

// CloseAll closes all memory-mapped files
func (mm *MemoryMapManager) CloseAll() error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	var lastErr error
	for collection, mf := range mm.maps {
		// Unmap memory
		if err := mm.munmap(mf.data); err != nil {
			lastErr = fmt.Errorf("failed to unmap memory for %s: %w", collection, err)
		}

		// Close file
		if err := mf.file.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close file %s: %w", mf.path, err)
		}
	}

	// Clear maps
	mm.maps = make(map[string]*MemoryMappedFile)

	return lastErr
}

// mmap performs the actual memory mapping using standard syscalls
func (mm *MemoryMapManager) mmap(file *os.File, size int64, readOnly bool) ([]byte, error) {
	prot := syscall.PROT_READ
	if !readOnly {
		prot |= syscall.PROT_WRITE
	}

	flags := syscall.MAP_SHARED
	if readOnly {
		flags = syscall.MAP_PRIVATE
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), prot, flags)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// munmap unmaps the memory
func (mm *MemoryMapManager) munmap(data []byte) error {
	return syscall.Munmap(data)
}

// Read reads data from the memory-mapped file
func (mf *MemoryMappedFile) Read(offset int64, length int) ([]byte, error) {
	mf.mu.RLock()
	defer mf.mu.RUnlock()

	if offset < 0 || offset >= mf.size {
		return nil, fmt.Errorf("offset %d out of range [0, %d)", offset, mf.size)
	}

	if offset+int64(length) > mf.size {
		length = int(mf.size - offset)
	}

	if length <= 0 {
		return []byte{}, nil
	}

	// Create a copy of the data to avoid issues with concurrent access
	result := make([]byte, length)
	copy(result, mf.data[offset:offset+int64(length)])

	return result, nil
}

// Write writes data to the memory-mapped file
func (mf *MemoryMappedFile) Write(offset int64, data []byte) error {
	if mf.readOnly {
		return fmt.Errorf("cannot write to read-only memory-mapped file")
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()

	if offset < 0 || offset >= mf.size {
		return fmt.Errorf("offset %d out of range [0, %d)", offset, mf.size)
	}

	if offset+int64(len(data)) > mf.size {
		return fmt.Errorf("write would exceed file size: offset %d + len %d > size %d",
			offset, len(data), mf.size)
	}

	// Write directly to memory-mapped region
	copy(mf.data[offset:], data)

	return nil
}

// Sync synchronizes the memory-mapped file to disk
func (mf *MemoryMappedFile) Sync() error {
	if mf.readOnly {
		return nil // No need to sync read-only files
	}

	mf.mu.RLock()
	defer mf.mu.RUnlock()

	// Use msync to synchronize memory-mapped region to disk
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&mf.data[0])),
		uintptr(len(mf.data)),
		syscall.MS_SYNC)

	if errno != 0 {
		return fmt.Errorf("msync failed: %v", errno)
	}

	return nil
}

// Resize resizes the memory-mapped file
func (mf *MemoryMappedFile) Resize(newSize int64) error {
	if mf.readOnly {
		return fmt.Errorf("cannot resize read-only memory-mapped file")
	}

	mf.mu.Lock()
	defer mf.mu.Unlock()

	if newSize <= mf.size {
		return fmt.Errorf("new size %d must be greater than current size %d", newSize, mf.size)
	}

	// Unmap current memory
	err := syscall.Munmap(mf.data)
	if err != nil {
		return fmt.Errorf("failed to unmap memory during resize: %w", err)
	}

	// Resize file
	err = mf.file.Truncate(newSize)
	if err != nil {
		return fmt.Errorf("failed to truncate file to size %d: %w", newSize, err)
	}

	// Remap with new size
	newData, err := syscall.Mmap(int(mf.file.Fd()), 0, int(newSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to remap memory with new size: %w", err)
	}

	mf.data = newData
	mf.size = newSize

	return nil
}

// Size returns the current size of the memory-mapped file
func (mf *MemoryMappedFile) Size() int64 {
	mf.mu.RLock()
	defer mf.mu.RUnlock()
	return mf.size
}

// Path returns the file path
func (mf *MemoryMappedFile) Path() string {
	return mf.path
}

// CollectionName returns the collection name (without path and extension)
func (mf *MemoryMappedFile) CollectionName() string {
	// Extract collection name from path
	base := filepath.Base(mf.path)
	ext := filepath.Ext(base)
	return base[:len(base)-len(ext)]
}

// IsReadOnly returns whether the file is read-only
func (mf *MemoryMappedFile) IsReadOnly() bool {
	return mf.readOnly
}
