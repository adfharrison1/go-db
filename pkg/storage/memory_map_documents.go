package storage

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// insertMemoryMap inserts a document using memory-mapped files
func (se *StorageEngine) insertMemoryMap(collection string, document domain.Document) (domain.Document, error) {
	// Get or create collection lock
	lock := se.getOrCreateCollectionLock(collection)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	// Ensure collection exists and create _id index
	se.mu.Lock()
	if se.collections[collection] == nil {
		se.collections[collection] = &CollectionInfo{
			State: CollectionStateLoaded,
		}
		// Create _id index for the collection
		se.indexEngine.CreateIndex(collection, "_id")
	}
	se.mu.Unlock()

	// Get or create memory-mapped file for collection
	mf, err := se.memoryMapManager.OpenCollection(collection, false)
	if err != nil {
		return nil, fmt.Errorf("failed to open memory-mapped file for collection %s: %w", collection, err)
	}

	// Generate ID if not provided
	if document["_id"] == nil {
		se.mu.Lock()
		if se.idCounters[collection] == nil {
			var counter int64 = 0
			se.idCounters[collection] = &counter
		}
		*se.idCounters[collection]++
		document["_id"] = strconv.FormatInt(*se.idCounters[collection], 10)
		se.mu.Unlock()
	}

	// Serialize document
	docBytes, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize document: %w", err)
	}

	// For memory-mapped files, we need to store documents in a structured way
	// We'll use a simple format: [length][data][length][data]...

	// Find the end of the file to append
	fileSize := mf.Size()

	// Calculate space needed: 8 bytes for length + document data
	neededSpace := int64(8 + len(docBytes))

	// Resize file if needed
	if fileSize+neededSpace > mf.Size() {
		newSize := fileSize + neededSpace + 4096 // Add some extra space
		err = mf.Resize(newSize)
		if err != nil {
			return nil, fmt.Errorf("failed to resize memory-mapped file: %w", err)
		}
	}

	// Write document length (8 bytes, little-endian)
	lengthBytes := make([]byte, 8)
	length := uint64(len(docBytes))
	for i := 0; i < 8; i++ {
		lengthBytes[i] = byte(length >> (i * 8))
	}

	err = mf.Write(fileSize, lengthBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to write document length: %w", err)
	}

	// Write document data
	err = mf.Write(fileSize+8, docBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to write document data: %w", err)
	}

	// Sync to disk
	err = mf.Sync()
	if err != nil {
		return nil, fmt.Errorf("failed to sync memory-mapped file: %w", err)
	}

	// Update in-memory cache for fast access
	se.mu.Lock()
	if se.collections[collection] == nil {
		se.collections[collection] = &CollectionInfo{
			State: CollectionStateLoaded,
		}
	}

	// Get or create collection in cache
	cachedCollection, _, found := se.cache.Get(collection)
	if !found {
		cachedCollection = domain.NewCollection(collection)
		se.cache.Put(collection, cachedCollection, se.collections[collection])
	}
	cachedCollection.Documents[document["_id"].(string)] = document
	se.mu.Unlock()

	return document, nil
}

// getByIdMemoryMap retrieves a document by ID using memory-mapped files
func (se *StorageEngine) getByIdMemoryMap(collection, docID string) (domain.Document, error) {
	// First check in-memory cache
	se.mu.RLock()
	cachedCollection, _, found := se.cache.Get(collection)
	if found {
		if doc, docExists := cachedCollection.Documents[docID]; docExists {
			se.mu.RUnlock()
			return doc, nil
		}
	}
	se.mu.RUnlock()

	// If not in cache, we need to scan the memory-mapped file
	// This is not efficient for large files, but it's a starting point
	lock := se.getOrCreateCollectionLock(collection)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	// Check if collection exists
	se.mu.RLock()
	_, exists := se.collections[collection]
	se.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("collection %s does not exist", collection)
	}

	mf, err := se.memoryMapManager.OpenCollection(collection, true)
	if err != nil {
		return nil, fmt.Errorf("failed to open memory-mapped file for collection %s: %w", collection, err)
	}

	// Scan through the file to find the document
	offset := int64(0)
	fileSize := mf.Size()

	for offset < fileSize {
		// Read document length
		lengthBytes, err := mf.Read(offset, 8)
		if err != nil {
			return nil, fmt.Errorf("failed to read document length at offset %d: %w", offset, err)
		}

		// Parse length
		var length uint64
		for i := 0; i < 8; i++ {
			length |= uint64(lengthBytes[i]) << (i * 8)
		}

		if length == 0 {
			break // End of documents
		}

		// Read document data
		docBytes, err := mf.Read(offset+8, int(length))
		if err != nil {
			return nil, fmt.Errorf("failed to read document data at offset %d: %w", offset, err)
		}

		// Parse document
		var doc domain.Document
		err = json.Unmarshal(docBytes, &doc)
		if err != nil {
			return nil, fmt.Errorf("failed to parse document at offset %d: %w", offset, err)
		}

		// Check if this is the document we're looking for
		if doc["_id"] == docID {
			// Cache the document for future access
			se.mu.Lock()
			if se.collections[collection] == nil {
				se.collections[collection] = &CollectionInfo{
					State: CollectionStateLoaded,
				}
			}

			// Get or create collection in cache
			cachedCollection, _, found := se.cache.Get(collection)
			if !found {
				cachedCollection = domain.NewCollection(collection)
				se.cache.Put(collection, cachedCollection, se.collections[collection])
			}
			cachedCollection.Documents[docID] = doc
			se.mu.Unlock()

			return doc, nil
		}

		// Move to next document
		offset += 8 + int64(length)
	}

	return nil, fmt.Errorf("document with id %s not found in collection %s", docID, collection)
}

// updateByIdMemoryMap updates a document by ID using memory-mapped files
func (se *StorageEngine) updateByIdMemoryMap(collection, docID string, updates domain.Document) (domain.Document, error) {
	// Get the existing document
	existingDoc, err := se.getByIdMemoryMap(collection, docID)
	if err != nil {
		return nil, err
	}

	// Apply updates
	for key, value := range updates {
		if key != "_id" { // Don't allow updating the ID
			existingDoc[key] = value
		}
	}

	// For memory-mapped files, we need to rewrite the entire file
	// This is not efficient for large files, but it's a starting point
	lock := se.getOrCreateCollectionLock(collection)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	// Get all documents in the collection
	se.mu.RLock()
	cachedCollection, _, found := se.cache.Get(collection)
	if !found {
		se.mu.RUnlock()
		return nil, fmt.Errorf("collection %s does not exist", collection)
	}

	// Create a copy of all documents
	allDocs := make(map[string]domain.Document)
	for id, doc := range cachedCollection.Documents {
		allDocs[id] = doc
	}
	se.mu.RUnlock()

	// Update the specific document
	allDocs[docID] = existingDoc

	// Rewrite the entire file
	err = se.rewriteMemoryMapFile(collection, allDocs)
	if err != nil {
		return nil, fmt.Errorf("failed to rewrite memory-mapped file: %w", err)
	}

	// Update cache
	se.mu.Lock()
	if cachedCollection, _, found := se.cache.Get(collection); found {
		cachedCollection.Documents[docID] = existingDoc
	}
	se.mu.Unlock()

	return existingDoc, nil
}

// replaceByIdMemoryMap replaces a document by ID using memory-mapped files
func (se *StorageEngine) replaceByIdMemoryMap(collection, docID string, newDoc domain.Document) (domain.Document, error) {
	// Set the ID
	newDoc["_id"] = docID

	// For memory-mapped files, we need to rewrite the entire file
	lock := se.getOrCreateCollectionLock(collection)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	// Get all documents in the collection
	se.mu.RLock()
	cachedCollection, _, found := se.cache.Get(collection)
	if !found {
		se.mu.RUnlock()
		return nil, fmt.Errorf("collection %s does not exist", collection)
	}

	// Create a copy of all documents
	allDocs := make(map[string]domain.Document)
	for id, doc := range cachedCollection.Documents {
		allDocs[id] = doc
	}
	se.mu.RUnlock()

	// Replace the specific document
	allDocs[docID] = newDoc

	// Rewrite the entire file
	err := se.rewriteMemoryMapFile(collection, allDocs)
	if err != nil {
		return nil, fmt.Errorf("failed to rewrite memory-mapped file: %w", err)
	}

	// Update cache
	se.mu.Lock()
	if cachedCollection, _, found := se.cache.Get(collection); found {
		cachedCollection.Documents[docID] = newDoc
	}
	se.mu.Unlock()

	return newDoc, nil
}

// deleteByIdMemoryMap deletes a document by ID using memory-mapped files
func (se *StorageEngine) deleteByIdMemoryMap(collection, docID string) error {
	// Check if document exists
	_, err := se.getByIdMemoryMap(collection, docID)
	if err != nil {
		return err
	}

	// For memory-mapped files, we need to rewrite the entire file
	lock := se.getOrCreateCollectionLock(collection)
	lock.mu.Lock()
	defer lock.mu.Unlock()

	// Get all documents in the collection
	se.mu.RLock()
	cachedCollection, _, found := se.cache.Get(collection)
	if !found {
		se.mu.RUnlock()
		return fmt.Errorf("collection %s does not exist", collection)
	}

	// Create a copy of all documents except the one to delete
	allDocs := make(map[string]domain.Document)
	for id, doc := range cachedCollection.Documents {
		if id != docID {
			allDocs[id] = doc
		}
	}
	se.mu.RUnlock()

	// Rewrite the entire file
	err = se.rewriteMemoryMapFile(collection, allDocs)
	if err != nil {
		return fmt.Errorf("failed to rewrite memory-mapped file: %w", err)
	}

	// Update cache
	se.mu.Lock()
	if cachedCollection, _, found := se.cache.Get(collection); found {
		delete(cachedCollection.Documents, docID)
	}
	se.mu.Unlock()

	return nil
}

// rewriteMemoryMapFile rewrites the entire memory-mapped file with the given documents
func (se *StorageEngine) rewriteMemoryMapFile(collection string, documents map[string]domain.Document) error {
	// Calculate total size needed
	var totalSize int64 = 8 // Start with 8 bytes for end marker
	for _, doc := range documents {
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to serialize document: %w", err)
		}
		totalSize += 8 + int64(len(docBytes)) // 8 bytes for length + document data
	}

	// Get memory-mapped file
	mf, err := se.memoryMapManager.OpenCollection(collection, false)
	if err != nil {
		return fmt.Errorf("failed to open memory-mapped file: %w", err)
	}

	// Resize file if needed
	if mf.Size() < totalSize {
		err = mf.Resize(totalSize)
		if err != nil {
			return fmt.Errorf("failed to resize memory-mapped file: %w", err)
		}
	}

	// Write all documents
	offset := int64(0)
	for _, doc := range documents {
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to serialize document: %w", err)
		}

		// Write document length
		lengthBytes := make([]byte, 8)
		length := uint64(len(docBytes))
		for i := 0; i < 8; i++ {
			lengthBytes[i] = byte(length >> (i * 8))
		}

		err = mf.Write(offset, lengthBytes)
		if err != nil {
			return fmt.Errorf("failed to write document length: %w", err)
		}
		offset += 8

		// Write document data
		err = mf.Write(offset, docBytes)
		if err != nil {
			return fmt.Errorf("failed to write document data: %w", err)
		}
		offset += int64(len(docBytes))
	}

	// Write end marker (length = 0)
	endMarker := make([]byte, 8)
	err = mf.Write(offset, endMarker)
	if err != nil {
		return fmt.Errorf("failed to write end marker: %w", err)
	}

	// Sync to disk
	err = mf.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync memory-mapped file: %w", err)
	}

	return nil
}

// loadCollectionFromMemoryMap loads all documents from a memory-mapped file into memory
func (se *StorageEngine) loadCollectionFromMemoryMap(collection string) error {
	lock := se.getOrCreateCollectionLock(collection)
	lock.mu.RLock()
	defer lock.mu.RUnlock()

	mf, err := se.memoryMapManager.OpenCollection(collection, true)
	if err != nil {
		return fmt.Errorf("failed to open memory-mapped file for collection %s: %w", collection, err)
	}

	// Initialize collection info
	se.mu.Lock()
	if se.collections[collection] == nil {
		se.collections[collection] = &CollectionInfo{
			State: CollectionStateLoaded,
		}
	}

	// Create collection in cache
	cachedCollection := domain.NewCollection(collection)
	se.cache.Put(collection, cachedCollection, se.collections[collection])
	se.mu.Unlock()

	// Scan through the file to load all documents
	offset := int64(0)
	fileSize := mf.Size()

	for offset < fileSize {
		// Read document length
		lengthBytes, err := mf.Read(offset, 8)
		if err != nil {
			return fmt.Errorf("failed to read document length at offset %d: %w", offset, err)
		}

		// Parse length
		var length uint64
		for i := 0; i < 8; i++ {
			length |= uint64(lengthBytes[i]) << (i * 8)
		}

		if length == 0 {
			break // End of documents
		}

		// Read document data
		docBytes, err := mf.Read(offset+8, int(length))
		if err != nil {
			return fmt.Errorf("failed to read document data at offset %d: %w", offset, err)
		}

		// Parse document
		var doc domain.Document
		err = json.Unmarshal(docBytes, &doc)
		if err != nil {
			return fmt.Errorf("failed to parse document at offset %d: %w", offset, err)
		}

		// Add to cache
		se.mu.Lock()
		cachedCollection, _, found := se.cache.Get(collection)
		if found {
			cachedCollection.Documents[doc["_id"].(string)] = doc
		}
		se.mu.Unlock()

		// Move to next document
		offset += 8 + int64(length)
	}

	return nil
}
