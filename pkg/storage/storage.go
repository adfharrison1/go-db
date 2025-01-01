package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/adfharrison1/go-db/pkg/data"
)

type StorageEngine struct {
    mu          sync.RWMutex
    collections map[string]*data.Collection
}

// NewStorageEngine creates a new storage engine.
func NewStorageEngine() *StorageEngine {
    return &StorageEngine{
        collections: make(map[string]*data.Collection),
    }
}

// LoadFromFile loads collections from a JSON file.
func (se *StorageEngine) LoadFromFile(filename string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    decoder := json.NewDecoder(file)
    var raw map[string]map[string]data.Document
    if err := decoder.Decode(&raw); err != nil {
        return err
    }

    se.mu.Lock()
    defer se.mu.Unlock()

    for collName, docs := range raw {
        coll := data.NewCollection(collName)
        for docID, docData := range docs {
            coll.Documents[docID] = docData
        }
        se.collections[collName] = coll
    }

    return nil
}

// SaveToFile saves collections to a JSON file.
func (se *StorageEngine) SaveToFile(filename string) error {
    se.mu.RLock()
    defer se.mu.RUnlock()

    // Prepare raw data
    raw := make(map[string]map[string]data.Document)
    for collName, coll := range se.collections {
        raw[collName] = coll.Documents
    }

    out, err := json.MarshalIndent(raw, "", "  ")
    if err != nil {
        return err
    }

    return os.WriteFile(filename, out, 0644)
}

// Insert inserts a document into a collection, creating the collection if needed.
func (se *StorageEngine) Insert(collName string, doc data.Document) error {
    se.mu.Lock()
    defer se.mu.Unlock()

    coll, exists := se.collections[collName]
    if !exists {
        coll = data.NewCollection(collName)
        se.collections[collName] = coll
    }

    // Generate a unique ID (for simplicity, using length + 1)
    newID := fmt.Sprintf("%d", len(coll.Documents)+1)
    doc["_id"] = newID
    coll.Documents[newID] = doc

    return nil
}

// FindAll returns all documents in a collection.
func (se *StorageEngine) FindAll(collName string) ([]data.Document, error) {
    se.mu.RLock()
    defer se.mu.RUnlock()

    coll, exists := se.collections[collName]
    if !exists {
        return nil, fmt.Errorf("collection %s does not exist", collName)
    }

    results := make([]data.Document, 0, len(coll.Documents))
    for _, d := range coll.Documents {
        results = append(results, d)
    }
    return results, nil
}
