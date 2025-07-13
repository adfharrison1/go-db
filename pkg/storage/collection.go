package storage

import (
	"time"

	"github.com/adfharrison1/go-db/pkg/data"
)

type CollectionState int

const (
	CollectionStateUnloaded CollectionState = iota
	CollectionStateLoading
	CollectionStateLoaded
	CollectionStateDirty
)

type CollectionInfo struct {
	Name          string
	DocumentCount int64
	SizeOnDisk    int64
	LastModified  time.Time
	State         CollectionState
	AccessCount   int64
	LastAccessed  time.Time
}

// Collection wraps data.Collection for storage-specific functionality
type Collection = data.Collection

// Document wraps data.Document for storage-specific functionality
type Document = data.Document

// NewCollection creates a new collection
func NewCollection(name string) *Collection {
	return data.NewCollection(name)
}
