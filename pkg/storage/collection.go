package storage

import (
	"time"

	"github.com/adfharrison1/go-db/pkg/domain"
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

// Collection wraps domain.Collection for storage-specific functionality
type Collection = domain.Collection

// Document wraps domain.Document for storage-specific functionality
type Document = domain.Document

// NewCollection creates a new collection
func NewCollection(name string) *Collection {
	return domain.NewCollection(name)
}
