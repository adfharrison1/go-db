package api

import (
	"github.com/adfharrison1/go-db/pkg/domain"
)

// Handler provides HTTP handlers for the database API
type Handler struct {
	storage domain.StorageEngine
	indexer domain.IndexEngine
}

// NewHandler creates a new API handler with dependency injection
func NewHandler(storage domain.StorageEngine, indexer domain.IndexEngine) *Handler {
	return &Handler{
		storage: storage,
		indexer: indexer,
	}
}
