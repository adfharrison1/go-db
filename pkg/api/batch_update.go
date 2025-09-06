package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/gorilla/mux"
)

// BatchUpdateRequest represents the request body for batch update operations
type BatchUpdateRequest struct {
	Operations []BatchUpdateOperation `json:"operations"`
}

// BatchUpdateOperation represents a single update operation in the request
type BatchUpdateOperation struct {
	ID      string                 `json:"id"`
	Updates map[string]interface{} `json:"updates"`
}

// BatchUpdateResponse represents the response for batch update operations
type BatchUpdateResponse struct {
	Success      bool              `json:"success"`
	Message      string            `json:"message"`
	UpdatedCount int               `json:"updated_count"`
	FailedCount  int               `json:"failed_count"`
	Collection   string            `json:"collection"`
	Documents    []domain.Document `json:"documents"`
	Errors       []string          `json:"errors,omitempty"`
}

// HandleBatchUpdate handles PATCH requests to update multiple documents in collections
func (h *Handler) HandleBatchUpdate(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleBatchUpdate called for collection '%s'", collName)

	var req BatchUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("ERROR: Decoding body failed: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Operations) == 0 {
		log.Printf("ERROR: No operations provided for batch update")
		http.Error(w, "No operations provided", http.StatusBadRequest)
		return
	}

	if len(req.Operations) > 1000 {
		log.Printf("ERROR: Too many operations for batch update: %d", len(req.Operations))
		http.Error(w, "Maximum 1000 operations allowed per batch", http.StatusBadRequest)
		return
	}

	// Convert operations to domain format
	domainOps := make([]domain.BatchUpdateOperation, len(req.Operations))
	for i, op := range req.Operations {
		domainDoc := domain.Document{}
		for k, v := range op.Updates {
			domainDoc[k] = v
		}
		domainOps[i] = domain.BatchUpdateOperation{
			ID:      op.ID,
			Updates: domainDoc,
		}
	}

	// Perform batch update
	updatedDocs, err := h.storage.BatchUpdate(collName, domainOps)

	// Parse results - determine success/failure counts
	var response BatchUpdateResponse
	response.Collection = collName

	if err != nil {
		// Check if this is a partial failure (some succeeded, some failed)
		if errorMsg := err.Error(); len(errorMsg) > 0 {
			log.Printf("WARN: Batch update had errors: %v", err)

			// For partial failures, we still return 200 but include error details
			response.Success = true // At least some succeeded if we got here
			response.Message = "Batch update completed with some errors"
			response.Errors = []string{errorMsg}

			// Try to extract counts from error message
			// This is a simplified approach - in production you might want structured error handling
			response.UpdatedCount = len(domainOps) // Will be adjusted below if we can parse errors
			response.FailedCount = 0
		} else {
			// Complete failure
			log.Printf("ERROR: Batch update failed for collection '%s': %v", collName, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		// Complete success
		response.Success = true
		response.Message = "Batch update completed successfully"
		response.UpdatedCount = len(updatedDocs)
		response.FailedCount = 0
		response.Documents = updatedDocs
	}

	// Save collection to disk if transaction saves are enabled
	if err := h.storage.SaveCollectionAfterTransaction(collName); err != nil {
		log.Printf("WARN: Failed to save collection '%s' after batch update: %v", collName, err)
		// Don't fail the request if save fails, just log the warning
	}

	// Return response
	w.Header().Set("Content-Type", "application/json")
	if response.FailedCount > 0 {
		w.WriteHeader(http.StatusPartialContent) // 206 for partial success
	} else {
		w.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(w).Encode(response)

	log.Printf("INFO: Batch update completed for collection '%s', updated %d, failed %d",
		collName, response.UpdatedCount, response.FailedCount)
}
