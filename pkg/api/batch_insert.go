package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/gorilla/mux"
)

// BatchInsertRequest represents the request body for batch insert operations
type BatchInsertRequest struct {
	Documents []map[string]interface{} `json:"documents"`
}

// BatchInsertResponse represents the response for batch insert operations
type BatchInsertResponse struct {
	Success       bool   `json:"success"`
	Message       string `json:"message"`
	InsertedCount int    `json:"inserted_count"`
	Collection    string `json:"collection"`
}

// HandleBatchInsert handles POST requests to insert multiple documents into collections
func (h *Handler) HandleBatchInsert(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleBatchInsert called for collection '%s'", collName)

	var req BatchInsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("ERROR: Decoding body failed: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate request
	if len(req.Documents) == 0 {
		log.Printf("ERROR: No documents provided for batch insert")
		http.Error(w, "No documents provided", http.StatusBadRequest)
		return
	}

	if len(req.Documents) > 1000 {
		log.Printf("ERROR: Too many documents for batch insert: %d", len(req.Documents))
		http.Error(w, "Maximum 1000 documents allowed per batch", http.StatusBadRequest)
		return
	}

	// Convert documents to domain.Document format
	docs := make([]domain.Document, len(req.Documents))
	for i, doc := range req.Documents {
		domainDoc := domain.Document{}
		for k, v := range doc {
			domainDoc[k] = v
		}
		docs[i] = domainDoc
	}

	// Perform batch insert
	if err := h.storage.BatchInsert(collName, docs); err != nil {
		log.Printf("ERROR: Batch insert failed for collection '%s': %v", collName, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Save collection to disk if transaction saves are enabled
	if err := h.storage.SaveCollectionAfterTransaction(collName); err != nil {
		log.Printf("WARN: Failed to save collection '%s' after batch insert: %v", collName, err)
		// Don't fail the request if save fails, just log the warning
	}

	// Return success response
	response := BatchInsertResponse{
		Success:       true,
		Message:       "Batch insert completed successfully",
		InsertedCount: len(docs),
		Collection:    collName,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)

	log.Printf("INFO: Batch insert successful for collection '%s', inserted %d documents", collName, len(docs))
}
