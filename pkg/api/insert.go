package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/gorilla/mux"
)

// HandleInsert handles POST requests to insert documents into collections
func (h *Handler) HandleInsert(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleInsert called for collection '%s'", collName)

	var doc map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		log.Printf("ERROR: Decoding body failed: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Convert map to domain.Document
	document := domain.Document{}
	for k, v := range doc {
		document[k] = v
	}

	if err := h.storage.Insert(collName, document); err != nil {
		log.Printf("ERROR: Insert failed for collection '%s': %v", collName, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Save collection to disk if transaction saves are enabled
	if err := h.storage.SaveCollectionAfterTransaction(collName); err != nil {
		log.Printf("WARN: Failed to save collection '%s' after insert: %v", collName, err)
		// Don't fail the request if save fails, just log the warning
	}

	log.Printf("INFO: Insert successful for collection '%s'", collName)
	w.WriteHeader(http.StatusCreated)
}
