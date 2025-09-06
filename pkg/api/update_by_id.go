package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/gorilla/mux"
)

// HandleUpdateById handles PUT requests to update a specific document by ID
func (h *Handler) HandleUpdateById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]
	docId := vars["id"]

	log.Printf("INFO: handleUpdateById called for collection '%s', document '%s'", collName, docId)

	var updates map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		log.Printf("ERROR: Decoding body failed: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Convert map to domain.Document
	updateDoc := domain.Document{}
	for k, v := range updates {
		updateDoc[k] = v
	}

	if err := h.storage.UpdateById(collName, docId, updateDoc); err != nil {
		log.Printf("ERROR: Update failed for document '%s' in collection '%s': %v", docId, collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Save collection to disk if transaction saves are enabled
	if err := h.storage.SaveCollectionAfterTransaction(collName); err != nil {
		log.Printf("WARN: Failed to save collection '%s' after update: %v", collName, err)
		// Don't fail the request if save fails, just log the warning
	}

	log.Printf("INFO: Updated document '%s' in collection '%s'", docId, collName)
	w.WriteHeader(http.StatusOK)
}
