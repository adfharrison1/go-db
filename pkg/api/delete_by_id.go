package api

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleDeleteById handles DELETE requests to remove a specific document by ID
func (h *Handler) HandleDeleteById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]
	docId := vars["id"]

	log.Printf("INFO: handleDeleteById called for collection '%s', document '%s'", collName, docId)

	if err := h.storage.DeleteById(collName, docId); err != nil {
		log.Printf("ERROR: Delete failed for document '%s' in collection '%s': %v", docId, collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Save collection to disk if transaction saves are enabled
	if err := h.storage.SaveCollectionAfterTransaction(collName); err != nil {
		log.Printf("WARN: Failed to save collection '%s' after delete: %v", collName, err)
		// Don't fail the request if save fails, just log the warning
	}

	log.Printf("INFO: Deleted document '%s' from collection '%s'", docId, collName)
	w.WriteHeader(http.StatusNoContent)
}
