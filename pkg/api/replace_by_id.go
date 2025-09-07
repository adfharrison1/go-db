package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleReplaceById handles PUT requests to completely replace a document by ID
// This performs an absolute update, replacing the entire document content
func (h *Handler) HandleReplaceById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]
	docId := vars["id"]

	log.Printf("INFO: handleReplaceById called for collection '%s', document '%s'", collName, docId)

	if collName == "" || docId == "" {
		WriteJSONError(w, http.StatusBadRequest, "collection name and document ID are required")
		return
	}

	// Parse the new document from request body
	var newDoc map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&newDoc); err != nil {
		log.Printf("ERROR: Decoding body failed: %v", err)
		WriteJSONError(w, http.StatusBadRequest, "invalid JSON in request body")
		return
	}

	// Replace the document completely
	replacedDoc, err := h.storage.ReplaceById(collName, docId, newDoc)
	if err != nil {
		log.Printf("ERROR: Replace failed for document '%s' in collection '%s': %v", docId, collName, err)
		WriteJSONError(w, http.StatusNotFound, err.Error())
		return
	}

	// Save collection to disk if transaction saves are enabled
	if err := h.storage.SaveCollectionAfterTransaction(collName); err != nil {
		log.Printf("WARN: Failed to save collection '%s' after replace: %v", collName, err)
		// Don't fail the request if save fails, just log the warning
	}

	log.Printf("INFO: Replaced document '%s' in collection '%s'", docId, collName)

	// Return the replaced document
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(replacedDoc)
}
