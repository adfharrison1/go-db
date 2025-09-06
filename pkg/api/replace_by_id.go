package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleReplaceById handles PUT requests to completely replace a document by ID
// This performs an absolute update, replacing the entire document content
func (h *Handler) HandleReplaceById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]
	docId := vars["id"]

	if collName == "" || docId == "" {
		http.Error(w, "collection name and document ID are required", http.StatusBadRequest)
		return
	}

	// Parse the new document from request body
	var newDoc map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&newDoc); err != nil {
		http.Error(w, "invalid JSON in request body", http.StatusBadRequest)
		return
	}

	// Replace the document completely
	replacedDoc, err := h.storage.ReplaceById(collName, docId, newDoc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Return the replaced document
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(replacedDoc)
}
