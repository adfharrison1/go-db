package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleGetIndexes handles GET requests to retrieve all indexes for a collection
func (h *Handler) HandleGetIndexes(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleGetIndexes called for collection '%s'", collName)

	// Get all indexes for the collection
	indexes, err := h.storage.GetIndexes(collName)
	if err != nil {
		log.Printf("ERROR: Failed to get indexes for collection '%s': %v", collName, err)
		WriteJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Prepare response
	response := map[string]interface{}{
		"success":     true,
		"collection":  collName,
		"indexes":     indexes,
		"index_count": len(indexes),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

	log.Printf("INFO: Retrieved %d indexes for collection '%s'", len(indexes), collName)
}
