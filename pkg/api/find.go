package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleFind handles GET requests to retrieve all documents from collections
func (h *Handler) HandleFind(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleFind called for collection '%s'", collName)

	docs, err := h.storage.FindAll(collName)
	if err != nil {
		log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	log.Printf("INFO: Found %d documents in collection '%s'", len(docs), collName)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(docs)
}
