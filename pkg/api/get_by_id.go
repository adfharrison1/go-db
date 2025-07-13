package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleGetById handles GET requests to retrieve a specific document by ID
func (h *Handler) HandleGetById(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]
	docId := vars["id"]

	log.Printf("INFO: handleGetById called for collection '%s', document '%s'", collName, docId)

	doc, err := h.storage.GetById(collName, docId)
	if err != nil {
		log.Printf("ERROR: Document '%s' not found in collection '%s': %v", docId, collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	log.Printf("INFO: Retrieved document '%s' from collection '%s'", docId, collName)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc)
}
