package api

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleCreateIndex creates an index on a specific field in a collection
func (h *Handler) HandleCreateIndex(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]
	fieldName := vars["field"]

	if fieldName == "" {
		http.Error(w, "field name is required", http.StatusBadRequest)
		return
	}

	// Prevent creating index on _id (it's automatically created)
	if fieldName == "_id" {
		http.Error(w, "cannot create index on _id field (automatically indexed)", http.StatusBadRequest)
		return
	}

	err := h.indexer.CreateIndex(collName, fieldName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"success":    true,
		"message":    "Index created successfully",
		"collection": collName,
		"field":      fieldName,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}
