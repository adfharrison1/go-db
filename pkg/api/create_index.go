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
		WriteJSONError(w, http.StatusBadRequest, "field name is required")
		return
	}

	// Prevent creating index on _id (it's automatically created)
	if fieldName == "_id" {
		WriteJSONError(w, http.StatusBadRequest, "cannot create index on _id field (automatically indexed)")
		return
	}

	err := h.storage.CreateIndex(collName, fieldName)
	if err != nil {
		WriteJSONError(w, http.StatusInternalServerError, err.Error())
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
