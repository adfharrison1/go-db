package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// HandleFindAll handles GET requests to find documents with filter criteria
func (h *Handler) HandleFindAll(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleFindAll called for collection '%s'", collName)

	// Parse query parameters to build filter
	filter := make(map[string]interface{})
	queryParams := r.URL.Query()

	for key, values := range queryParams {
		if len(values) > 0 {
			value := values[0] // Take first value if multiple provided

			// Try to convert to number if possible
			if num, err := strconv.ParseFloat(value, 64); err == nil {
				filter[key] = num
			} else if num, err := strconv.ParseInt(value, 10, 64); err == nil {
				filter[key] = num
			} else {
				// Treat as string
				filter[key] = value
			}
		}
	}

	// Use the unified FindAll method with filter
	docs, err := h.storage.FindAll(collName, filter)
	if err != nil {
		log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if len(filter) == 0 {
		log.Printf("INFO: Found %d documents in collection '%s' (no filter)", len(docs), collName)
	} else {
		log.Printf("INFO: Found %d documents in collection '%s' with filter %v", len(docs), collName, filter)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(docs)
}
