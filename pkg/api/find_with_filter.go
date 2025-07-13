package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// HandleFindWithFilter handles GET requests to find documents with filter criteria
func (h *Handler) HandleFindWithFilter(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleFindWithFilter called for collection '%s'", collName)

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

	// If no filters provided, return all documents
	if len(filter) == 0 {
		docs, err := h.storage.FindAll(collName)
		if err != nil {
			log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		log.Printf("INFO: Found %d documents in collection '%s' (no filter)", len(docs), collName)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(docs)
		return
	}

	// Apply filters
	docs, err := h.storage.FindAllWithFilter(collName, filter)
	if err != nil {
		log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	log.Printf("INFO: Found %d documents in collection '%s' with filter %v", len(docs), collName, filter)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(docs)
}
