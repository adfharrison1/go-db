package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/gorilla/mux"
)

// HandleFindAll handles GET requests to find documents with filter criteria and pagination
func (h *Handler) HandleFindAll(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleFindAll called for collection '%s'", collName)

	// Parse query parameters to build filter
	filter := make(map[string]interface{})
	queryParams := r.URL.Query()

	// Extract pagination parameters
	paginationOptions := domain.DefaultPaginationOptions()

	// Parse limit
	if limitStr := queryParams.Get("limit"); limitStr != "" {
		if limit, err := strconv.Atoi(limitStr); err == nil {
			paginationOptions.Limit = limit
		}
	}

	// Parse offset
	if offsetStr := queryParams.Get("offset"); offsetStr != "" {
		if offset, err := strconv.Atoi(offsetStr); err == nil {
			paginationOptions.Offset = offset
		}
	}

	// Parse cursor-based pagination
	if after := queryParams.Get("after"); after != "" {
		paginationOptions.After = after
	}

	if before := queryParams.Get("before"); before != "" {
		paginationOptions.Before = before
	}

	// Build filter from remaining query parameters
	for key, values := range queryParams {
		// Skip pagination parameters
		if key == "limit" || key == "offset" || key == "after" || key == "before" {
			continue
		}

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

	// Always use paginated version
	result, err := h.storage.FindAll(collName, filter, paginationOptions)
	if err != nil {
		log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	log.Printf("INFO: Found %d documents in collection '%s' with pagination (total: %d)",
		len(result.Documents), collName, result.Total)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
