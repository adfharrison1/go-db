package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

// HandleFindAllWithStream handles GET requests to stream documents from collections
// NOTE: This endpoint does NOT apply pagination - it streams ALL matching documents.
// Use /collections/{coll}/find for paginated queries, or handle pagination at the client level.
func (h *Handler) HandleFindAllWithStream(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleFindAllWithStream called for collection '%s'", collName)

	// Set headers for streaming
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Parse query parameters for filtering only (pagination is ignored)
	filter := make(map[string]interface{})
	queryParams := r.URL.Query()

	// Build filter from query parameters (ignore pagination parameters)
	for key, values := range queryParams {
		// Skip pagination parameters - they are ignored in streaming
		if key == "limit" || key == "offset" || key == "after" || key == "before" {
			log.Printf("WARN: Pagination parameter '%s' ignored in streaming endpoint", key)
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

	// Stream all matching documents (no pagination)
	docChan, err := h.storage.FindAllStream(collName, filter)
	if err != nil {
		log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// Start JSON array
	w.Write([]byte("[\n"))

	first := true
	docCount := 0

	// Stream documents one by one
	for doc := range docChan {
		if !first {
			w.Write([]byte(",\n"))
		}
		first = false

		// Marshal document to JSON
		docJSON, err := json.Marshal(doc)
		if err != nil {
			log.Printf("ERROR: Failed to marshal document: %v", err)
			continue // Skip this document and continue streaming
		}

		// Write document to response
		if _, err := w.Write(docJSON); err != nil {
			log.Printf("ERROR: Failed to write to response: %v", err)
			return
		}

		// Flush the response to ensure streaming
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}

		docCount++
	}

	// End JSON array
	w.Write([]byte("\n]"))

	log.Printf("INFO: Streamed %d documents from collection '%s' (no pagination applied)", docCount, collName)
}
