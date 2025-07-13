package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

// HandleStream handles GET requests to stream documents from collections
func (h *Handler) HandleStream(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleStream called for collection '%s'", collName)

	// Set headers for streaming
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Get document stream from storage engine
	docChan, err := h.storage.FindAllStream(collName)
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

	log.Printf("INFO: Streamed %d documents from collection '%s'", docCount, collName)
}
