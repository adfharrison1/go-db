package api

import (
	"github.com/gorilla/mux"
)

// RegisterRoutes registers all API routes with the given router
func (h *Handler) RegisterRoutes(router *mux.Router) {
	// Collection operations
	router.HandleFunc("/collections/{coll}/insert", h.HandleInsert).Methods("POST")

	// Document operations (by ID)
	router.HandleFunc("/collections/{coll}/documents/{id}", h.HandleGetById).Methods("GET")
	router.HandleFunc("/collections/{coll}/documents/{id}", h.HandleUpdateById).Methods("PUT")
	router.HandleFunc("/collections/{coll}/documents/{id}", h.HandleDeleteById).Methods("DELETE")

	// Find with optional filtering (query parameters)
	router.HandleFunc("/collections/{coll}/find", h.HandleFindAll).Methods("GET")
	router.HandleFunc("/collections/{coll}/find_with_stream", h.HandleFindAllWithStream).Methods("GET")

	// Index operations
	router.HandleFunc("/collections/{coll}/indexes/{field}", h.HandleCreateIndex).Methods("POST")

	// Add more routes as needed
}
