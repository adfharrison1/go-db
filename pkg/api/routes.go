package api

import (
	"github.com/gorilla/mux"
)

// RegisterRoutes registers all API routes with the given router
func (h *Handler) RegisterRoutes(router *mux.Router) {
	// Collection operations
	router.HandleFunc("/collections/{coll}/insert", h.HandleInsert).Methods("POST")
	router.HandleFunc("/collections/{coll}/stream", h.HandleStream).Methods("GET")

	// Document operations (by ID)
	router.HandleFunc("/collections/{coll}/documents/{id}", h.HandleGetById).Methods("GET")
	router.HandleFunc("/collections/{coll}/documents/{id}", h.HandleUpdateById).Methods("PUT")
	router.HandleFunc("/collections/{coll}/documents/{id}", h.HandleDeleteById).Methods("DELETE")

	// Find with optional filtering (query parameters)
	router.HandleFunc("/collections/{coll}/find", h.HandleFindWithFilter).Methods("GET")

	// Add more routes as needed
}
