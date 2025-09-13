package api

import (
	"encoding/json"
	"log"
	"net/http"
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// HandleHealth handles GET requests to the health check endpoint
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	log.Printf("INFO: Health check endpoint called")
	response := HealthResponse{
		Status:  "healthy",
		Message: "go-db is running",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
