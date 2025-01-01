package main

import (
	"log"
	"net/http"

	"github.com/adfharrison1/go-db/pkg/server"
)

func main() {
    // Create a new server
    srv := server.NewServer()

    // (Optional) Start your database initialization or load from disk
    srv.InitDB("go-db_data.json")

    // Start listening for connections
    log.Println("Starting go-db server on :8080")
    if err := http.ListenAndServe(":8080", srv.Router()); err != nil {
        log.Fatalf("Server failed to start: %v", err)
    }
}
