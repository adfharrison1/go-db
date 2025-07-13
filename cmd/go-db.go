package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adfharrison1/go-db/pkg/server"
)

func main() {
	// Create a new server
	srv := server.NewServer()

	// (Optional) Start your database initialization or load from disk
	srv.InitDB("go-db_data.godb")

	// Create HTTP server
	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: srv.Router(),
	}

	// Start server in a goroutine
	go func() {
		log.Println("Starting go-db server on :8080")
		log.Println("go-db server running on :8080")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// Save database before shutdown
	srv.SaveDB("go-db_data.godb")

	// Give outstanding requests a deadline for completion
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exited")
}
