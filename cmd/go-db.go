package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adfharrison1/go-db/pkg/server"
	"github.com/adfharrison1/go-db/pkg/storage"
)

func main() {
	// Command line flags
	var (
		port      = flag.String("port", "8080", "Server port")
		dataFile  = flag.String("data-file", "go-db_data.godb", "Data file path for persistence")
		dataDir   = flag.String("data-dir", ".", "Data directory for storage")
		maxMemory = flag.Int("max-memory", 1024, "Maximum memory usage in MB")
		noSaves   = flag.Bool("no-saves", false, "Disable automatic disk writes (only save on shutdown)")
		showHelp  = flag.Bool("help", false, "Show help message")
	)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\ngo-db is an in-memory document database with optional persistence.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s                                    # Start with defaults (dual-write mode)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -port 9090 -max-memory 2048       # Custom port and memory\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -no-saves                          # Disable automatic disk writes (shutdown only)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -data-dir /tmp/go-db              # Custom data directory\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nPersistence Options:\n")
		fmt.Fprintf(os.Stderr, "  Dual-write mode: Data saved to memory and disk immediately (default)\n")
		fmt.Fprintf(os.Stderr, "  No-saves mode: Data only saved on graceful shutdown (maximum performance)\n")
	}

	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Build storage options based on flags
	var storageOptions []storage.StorageOption

	// Set data directory
	if *dataDir != "." {
		storageOptions = append(storageOptions, storage.WithDataDir(*dataDir))
		log.Printf("INFO: Using data directory: %s", *dataDir)
	}

	// Set max memory
	if *maxMemory != 1024 {
		storageOptions = append(storageOptions, storage.WithMaxMemory(*maxMemory))
		log.Printf("INFO: Max memory set to: %d MB", *maxMemory)
	}

	// Set no-saves option
	if *noSaves {
		storageOptions = append(storageOptions, storage.WithNoSaves(true))
		log.Printf("INFO: No-saves mode enabled - data only saved on shutdown")
	} else {
		log.Printf("INFO: Dual-write mode enabled - data saved to memory and disk immediately")
	}

	// Create a new server with storage options
	srv := server.NewServer(storageOptions...)
	defer srv.StopBackgroundWorkers() // Ensure cleanup

	// Initialize database from file
	log.Printf("INFO: Loading data from: %s", *dataFile)
	srv.InitDB(*dataFile)

	// Create HTTP server
	httpServer := &http.Server{
		Addr:    ":" + *port,
		Handler: srv.Router(),
	}

	// Start server in a goroutine
	go func() {
		log.Printf("Starting go-db server on :%s", *port)
		log.Printf("API endpoints available at http://localhost:%s", *port)
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
	log.Printf("INFO: Saving data to: %s", *dataFile)
	srv.SaveDB(*dataFile)

	// Give outstanding requests a deadline for completion
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exited")
}
