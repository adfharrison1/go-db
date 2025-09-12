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
		mode      = flag.String("mode", "dual-write", "Operation mode: dual-write, no-saves, memory-map")
		noSaves   = flag.Bool("no-saves", false, "Disable automatic disk writes (only save on shutdown) [deprecated: use -mode]")
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
		fmt.Fprintf(os.Stderr, "  %s -mode no-saves                     # No-saves mode (maximum performance)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode memory-map                   # Memory-mapped mode (optimal for large datasets)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -data-dir /tmp/go-db              # Custom data directory\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nOperation Modes:\n")
		fmt.Fprintf(os.Stderr, "  dual-write: Data saved to memory and disk immediately (default, maximum safety)\n")
		fmt.Fprintf(os.Stderr, "  no-saves: Data only saved on graceful shutdown (maximum performance)\n")
		fmt.Fprintf(os.Stderr, "  memory-map: Memory-mapped files for optimal performance with large datasets\n")
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

	// Parse operation mode
	var operationMode storage.OperationMode
	switch *mode {
	case "dual-write":
		operationMode = storage.ModeDualWrite
	case "no-saves":
		operationMode = storage.ModeNoSaves
	case "memory-map":
		operationMode = storage.ModeMemoryMap
	default:
		log.Fatalf("ERROR: Invalid operation mode '%s'. Valid modes: dual-write, no-saves, memory-map", *mode)
	}

	// Handle deprecated -no-saves flag
	if *noSaves && *mode == "dual-write" {
		operationMode = storage.ModeNoSaves
		log.Printf("WARNING: -no-saves flag is deprecated, use -mode no-saves instead")
	}

	// Set operation mode
	storageOptions = append(storageOptions, storage.WithOperationMode(operationMode))
	log.Printf("INFO: Operation mode: %s", operationMode.String())

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
