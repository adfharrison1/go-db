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
		port           = flag.String("port", "8080", "Server port")
		dataFile       = flag.String("data-file", "go-db_data.godb", "Data file path for persistence")
		dataDir        = flag.String("data-dir", ".", "Data directory for storage")
		maxMemory      = flag.Int("max-memory", 1024, "Maximum memory usage in MB")
		backgroundSave = flag.Duration("background-save", 0, "Background save interval (e.g., 5m, 30s). Set to 0 to disable.")
		showHelp       = flag.Bool("help", false, "Show help message")
	)

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\ngo-db is an in-memory document database with optional persistence.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s                                    # Start with defaults\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -port 9090 -max-memory 2048       # Custom port and memory\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -background-save 5m               # Auto-save every 5 minutes\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -data-dir /tmp/go-db              # Custom data directory\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nSafety Note:\n")
		fmt.Fprintf(os.Stderr, "  Without -background-save, data is only saved on graceful shutdown.\n")
		fmt.Fprintf(os.Stderr, "  Enable background saves for better data safety in production.\n")
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

	// Set background save if specified
	if *backgroundSave > 0 {
		storageOptions = append(storageOptions, storage.WithBackgroundSave(*backgroundSave))
		log.Printf("INFO: Background save enabled: every %v", *backgroundSave)
	} else {
		log.Printf("WARN: Background save disabled - data only saved on graceful shutdown")
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
