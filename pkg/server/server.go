package server

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/adfharrison1/go-db/pkg/data"
	"github.com/adfharrison1/go-db/pkg/storage"
)

// Server holds references to storage, router, etc.
type Server struct {
	router   *mux.Router
	dbEngine *storage.StorageEngine
	mu       sync.RWMutex
}

// NewServer creates a new instance of Server.
func NewServer() *Server {
	s := &Server{
		router:   mux.NewRouter(),
		dbEngine: storage.NewStorageEngine(),
	}
	// Define HTTP routes
	s.routes()

	// Use the logging middleware for all routes
	s.router.Use(requestLoggerMiddleware)

	// Customize NotFoundHandler to log 404s
	s.router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("WARN: No route found for %s %s", r.Method, r.URL.Path)
		http.NotFound(w, r)
	})

	return s
}

// requestLoggerMiddleware logs the method, URL path, and duration for each request.
func requestLoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		elapsed := time.Since(start)
		log.Printf("INFO: Request %s %s took %s", r.Method, r.URL.Path, elapsed)
	})
}

// InitDB optionally load data from a file, or do any initialization steps.
func (s *Server) InitDB(filename string) {
	if err := s.dbEngine.LoadCollectionMetadata(filename); err != nil {
		log.Printf("ERROR: Could not load DB metadata from file %s: %v", filename, err)
	} else {
		log.Printf("INFO: Loaded DB metadata from file %s successfully", filename)
	}
}

// SaveDB saves the current database state to file
func (s *Server) SaveDB(filename string) {
	if err := s.dbEngine.SaveToFile(filename); err != nil {
		log.Printf("ERROR: Could not save DB to file %s: %v", filename, err)
	} else {
		log.Printf("INFO: Saved DB to file %s successfully", filename)
	}
}

// Router exposes the internal mux.Router.
func (s *Server) Router() http.Handler {
	return s.router
}

// routes defines all REST endpoints.
func (s *Server) routes() {
	s.router.HandleFunc("/collections/{coll}/insert", s.handleInsert).Methods("POST")
	s.router.HandleFunc("/collections/{coll}/find", s.handleFind).Methods("GET")
	// Add more routes as needed
}

// handleInsert is a simple example to insert a new document.
func (s *Server) handleInsert(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleInsert called for collection '%s'", collName)

	var doc data.Document
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		log.Printf("ERROR: Decoding body failed: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.dbEngine.Insert(collName, doc); err != nil {
		log.Printf("ERROR: Insert failed for collection '%s': %v", collName, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("INFO: Insert successful for collection '%s'", collName)
	w.WriteHeader(http.StatusCreated)
}

// handleFind is a simplistic find (returns all documents for now).
func (s *Server) handleFind(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collName := vars["coll"]

	log.Printf("INFO: handleFind called for collection '%s'", collName)

	docs, err := s.dbEngine.FindAll(collName)
	if err != nil {
		log.Printf("ERROR: Collection '%s' not found: %v", collName, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	log.Printf("INFO: Found %d documents in collection '%s'", len(docs), collName)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(docs)
}
