package server

import (
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/adfharrison1/go-db/pkg/api"
	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/adfharrison1/go-db/pkg/storage"
)

// Server holds references to storage, router, etc.
type Server struct {
	router   *mux.Router
	dbEngine domain.StorageEngine
	api      *api.Handler
	mu       sync.RWMutex
}

// NewServer creates a new instance of Server.
func NewServer() *Server {
	dbEngine := storage.NewStorageEngine()

	s := &Server{
		router:   mux.NewRouter(),
		dbEngine: dbEngine,
		api:      api.NewHandler(dbEngine),
	}

	// Register API routes
	s.api.RegisterRoutes(s.router)

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
