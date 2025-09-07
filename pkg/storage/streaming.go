package storage

import (
	"github.com/adfharrison1/go-db/pkg/domain"
)

// FindAllStream streams documents that match the given filter criteria
// This is the true streaming implementation that yields documents one at a time
// without loading everything into memory first.
// NOTE: This method does NOT apply pagination - it streams ALL matching documents.
// Use FindAll for paginated queries, or handle pagination at the API/client level.
func (se *StorageEngine) FindAllStream(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	// First, check if the collection exists before starting the goroutine
	err := se.withCollectionReadLock(collName, func() error {
		_, err := se.getCollectionInternal(collName)
		return err
	})

	if err != nil {
		return nil, err
	}

	out := make(chan domain.Document, 100)

	go func() {
		defer close(out)

		// Use collection read lock to safely collect all matching documents
		err := se.withCollectionReadLock(collName, func() error {
			collection, err := se.getCollectionInternal(collName)
			if err != nil {
				return err
			}

			var candidateIDs []string
			var useIndex bool

			// Try to use index optimization if filter is present
			if len(filter) > 0 {
				candidateIDs, useIndex = se.optimizeWithIndexes(collName, filter)
			}

			if useIndex {
				// Stream documents using index optimization
				for _, docID := range candidateIDs {
					if doc, exists := collection.Documents[docID]; exists {
						if MatchesFilter(doc, filter) {
							out <- doc
						}
					}
				}
			} else {
				// Stream documents using full scan
				for _, doc := range collection.Documents {
					if len(filter) == 0 || MatchesFilter(doc, filter) {
						out <- doc
					}
				}
			}
			return nil
		})

		if err != nil {
			// If there's an error, we can't send it through the channel
			// The channel will be closed and the error will be lost
			// This is a limitation of the streaming interface
		}
	}()

	return out, nil
}

// streamGeneratorUnsafe yields matching documents for a given filter, using index optimization if possible.
// This is the core streaming implementation that yields documents one at a time.
// NOTE: This function assumes the caller holds the appropriate collection lock.
func (se *StorageEngine) streamGeneratorUnsafe(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	out := make(chan domain.Document, 100)

	collection, err := se.getCollectionInternal(collName)
	if err != nil {
		close(out)
		return nil, err
	}

	go func() {
		defer close(out)
		var candidateIDs []string
		var useIndex bool

		// Try to use index optimization if filter is present
		if len(filter) > 0 {
			candidateIDs, useIndex = se.optimizeWithIndexes(collName, filter)
		}

		if useIndex {
			// Stream documents using index optimization
			for _, docID := range candidateIDs {
				if doc, exists := collection.Documents[docID]; exists {
					if MatchesFilter(doc, filter) {
						out <- doc
					}
				}
			}
		} else {
			// Stream documents using full scan
			for _, doc := range collection.Documents {
				if len(filter) == 0 || MatchesFilter(doc, filter) {
					out <- doc
				}
			}
		}
	}()
	return out, nil
}

// streamGenerator yields matching documents for a given filter, using index optimization if possible.
// This is the core streaming implementation that yields documents one at a time.
// DEPRECATED: Use streamGeneratorUnsafe with proper locking instead.
func (se *StorageEngine) streamGenerator(collName string, filter map[string]interface{}) (<-chan domain.Document, error) {
	out := make(chan domain.Document, 100)

	se.mu.RLock()
	collection, err := se.getCollectionInternal(collName)
	se.mu.RUnlock()
	if err != nil {
		close(out)
		return nil, err
	}

	go func() {
		defer close(out)
		var candidateIDs []string
		var useIndex bool

		// Try to use index optimization if filter is present
		if len(filter) > 0 {
			candidateIDs, useIndex = se.optimizeWithIndexes(collName, filter)
		}

		if useIndex {
			// Stream documents using index optimization
			for _, docID := range candidateIDs {
				if doc, exists := collection.Documents[docID]; exists {
					if MatchesFilter(doc, filter) {
						out <- doc
					}
				}
			}
		} else {
			// Stream documents using full scan
			for _, doc := range collection.Documents {
				if len(filter) == 0 || MatchesFilter(doc, filter) {
					out <- doc
				}
			}
		}
	}()
	return out, nil
}
