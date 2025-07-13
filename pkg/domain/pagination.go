package domain

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// PaginationOptions defines pagination parameters
type PaginationOptions struct {
	// Cursor-based pagination
	After  string `json:"after,omitempty"`  // Base64 encoded cursor
	Before string `json:"before,omitempty"` // Base64 encoded cursor

	// Limit/offset pagination (fallback)
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`

	// Common
	MaxLimit int `json:"max_limit,omitempty"` // Maximum allowed limit
}

// PaginationResult contains pagination metadata
type PaginationResult struct {
	Documents  []Document `json:"documents"`
	HasNext    bool       `json:"has_next"`
	HasPrev    bool       `json:"has_prev"`
	NextCursor string     `json:"next_cursor,omitempty"`
	PrevCursor string     `json:"prev_cursor,omitempty"`
	Total      int64      `json:"total,omitempty"` // Only for offset-based
}

// Cursor represents a pagination cursor
type Cursor struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	SortKey   string    `json:"sort_key,omitempty"` // For custom sorting
}

// EncodeCursor encodes a cursor to base64
func EncodeCursor(cursor *Cursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("failed to marshal cursor: %w", err)
	}
	return base64.URLEncoding.EncodeToString(data), nil
}

// DecodeCursor decodes a base64 cursor
func DecodeCursor(encoded string) (*Cursor, error) {
	data, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to decode cursor: %w", err)
	}

	var cursor Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cursor: %w", err)
	}

	return &cursor, nil
}

// DefaultPaginationOptions returns default pagination settings
func DefaultPaginationOptions() *PaginationOptions {
	return &PaginationOptions{
		Limit:    50,
		MaxLimit: 1000,
	}
}

// Validate validates pagination options
func (po *PaginationOptions) Validate() error {
	if po.Limit < 0 {
		return fmt.Errorf("limit cannot be negative")
	}
	if po.Offset < 0 {
		return fmt.Errorf("offset cannot be negative")
	}
	if po.MaxLimit > 0 && po.Limit > po.MaxLimit {
		return fmt.Errorf("limit %d exceeds maximum %d", po.Limit, po.MaxLimit)
	}

	// Ensure we're not mixing cursor and offset pagination
	if (po.After != "" || po.Before != "") && (po.Offset > 0) {
		return fmt.Errorf("cannot mix cursor-based and offset-based pagination")
	}

	return nil
}
