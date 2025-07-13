package storage

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// Magic bytes to identify our file format
	MagicBytes = "GODB"
	// Current version
	FormatVersion = 1
	// File extension for our optimized format
	FileExtension = ".godb"
)

// FileHeader represents the header of our storage file
type FileHeader struct {
	Magic    [4]byte // "GODB"
	Version  uint8   // Format version
	Flags    uint8   // Reserved for future use
	Reserved [2]byte // Reserved for future use
}

// WriteHeader writes the file header to the given writer
func WriteHeader(w io.Writer) error {
	header := FileHeader{
		Magic:    [4]byte{'G', 'O', 'D', 'B'},
		Version:  FormatVersion,
		Flags:    0,
		Reserved: [2]byte{0, 0},
	}

	return binary.Write(w, binary.LittleEndian, header)
}

// ReadHeader reads and validates the file header
func ReadHeader(r io.Reader) (*FileHeader, error) {
	var header FileHeader
	if err := binary.Read(r, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Validate magic bytes
	if string(header.Magic[:]) != MagicBytes {
		return nil, fmt.Errorf("invalid file format: expected %s, got %s", MagicBytes, string(header.Magic[:]))
	}

	// Validate version
	if header.Version != FormatVersion {
		return nil, fmt.Errorf("unsupported file version: %d", header.Version)
	}

	return &header, nil
}

// StorageData represents the actual data structure we store
type StorageData struct {
	Collections map[string]map[string]interface{} `msgpack:"collections"`
	Indexes     map[string]map[string][]string    `msgpack:"indexes,omitempty"`
	Metadata    map[string]interface{}            `msgpack:"metadata,omitempty"`
}

// NewStorageData creates a new empty storage data structure
func NewStorageData() *StorageData {
	return &StorageData{
		Collections: make(map[string]map[string]interface{}),
		Indexes:     make(map[string]map[string][]string),
		Metadata:    make(map[string]interface{}),
	}
}
