package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileHeader_WriteAndRead(t *testing.T) {
	// Test writing header
	var buf bytes.Buffer
	err := WriteHeader(&buf)
	require.NoError(t, err)

	// Verify header was written
	data := buf.Bytes()
	assert.Len(t, data, 8) // 4 bytes magic + 1 byte version + 1 byte flags + 2 bytes reserved

	// Test reading header
	header, err := ReadHeader(&buf)
	require.NoError(t, err)

	// Verify header contents
	assert.Equal(t, MagicBytes, string(header.Magic[:]))
	assert.EqualValues(t, FormatVersion, header.Version)
	assert.Equal(t, uint8(0), header.Flags)
	assert.Equal(t, [2]byte{0, 0}, header.Reserved)
}

func TestFileHeader_InvalidMagic(t *testing.T) {
	// Create buffer with invalid magic bytes
	var buf bytes.Buffer
	invalidHeader := FileHeader{
		Magic:    [4]byte{'I', 'N', 'V', 'L'},
		Version:  FormatVersion,
		Flags:    0,
		Reserved: [2]byte{0, 0},
	}

	// Write invalid header
	err := binary.Write(&buf, binary.LittleEndian, invalidHeader)
	require.NoError(t, err)

	// Try to read it
	_, err = ReadHeader(&buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid file format")
}

func TestFileHeader_InvalidVersion(t *testing.T) {
	// Create buffer with invalid version
	var buf bytes.Buffer
	invalidHeader := FileHeader{
		Magic:    [4]byte{'G', 'O', 'D', 'B'},
		Version:  99, // Invalid version
		Flags:    0,
		Reserved: [2]byte{0, 0},
	}

	// Write invalid header
	err := binary.Write(&buf, binary.LittleEndian, invalidHeader)
	require.NoError(t, err)

	// Try to read it
	_, err = ReadHeader(&buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported file version")
}

func TestFileHeader_ShortBuffer(t *testing.T) {
	// Create buffer with insufficient data
	var buf bytes.Buffer
	buf.Write([]byte{1, 2, 3}) // Only 3 bytes

	// Try to read header
	_, err := ReadHeader(&buf)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read header")
}

func TestStorageData_NewStorageData(t *testing.T) {
	storageData := NewStorageData()

	assert.NotNil(t, storageData)
	assert.NotNil(t, storageData.Collections)
	assert.NotNil(t, storageData.Indexes)
	assert.NotNil(t, storageData.Metadata)

	// Verify maps are empty
	assert.Len(t, storageData.Collections, 0)
	assert.Len(t, storageData.Indexes, 0)
	assert.Len(t, storageData.Metadata, 0)
}

func TestStorageData_AddCollections(t *testing.T) {
	storageData := NewStorageData()

	// Add some test data
	storageData.Collections["users"] = map[string]interface{}{
		"1": domain.Document{"name": "Alice"},
		"2": domain.Document{"name": "Bob", "age": 25},
	}

	storageData.Collections["products"] = map[string]interface{}{
		"1": domain.Document{"name": "Laptop", "price": 999.99},
	}

	// Verify data was added
	assert.Len(t, storageData.Collections, 2)
	assert.Len(t, storageData.Collections["users"], 2)
	assert.Len(t, storageData.Collections["products"], 1)

	// Verify specific values
	user1 := storageData.Collections["users"]["1"].(domain.Document)
	assert.Equal(t, "Alice", user1["name"])
	// No 'age' field for user1
	user2 := storageData.Collections["users"]["2"].(domain.Document)
	assert.Equal(t, "Bob", user2["name"])
	ageVal := user2["age"]
	switch v := ageVal.(type) {
	case int:
		assert.Equal(t, 25, v)
	case float64:
		assert.Equal(t, 25.0, v)
	default:
		t.Errorf("unexpected type for age: %T", v)
	}

	product1 := storageData.Collections["products"]["1"].(domain.Document)
	assert.Equal(t, "Laptop", product1["name"])
	assert.Equal(t, 999.99, product1["price"])
}

func TestStorageData_AddIndexes(t *testing.T) {
	storageData := NewStorageData()

	// Add some test indexes
	storageData.Indexes["users"] = map[string][]string{
		"name": {"1", "2"},
		"age":  {"2", "1"},
	}

	// Verify indexes were added
	assert.Len(t, storageData.Indexes, 1)
	assert.Len(t, storageData.Indexes["users"], 2)
	assert.Equal(t, []string{"1", "2"}, storageData.Indexes["users"]["name"])
	assert.Equal(t, []string{"2", "1"}, storageData.Indexes["users"]["age"])
}

func TestStorageData_AddMetadata(t *testing.T) {
	storageData := NewStorageData()

	// Add some test metadata
	storageData.Metadata["created_at"] = "2023-01-01T00:00:00Z"
	storageData.Metadata["version"] = "1.0.0"
	storageData.Metadata["total_documents"] = 1000

	// Verify metadata was added
	assert.Len(t, storageData.Metadata, 3)
	assert.Equal(t, "2023-01-01T00:00:00Z", storageData.Metadata["created_at"])
	assert.Equal(t, "1.0.0", storageData.Metadata["version"])
	assert.Equal(t, 1000, storageData.Metadata["total_documents"])
}

func TestConstants(t *testing.T) {
	// Test magic bytes
	assert.Equal(t, "GODB", MagicBytes)
	assert.Len(t, MagicBytes, 4)

	// Test format version
	assert.EqualValues(t, uint8(1), FormatVersion)
	assert.Greater(t, int(FormatVersion), 0)

	// Test file extension
	assert.Equal(t, ".godb", FileExtension)
	assert.True(t, len(FileExtension) > 0)
}

func TestFileHeader_Endianness(t *testing.T) {
	// Test that header is written in little endian
	var buf bytes.Buffer
	err := WriteHeader(&buf)
	require.NoError(t, err)

	data := buf.Bytes()

	// Magic bytes should be in correct order
	assert.Equal(t, byte('G'), data[0])
	assert.Equal(t, byte('O'), data[1])
	assert.Equal(t, byte('D'), data[2])
	assert.Equal(t, byte('B'), data[3])

	// Version should be in correct position
	assert.Equal(t, byte(FormatVersion), data[4])

	// Flags should be in correct position
	assert.Equal(t, byte(0), data[5])

	// Reserved bytes should be in correct position
	assert.Equal(t, byte(0), data[6])
	assert.Equal(t, byte(0), data[7])
}

func TestFileHeader_Flags(t *testing.T) {
	// Test that flags are properly handled
	var buf bytes.Buffer

	// Create header with non-zero flags
	header := FileHeader{
		Magic:    [4]byte{'G', 'O', 'D', 'B'},
		Version:  FormatVersion,
		Flags:    0x42,                // Some flags
		Reserved: [2]byte{0x12, 0x34}, // Some reserved values
	}

	// Write header
	err := binary.Write(&buf, binary.LittleEndian, header)
	require.NoError(t, err)

	// Read header
	readHeader, err := ReadHeader(&buf)
	require.NoError(t, err)

	// Verify flags and reserved bytes are preserved
	assert.Equal(t, uint8(0x42), readHeader.Flags)
	assert.Equal(t, [2]byte{0x12, 0x34}, readHeader.Reserved)
}
