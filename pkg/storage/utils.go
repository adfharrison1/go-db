package storage

import (
	"strings"

	"github.com/adfharrison1/go-db/pkg/domain"
)

// MatchesFilter checks if a document matches the given filter criteria
func MatchesFilter(doc domain.Document, filter map[string]interface{}) bool {
	for field, expectedValue := range filter {
		actualValue, exists := doc[field]
		if !exists {
			return false // Field doesn't exist in document
		}

		if !ValuesMatch(actualValue, expectedValue) {
			return false // Values don't match
		}
	}
	return true // All filter criteria match
}

// ValuesMatch compares two values for equality, handling different types
func ValuesMatch(actual, expected interface{}) bool {
	// Handle nil values
	if actual == nil && expected == nil {
		return true
	}
	if actual == nil || expected == nil {
		return false
	}

	// Handle string comparison (case-insensitive for better UX)
	if actualStr, ok1 := actual.(string); ok1 {
		if expectedStr, ok2 := expected.(string); ok2 {
			return strings.EqualFold(actualStr, expectedStr)
		}
	}

	// Handle numeric comparison
	if actualNum, ok1 := ToFloat64(actual); ok1 {
		if expectedNum, ok2 := ToFloat64(expected); ok2 {
			return actualNum == expectedNum
		}
	}

	// Default to direct comparison
	return actual == expected
}

// ToFloat64 converts various numeric types to float64 for comparison
func ToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}

// IntersectStringSlices returns the intersection of multiple string slices
// This is used for index intersection in multi-field queries
func IntersectStringSlices(slices ...[]string) []string {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	// Create a map to track counts of each ID
	countMap := make(map[string]int)

	// Count occurrences of each ID across all slices
	for _, slice := range slices {
		for _, id := range slice {
			countMap[id]++
		}
	}

	// Find IDs that appear in all slices (count equals number of slices)
	var result []string
	expectedCount := len(slices)
	for id, count := range countMap {
		if count == expectedCount {
			result = append(result, id)
		}
	}

	return result
}
