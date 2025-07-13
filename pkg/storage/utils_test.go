package storage

import (
	"reflect"
	"sort"
	"testing"

	"github.com/adfharrison1/go-db/pkg/domain"
	"github.com/stretchr/testify/assert"
)

func TestMatchesFilter(t *testing.T) {
	doc := domain.Document{"name": "Alice", "age": 30, "city": "New York"}
	assert.True(t, MatchesFilter(doc, map[string]interface{}{"name": "Alice"}))
	assert.True(t, MatchesFilter(doc, map[string]interface{}{"age": 30}))
	assert.True(t, MatchesFilter(doc, map[string]interface{}{"city": "New York"}))
	assert.True(t, MatchesFilter(doc, map[string]interface{}{"name": "Alice", "age": 30}))
	assert.False(t, MatchesFilter(doc, map[string]interface{}{"name": "Bob"}))
	assert.False(t, MatchesFilter(doc, map[string]interface{}{"country": "USA"}))
}

func TestValuesMatch(t *testing.T) {
	assert.True(t, ValuesMatch("Alice", "alice")) // case-insensitive
	assert.True(t, ValuesMatch(42, 42))
	assert.True(t, ValuesMatch(42, 42.0))
	assert.True(t, ValuesMatch(nil, nil))
	assert.False(t, ValuesMatch(nil, 1))
	assert.False(t, ValuesMatch("Alice", "Bob"))
	assert.False(t, ValuesMatch(42, 43))
	// Use InDelta for float comparison
	f, ok := ToFloat64(float32(3.14))
	assert.True(t, ok)
	assert.InDelta(t, 3.14, f, 1e-6)
	f2, ok2 := ToFloat64(int64(7))
	assert.True(t, ok2)
	assert.InDelta(t, 7.0, f2, 1e-6)
}

func TestToFloat64(t *testing.T) {
	cases := []struct {
		input    interface{}
		expected float64
		ok       bool
	}{
		{42, 42.0, true},
		{int32(7), 7.0, true},
		{int64(8), 8.0, true},
		{float32(3.5), 3.5, true},
		{float64(2.2), 2.2, true},
		{uint(5), 5.0, true},
		{uint32(6), 6.0, true},
		{uint64(9), 9.0, true},
		{"not a number", 0, false},
	}
	for _, c := range cases {
		result, ok := ToFloat64(c.input)
		if c.ok {
			assert.True(t, ok)
			assert.Equal(t, c.expected, result)
		} else {
			assert.False(t, ok)
		}
	}
}

func TestIntersectStringSlices(t *testing.T) {
	tests := []struct {
		name     string
		slices   [][]string
		expected []string
	}{
		{
			name:     "empty slices",
			slices:   [][]string{},
			expected: nil,
		},
		{
			name:     "single slice",
			slices:   [][]string{{"a", "b", "c"}},
			expected: []string{"a", "b", "c"},
		},
		{
			name: "two slices with intersection",
			slices: [][]string{
				{"a", "b", "c", "d"},
				{"b", "c", "e", "f"},
			},
			expected: []string{"b", "c"},
		},
		{
			name: "three slices with intersection",
			slices: [][]string{
				{"a", "b", "c", "d"},
				{"b", "c", "e", "f"},
				{"b", "c", "g", "h"},
			},
			expected: []string{"b", "c"},
		},
		{
			name: "no intersection",
			slices: [][]string{
				{"a", "b", "c"},
				{"d", "e", "f"},
			},
			expected: []string{},
		},
		{
			name: "empty slice in input",
			slices: [][]string{
				{"a", "b", "c"},
				{},
				{"b", "c", "d"},
			},
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntersectStringSlices(tt.slices...)

			// Sort both slices for comparison
			sort.Strings(result)
			sort.Strings(tt.expected)

			// Handle empty slice comparison
			if len(result) == 0 && len(tt.expected) == 0 {
				return // Both are empty, test passes
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("IntersectStringSlices() = %v, want %v", result, tt.expected)
			}
		})
	}
}
