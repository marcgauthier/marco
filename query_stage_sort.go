package marco

import (
	"fmt"
	"sort"
)

// sortStage implements a document sorting operation similar to MongoDB's $sort stage
// It allows sorting of a slice of documents based on multiple fields and directions
//
// Parameters:
// - input: Slice of documents to be sorted
// - params: A map specifying sort fields and their sort directions
//   - Keys are field names to sort by
//   - Values are sort directions: 1 (ascending), -1 (descending)
//
// Returns:
// - A new slice of documents sorted according to the specified parameters
//
// Sorting Behavior:
// - Supports multi-field sorting (primary, secondary, etc. sort keys)
// - Handles both numeric and string comparisons
// - Numeric values are prioritized over string comparisons
// - Uses stable sorting to maintain relative order of equal elements
// - Sort direction: 1 for ascending, -1 for descending
//
// Examples:
// { "$sort": { "amount": 1, "name": -1 } }
// - First sort by amount in ascending order
// - Then sort by name in descending order for items with equal amount
func (db *DB) sortStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	// Create a copy of the input to avoid modifying the original slice
	results := make([]map[string]interface{}, len(input))
	copy(results, input)

	// Use stable sort to maintain relative order of equal elements
	sort.SliceStable(results, func(i, j int) bool {
		// Iterate through sort fields in order
		for field, direction := range params {
			// Ensure sort direction is a valid numeric value
			dirFloat, ok := direction.(float64)
			if !ok {
				// Skip invalid sort directions
				continue
			}

			// Extract values for current field
			iVal := results[i][field]
			jVal := results[j][field]

			// Attempt to convert values to numeric for comparison
			iNum, iOk := toFloat64(iVal)
			jNum, jOk := toFloat64(jVal)

			// Prioritize numeric comparison if both values are numeric
			if iOk && jOk {
				if iNum == jNum {
					// If numeric values are equal, continue to next sort field
					continue
				}
				// Sort based on direction: 1 (ascending), -1 (descending)
				if dirFloat == 1 {
					return iNum < jNum
				}
				return iNum > jNum
			}

			// Fallback to string comparison for non-numeric values
			iStr := fmt.Sprintf("%v", iVal)
			jStr := fmt.Sprintf("%v", jVal)
			if iStr == jStr {
				// If string values are equal, continue to next sort field
				continue
			}
			// Sort based on direction: 1 (ascending), -1 (descending)
			if dirFloat == 1 {
				return iStr < jStr
			}
			return iStr > jStr
		}

		// If no conclusive sorting is found, maintain stable ordering
		return false
	})

	return results
}

func (db *DB) validateSortStage(params map[string]interface{}) error {

	// $sort expects { field: 1 or -1, ... }
	if len(params) == 0 {
		return fmt.Errorf("$sort stage must not be empty")
	}
	for field, val := range params {
		vNum, ok := val.(float64)
		if !ok {
			return fmt.Errorf("$sort field %q must have a numeric value (1 or -1)", field)
		}
		if vNum != 1 && vNum != -1 {
			return fmt.Errorf("$sort field %q must be either 1 or -1, got %v", field, vNum)
		}
	}
	return nil

}
