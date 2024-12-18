package marco

import "fmt"

// limitStage implements a document limiting operation similar to MongoDB's $limit stage
// It restricts the number of documents returned from an input slice
//
// Parameters:
// - input: Slice of documents to be limited
// - params: A map containing the limit parameter
//
// Returns:
// - A slice containing at most the specified number of documents
//
// Behavior:
// - If no valid limit is provided, returns the original input
// - If limit is greater than input length, returns all input documents
// - If limit is a negative or zero value, returns an empty slice

func (db *DB) limitStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	// Extract limit value, handling different potential input types

	limitFloat, ok := toFloat64(params["$limit"])
	if !ok {
		limitFloat, ok = toFloat64(params["value"])
		if !ok {
			return input
		}
	}

	limit := int(limitFloat)

	// Handle edge cases
	switch {
	case limit <= 0:
		// Return empty slice for non-positive limits
		return []map[string]interface{}{}
	case limit >= len(input):
		// Return all documents if limit exceeds input length
		return input
	default:
		// Return first 'limit' number of documents
		return input[:limit]
	}
}

// Example usage:
// documents := []map[string]interface{}{...}
// params := map[string]interface{}{"$limit": 5}
// limitedDocuments := limitStage(documents, params)

func (db *DB) validateLimitStage(params map[string]interface{}) error {
	// $limit expects a positive number
	v, ok := params["value"].(float64)
	if !ok {
		return fmt.Errorf("$limit must have a numeric value")
	}
	if v < 0 {
		return fmt.Errorf("$limit value must be non-negative")
	}
	return nil

}
