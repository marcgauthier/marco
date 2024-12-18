package marco

import (
	"fmt"
	"log"
	"math"
)

// skipStage implements a document skipping operation similar to MongoDB's $skip stage
// It removes a specified number of documents from the beginning of the input slice
//
// Parameters:
// - input: Slice of documents to be processed
// - params: A map containing the skip parameter
//
// Returns:
// - A slice of documents with the specified number of initial documents removed
//
// Behavior:
// - If no valid skip value is provided, returns the original input
// - If skip value is greater than input length, returns an empty slice
// - Supports specifying skip value using "$skip" or "value" keys
// - Uses toFloat64 for flexible type conversion of skip parameter
func (db *DB) skipStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	// Attempt to extract skip value, first from "$skip", then from "value"
	skip, ok := toFloat64(params["$skip"])
	if !ok {
		// Fallback to "value" key if "$skip" is not present or invalid
		skip, ok = toFloat64(params["value"])
		if !ok {
			// If no valid skip value is found, return original input
			log.Println("Warning: No valid skip value provided")
			return input
		}
	}

	// Convert skip to integer, handling potential float values
	n := int(math.Max(0, math.Floor(skip)))

	// If skip is greater than input length, return empty slice
	if n > len(input) {
		return []map[string]interface{}{}
	}

	// Return slice starting from the nth document
	return input[n:]
}

func (db *DB) validateSkipStage(params map[string]interface{}) error {

	// $skip expects a positive number
	v, ok := params["value"].(float64)
	if !ok {
		return fmt.Errorf("$skip must have a numeric value")
	}
	if v < 0 {
		return fmt.Errorf("$skip value must be non-negative")
	}
	return nil

}
