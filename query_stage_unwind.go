package marco

import (
	"fmt"
	"log"
	"strings"
)

// unwindStage refactored to support preserveNullAndEmptyArrays and includeArrayIndex options
func (db *DB) unwindStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	// Extract and normalize the path to unwind
	pathParam, ok := params["path"].(string)
	if !ok || pathParam == "" {
		log.Println("Error: Invalid or missing path for $unwind")
		return input
	}
	path := strings.TrimPrefix(pathParam, "$")

	// Extract optional parameters
	preserveNullAndEmptyArrays, _ := params["preserveNullAndEmptyArrays"].(bool)
	includeArrayIndexField, _ := params["includeArrayIndex"].(string)

	var results []map[string]interface{}

	// Iterate through each input document
	for _, doc := range input {
		arrayToUnwind, exists := doc[path]

		// If the field doesn't exist or is nil:
		// - If preserveNullAndEmptyArrays is true, keep the original doc as-is.
		// - Otherwise, skip the doc (same as original code).
		if !exists || arrayToUnwind == nil {
			if preserveNullAndEmptyArrays {
				// Pass the original document through unchanged
				results = append(results, doc)
			}
			continue
		}

		// Handle array types. If arrayToUnwind is not an array, treat it as a single item.
		switch arr := arrayToUnwind.(type) {

		// 1) If it's a []map[string]interface{}
		case []map[string]interface{}:
			// If empty array, decide based on preserveNullAndEmptyArrays
			if len(arr) == 0 {
				if preserveNullAndEmptyArrays {
					// Pass the original doc as-is
					results = append(results, doc)
				}
				continue
			}

			// Process each element
			for idx, itemMap := range arr {
				newDoc := cloneDocument(doc)
				newDoc[path] = itemMap

				// If includeArrayIndexField is specified, add the index
				if includeArrayIndexField != "" {
					newDoc[includeArrayIndexField] = idx
				}
				results = append(results, newDoc)
			}

		// 2) If it's a []interface{}
		case []interface{}:
			// If empty array, handle preserveNullAndEmptyArrays
			if len(arr) == 0 {
				if preserveNullAndEmptyArrays {
					results = append(results, doc)
				}
				continue
			}

			for idx, item := range arr {
				itemMap, ok := item.(map[string]interface{})
				if !ok {
					// If item can’t be converted to map, wrap it under "value" key
					itemMap = map[string]interface{}{"value": item}
				}

				newDoc := cloneDocument(doc)
				newDoc[path] = itemMap

				// Optionally include the array index
				if includeArrayIndexField != "" {
					newDoc[includeArrayIndexField] = idx
				}
				results = append(results, newDoc)
			}

		// 3) If it's not a slice at all, treat it like a single item (like MongoDB does).
		default:
			// For a single value, if preserveNullAndEmptyArrays is on, that doc remains with a single unwound item.
			newDoc := cloneDocument(doc)
			newDoc[path] = arr
			// No index is relevant because it's not actually an array
			results = append(results, newDoc)
		}
	}

	return results
}

// cloneDocument is a helper that copies the original map to avoid mutation issues.
// (Alternative: you could do a deep copy if there are nested maps, but shallow clone may suffice.)
func cloneDocument(original map[string]interface{}) map[string]interface{} {
	newDoc := make(map[string]interface{}, len(original))
	for k, v := range original {
		newDoc[k] = v
	}
	return newDoc
}

func (db *DB) validateUnwindStage(params map[string]interface{}) error {

	// For unwind, path is typically required; preserveNullAndEmptyArrays and includeArrayIndex are optional
	if _, ok := params["path"]; !ok {
		return fmt.Errorf("$unwind is missing 'path' field")
	}
	pathStr, ok := params["path"].(string)
	if !ok || pathStr == "" {
		return fmt.Errorf("$unwind 'path' must be a non-empty string")
	}

	// Optional fields
	if pNullEmpty, ok := params["preserveNullAndEmptyArrays"]; ok {
		if _, isBool := pNullEmpty.(bool); !isBool {
			return fmt.Errorf("$unwind 'preserveNullAndEmptyArrays' must be a boolean if set")
		}
	}
	if arrIndex, ok := params["includeArrayIndex"]; ok {
		if _, isString := arrIndex.(string); !isString {
			return fmt.Errorf("$unwind 'includeArrayIndex' must be a string if set")
		}
	}

	return nil

}
