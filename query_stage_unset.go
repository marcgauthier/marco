package marco

import (
	"encoding/json"
	"fmt"
)

// unsetStage removes specified fields from each document in the input slice.
//
// Parameters:
// - input: Slice of documents (maps) to process.
// - params: A string, slice of strings, or map[string]interface{} specifying the fields to unset.
//
// Returns:
// - A new slice of documents with the specified fields removed.
// - An error if validation fails.
func (db *DB) unsetStage(
	input []map[string]interface{},
	params interface{},
) ([]map[string]interface{}, error) {

	// Validate the parameters
	fields, err := db.validateUnsetStage(params)
	if err != nil {
		return nil, err
	}

	// Debug statement (optional)
	b, _ := json.Marshal(fields)
	fmt.Println("unset fields=", string(b))

	// Create a copy of the input to avoid modifying the original slice
	results := make([]map[string]interface{}, len(input))
	for i, doc := range input {
		// Create a shallow copy of the document
		newDoc := make(map[string]interface{})
		for k, v := range doc {
			newDoc[k] = v
		}

		// Remove the fields specified
		for _, field := range fields {
			delete(newDoc, field)
		}

		results[i] = newDoc
	}

	return results, nil
}

// validateUnsetStage ensures params is valid for the $unset operation.
//
// Accepted formats:
// - A string: single field to remove.
// - A slice of strings or interfaces: multiple fields to remove.
// - A map[string]interface{} where keys are the fields to remove.
// - A map[string]interface{} with a "path" key specifying the fields to remove.
//
// Returns:
// - A slice of valid field names.
// - An error if validation fails.
func (db *DB) validateUnsetStage(params interface{}) ([]string, error) {
	var fields []string

	fmt.Printf("DEBUG: validateUnsetStage called with params type: %T, value: %v\n", params, params)

	switch v := params.(type) {
	case string:
		// Single field name
		if v == "" {
			return nil, fmt.Errorf("$unset stage contains an empty field name")
		}
		fields = append(fields, v)

	case []string:
		// List of field names
		for _, field := range v {
			if field == "" {
				return nil, fmt.Errorf("$unset stage contains an empty field name")
			}
			fields = append(fields, field)
		}

	case []interface{}:
		// List of field names as interface{}
		for _, field := range v {
			s, ok := field.(string)
			if !ok {
				return nil, fmt.Errorf("$unset stage array contains a non-string field: %v", field)
			}
			if s == "" {
				return nil, fmt.Errorf("$unset stage contains an empty field name")
			}
			fields = append(fields, s)
		}

	case map[string]interface{}:
		// Check if the map has a "path" key
		if pathVal, exists := v["path"]; exists {
			// Process the "path" value
			switch path := pathVal.(type) {
			case string:
				if path == "" {
					return nil, fmt.Errorf("$unset stage contains an empty field name in path")
				}
				fields = append(fields, path)
			case []string:
				for _, field := range path {
					if field == "" {
						return nil, fmt.Errorf("$unset stage contains an empty field name in path")
					}
					fields = append(fields, field)
				}
			case []interface{}:
				for _, field := range path {
					s, ok := field.(string)
					if !ok {
						return nil, fmt.Errorf("$unset stage path array contains a non-string field: %v", field)
					}
					if s == "" {
						return nil, fmt.Errorf("$unset stage contains an empty field name in path")
					}
					fields = append(fields, s)
				}
			default:
				return nil, fmt.Errorf("$unset stage 'path' must be a string or array of strings")
			}
		} else {
			// Existing handling: map with field names as keys
			for field := range v {
				if field == "" {
					return nil, fmt.Errorf("$unset stage contains an empty field name")
				}
				fields = append(fields, field)
			}
		}

	default:
		return nil, fmt.Errorf("invalid $unset stage parameters: must be a string, array of strings, or map")
	}

	return fields, nil
}
