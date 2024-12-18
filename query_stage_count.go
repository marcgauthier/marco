package marco

import (
	"errors"
	"fmt"
	"strings"
)

// countStage implements MongoDB's $count stage, counting documents in the input slice.
// It produces a single document with the specified field name containing the count.
//
// Parameters:
// - input: Slice of documents to be processed.
// - params: Can be a string representing the field name, or a map with a 'field' key.
//
// Returns:
// - A slice containing a single document with the count.
// - An error if the input is invalid.
func (db *DB) countStage(
	input []map[string]interface{},
	params interface{},
) ([]map[string]interface{}, error) {
	var fieldName string

	// Determine the field name based on the input params
	switch v := params.(type) {
	case string:
		// Directly use the string as the field name
		if strings.TrimSpace(v) == "" {
			return nil, errors.New("$count stage requires a non-empty string as the field name")
		}
		fieldName = v
	case map[string]interface{}:
		// Use 'field' key if provided
		if fieldVal, ok := v["field"]; ok {
			if fieldStr, ok := fieldVal.(string); ok && strings.TrimSpace(fieldStr) != "" {
				fieldName = fieldStr
			} else {
				return nil, errors.New("$count 'field' parameter must be a non-empty string")
			}
			// Use 'field' key if provided
		} else if fieldVal, ok := v["$count"]; ok {
			if fieldStr, ok := fieldVal.(string); ok && strings.TrimSpace(fieldStr) != "" {
				fieldName = fieldStr
			} else {
				return nil, errors.New("$count 'field' parameter must be a non-empty string")
			}
		} else if fieldVal, ok := v["path"]; ok {
			if fieldStr, ok := fieldVal.(string); ok && strings.TrimSpace(fieldStr) != "" {
				fieldName = fieldStr
			} else {
				return nil, errors.New("$count 'field' parameter must be a non-empty string")
			}
		} else {

			return nil, errors.New("$count stage requires a 'field' key in the map")
		}
	default:
		fmt.Println("type = ", v)
		return nil, errors.New("$count stage requires a string or a map with a 'field' key")
	}

	// Count the number of documents
	count := len(input)

	// Create the result document
	result := map[string]interface{}{
		fieldName: count,
	}

	return []map[string]interface{}{result}, nil
}

// validateCountStage validates the parameters for the $count stage.
//
// Parameters:
// - params: Can be a string (field name) or a map containing the 'field' parameter.
//
// Returns:
// - An error if validation fails; otherwise, nil.
func (db *DB) validateCountStage(params interface{}) error {
	//b, _ := json.Marshal(params)
	//fmt.Printf("******* $count params type = %T, value = %s\n", params, string(b))

	switch v := params.(type) {
	case string:
		// Validate a direct string as the field name
		if strings.TrimSpace(v) == "" {
			return errors.New("$count stage requires a non-empty string as the field name")
		}
		if strings.HasPrefix(v, "$") {
			return fmt.Errorf("$count field name must not start with '$'")
		}
		if strings.ContainsAny(v, " \t\n") {
			return fmt.Errorf("$count field name must not contain whitespace characters")
		}
	case map[string]interface{}:
		// Validate 'field' key in a map
		fieldVal, ok := v["field"]
		if !ok {
			fieldVal, ok = v["$count"]
			if !ok {
				fieldVal, ok = v["path"]
				if !ok {
					return errors.New("$count stage requires a 'field' parameter in the map")
				}
			}
		}

		// Ensure 'field' is a non-empty string
		fieldName, ok := fieldVal.(string)
		if !ok || strings.TrimSpace(fieldName) == "" {
			return errors.New("$count 'field' parameter must be a non-empty string")
		}
		if strings.HasPrefix(fieldName, "$") {
			return fmt.Errorf("$count 'field' parameter must not start with '$'")
		}
		if strings.ContainsAny(fieldName, " \t\n") {
			return fmt.Errorf("$count 'field' parameter must not contain whitespace characters")
		}
	default:
		// Invalid input type
		return errors.New("$count stage requires a string or a map with a 'field' key")
	}

	return nil
}
