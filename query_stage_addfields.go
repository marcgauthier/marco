package marco

import (
	"errors"
	"fmt"
	"strings"
)

// addFieldsStage implements the $addFields and $set aggregation stages.
// It adds new fields to each document or updates existing fields with new values.
//
// Parameters:
// - input: Slice of documents to be processed.
// - params: A map containing the fields to add or set.
//
// Returns:
// - A slice of updated documents.
// - An error if the parameters are invalid or if field expressions cannot be evaluated.
//
// Behavior:
// - For each document, adds or updates fields as specified in params.
// - Supports simple field assignments and expressions.
// - If an expression is used, it should be a valid expression starting with "$".
func (db *DB) addFieldsStage(
	input []map[string]interface{},
	params map[string]interface{},
) ([]map[string]interface{}, error) {
	// Validate parameters before processing
	if err := db.validateAddFieldsStage(params); err != nil {
		return nil, fmt.Errorf("validation error in $addFields stage: %w", err)
	}

	// Iterate over each document and add/set fields
	for i, doc := range input {
		for field, expr := range params {
			// Evaluate the expression
			value, err := db.evaluateExpression(doc, expr)
			if err != nil {
				return nil, fmt.Errorf("error evaluating expression for field '%s': %w", field, err)
			}

			// Set the field to the evaluated value
			doc[field] = value
		}
		input[i] = doc
	}

	return input, nil
}

// validateAddFieldsStage validates the parameters for the $addFields and $set stages.
//
// Parameters:
// - params: A map containing the fields to add or set.
//
// Returns:
// - An error if validation fails; otherwise, nil.
func (db *DB) validateAddFieldsStage(params map[string]interface{}) error {
	if len(params) == 0 {
		return errors.New("$addFields/$set stage must not be empty")
	}

	for fieldName, expr := range params {
		// Field names must be non-empty strings
		if strings.TrimSpace(fieldName) == "" {
			return fmt.Errorf("$addFields/$set field name must be a non-empty string, got: %v", fieldName)
		}

		// Validate the expression associated with the field
		switch exprTyped := expr.(type) {
		case string:
			// If it's a string starting with "$", treat it as a field reference
			if strings.HasPrefix(exprTyped, "$") {
				refField := strings.TrimPrefix(exprTyped, "$")
				if refField == "" {
					return fmt.Errorf("$addFields/$set stage has an invalid field reference for field '%s'", fieldName)
				}
				// Optionally, you can check if refField exists in some schema or context
			}
			// Else, it's a direct string value; no action needed
		case float64, float32, int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64,
			bool, map[string]interface{}, []interface{}:
			// These types are acceptable
			// You can add more types or further validation as needed
		default:
			return fmt.Errorf("$addFields/$set stage has unsupported expression type for field '%s': %T", fieldName, expr)
		}
	}

	return nil
}

// evaluateExpression evaluates the given expression based on the document.
//
// Parameters:
// - doc: The current document being processed.
// - expr: The expression to evaluate.
//
// Returns:
// - The evaluated value.
// - An error if the expression cannot be evaluated.
func (db *DB) evaluateExpression(doc map[string]interface{}, expr interface{}) (interface{}, error) {
	switch v := expr.(type) {
	case string:
		// Handle simple field reference (e.g., "$existingField")
		if strings.HasPrefix(v, "$") {
			fieldName := v[1:]
			value, exists := doc[fieldName]
			if !exists {
				// Field does not exist; return nil or handle as needed
				return nil, nil
			}
			return value, nil
		}
		// Direct string value
		return v, nil
	case map[string]interface{}:
		// Handle expression objects
		if len(v) != 1 {
			return nil, fmt.Errorf("invalid expression object: %v", v)
		}
		for op, params := range v {
			if !isValidExpressionOperator(op) {
				return nil, fmt.Errorf("unsupported expression operator: %s", op)
			}
			switch op {
			case "$concat":
				return db.exprConcat(doc, params)
			case "$toString":
				return db.exprToString(doc, params)
			// Implement other expression operators as needed
			default:
				return nil, fmt.Errorf("unsupported expression operator: %s", op)
			}
		}
	case float64, float32, int, int32, int64, bool, nil:
		// Direct scalar value
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", v)
	}
	return nil, nil
}

// exprConcat handles the $concat expression operator.
//
// Parameters:
// - doc: The current document being processed.
// - params: The parameters for the $concat operator (expected to be a slice).
//
// Returns:
// - The concatenated string.
// - An error if the parameters are invalid.
func (db *DB) exprConcat(doc map[string]interface{}, params interface{}) (interface{}, error) {
	parts, ok := params.([]interface{})
	if !ok {
		return nil, fmt.Errorf("$concat expects an array of expressions, got: %T", params)
	}

	var result strings.Builder
	for _, part := range parts {
		str, err := db.evaluateExpression(doc, part)
		if err != nil {
			return nil, err
		}
		if str == nil {
			str = ""
		}
		result.WriteString(fmt.Sprintf("%v", str))
	}

	return result.String(), nil
}

// exprToString handles the $toString expression operator.
//
// Parameters:
// - doc: The current document being processed.
// - params: The parameter for the $toString operator (expected to be a single expression).
//
// Returns:
// - The string representation of the evaluated expression.
// - An error if the parameters are invalid.
func (db *DB) exprToString(doc map[string]interface{}, params interface{}) (interface{}, error) {
	// $toString expects a single expression
	expr := params

	// Evaluate the expression with the current document context
	value, err := db.evaluateExpression(doc, expr)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return "", nil
	}

	// Convert the value to string
	switch v := value.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return fmt.Sprintf("%v", v), nil
	default:
		return nil, fmt.Errorf("$toString cannot convert type: %T", v)
	}
}

// isValidExpressionOperator checks if the given operator is supported for expressions.
//
// Parameters:
// - op: The operator string to check.
//
// Returns:
// - True if the operator is supported; otherwise, false.
func isValidExpressionOperator(op string) bool {
	allowed := map[string]bool{
		"$concat":   true,
		"$toString": true, // Added $toString
		// Add more supported operators as needed
	}
	return allowed[op]
}
