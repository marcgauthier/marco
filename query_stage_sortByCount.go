package marco

import (
	"fmt"
	"sort"
	"strings"
)

// sortByCountStage implements the $sortByCount aggregation stage.
// It groups documents by the specified expression, counts the number of documents in each group,
// and sorts the results in descending order of the count.
//
// Parameters:
// - input: Slice of documents to be processed
// - params: A map containing the $sortByCount parameter
//
// Returns:
// - A slice of documents with '_id' as the group key and 'count' as the number of documents in each group
// - An error if the stage parameters are invalid
func (db *DB) sortByCountStage(
	input []map[string]interface{},
	params map[string]interface{},
) ([]map[string]interface{}, error) {
	// Extract the expression to group by
	expr, ok := params["path"].(string)
	if !ok || expr == "" {
		return nil, fmt.Errorf("$sortByCount requires a non-empty 'path' parameter")
	}

	// Remove the leading '$' if present
	expr = strings.TrimPrefix(expr, "$")

	// Map to hold the count for each group
	countMap := make(map[interface{}]int)

	for _, doc := range input {
		// Retrieve the value based on the expression
		value, exists := doc[expr]
		if !exists {
			value = nil // Treat missing fields as nil
		}

		// Increment the count for this group
		countMap[value]++
	}

	// Construct the result slice
	result := make([]map[string]interface{}, 0, len(countMap))
	for key, count := range countMap {
		result = append(result, map[string]interface{}{
			"_id":   key,
			"count": count,
		})
	}

	// Sort the result by 'count' in descending order
	sort.Slice(result, func(i, j int) bool {
		// Ensure that 'count' is of type float64 for consistent comparison
		countI, okI := toFloat64(result[i]["count"])
		countJ, okJ := toFloat64(result[j]["count"])
		if !okI || !okJ {
			// Fallback to integer comparison if float conversion fails
			intCountI, _ := result[i]["count"].(int)
			intCountJ, _ := result[j]["count"].(int)
			return intCountI > intCountJ
		}
		return countI > countJ
	})

	return result, nil
}

// validateSortByCountStage validates the parameters for the $sortByCount stage.
//
// Parameters:
// - params: A map containing the $sortByCount parameter
//
// Returns:
// - An error if validation fails
func (db *DB) validateSortByCountStage(params map[string]interface{}) error {
	// Check if 'path' parameter exists
	path, ok := params["path"]
	if !ok {
		return fmt.Errorf("$sortByCount stage requires a 'path' parameter")
	}

	// Ensure that 'path' is a non-empty string
	pathStr, ok := path.(string)
	if !ok || strings.TrimSpace(pathStr) == "" {
		return fmt.Errorf("$sortByCount 'path' parameter must be a non-empty string")
	}

	// Optionally, further validation can be performed here (e.g., checking for valid field paths)

	return nil
}
