package marco

import (
	"fmt"
	"sort"
)

// bucketStage implements the $bucket aggregation stage.
// It categorizes documents into specified buckets based on the groupBy field.
func (db *DB) bucketStage(
	input []map[string]interface{},
	params map[string]interface{},
) ([]map[string]interface{}, error) {
	// Extract required parameters
	groupBy, ok := params["groupBy"].(string)
	if !ok {
		return nil, fmt.Errorf("$bucket stage requires a string 'groupBy' field")
	}

	// Extract boundaries
	boundariesInterface, ok := params["boundaries"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("$bucket stage requires an array of 'boundaries'")
	}

	// Convert boundaries to a slice of float64 or string, depending on the groupBy field
	var boundaries []float64
	for _, b := range boundariesInterface {
		switch v := b.(type) {
		case float64:
			boundaries = append(boundaries, v)
		case int:
			boundaries = append(boundaries, float64(v))
		default:
			return nil, fmt.Errorf("$bucket stage 'boundaries' must be numeric")
		}
	}

	// Ensure boundaries are sorted
	sort.Float64s(boundaries)

	// Extract default bucket if provided
	_, hasDefault := params["default"].(string)

	// Extract output definitions
	output, hasOutput := params["output"].(map[string]interface{})

	// Prepare buckets
	type Bucket struct {
		Label        string
		Docs         []map[string]interface{}
		Aggregations map[string]interface{}
	}

	buckets := []Bucket{}
	for i := 0; i < len(boundaries)-1; i++ {
		label := fmt.Sprintf("[%v, %v)", boundaries[i], boundaries[i+1])
		buckets = append(buckets, Bucket{
			Label:        label,
			Docs:         []map[string]interface{}{},
			Aggregations: make(map[string]interface{}),
		})
	}

	// Default bucket
	if hasDefault {
		buckets = append(buckets, Bucket{
			Label:        "Other",
			Docs:         []map[string]interface{}{},
			Aggregations: make(map[string]interface{}),
		})
	}

	// Assign documents to buckets
	for _, doc := range input {
		value, exists := doc[groupBy]
		if !exists {
			if hasDefault {
				buckets[len(buckets)-1].Docs = append(buckets[len(buckets)-1].Docs, doc)
			}
			continue
		}

		var numericValue float64
		switch v := value.(type) {
		case float64:
			numericValue = v
		case int:
			numericValue = float64(v)
		default:
			// Unsupported type for groupBy
			if hasDefault {
				buckets[len(buckets)-1].Docs = append(buckets[len(buckets)-1].Docs, doc)
			}
			continue
		}

		// Find the appropriate bucket
		placed := false
		for i := 0; i < len(boundaries)-1; i++ {
			if numericValue >= boundaries[i] && numericValue < boundaries[i+1] {
				buckets[i].Docs = append(buckets[i].Docs, doc)
				placed = true
				break
			}
		}

		// If not placed and has default, place in default bucket
		if !placed && hasDefault {
			buckets[len(buckets)-1].Docs = append(buckets[len(buckets)-1].Docs, doc)
		}
	}

	// Prepare output
	results := []map[string]interface{}{}
	for _, bucket := range buckets {
		result := make(map[string]interface{})
		result["_id"] = bucket.Label

		// Process output aggregations
		if hasOutput {
			for key, expr := range output {
				switch e := expr.(type) {
				case map[string]interface{}:
					for op, field := range e {
						switch op {
						case "$sum":
							// Use toFloat64 to handle various types for the $sum field
							sumValue, success := toFloat64(field)
							if !success {
								return nil, fmt.Errorf("$sum field must be numeric or string representing a number")
							}
							if sumValue == 1 {
								result[key] = len(bucket.Docs)
							} else {
								return nil, fmt.Errorf("$sum currently supports only counting documents (field value must be 1)")
							}
						// Implement other aggregation operations as needed
						default:
							return nil, fmt.Errorf("unsupported aggregation operator in $bucket output: %s", op)
						}
					}
				default:
					return nil, fmt.Errorf("$bucket stage 'output' must be an object")
				}
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// validateBucketStage validates the parameters for the $bucket stage.
func (db *DB) validateBucketStage(params map[string]interface{}) error {
	// Check required fields
	if _, ok := params["groupBy"]; !ok {
		return fmt.Errorf("$bucket stage requires a 'groupBy' field")
	}
	if _, ok := params["boundaries"]; !ok {
		return fmt.Errorf("$bucket stage requires 'boundaries' field")
	}

	// Validate 'groupBy'
	groupBy, ok := params["groupBy"].(string)
	if !ok || groupBy == "" {
		return fmt.Errorf("$bucket stage 'groupBy' must be a non-empty string")
	}

	// Validate 'boundaries'
	boundaries, ok := params["boundaries"].([]interface{})
	if !ok || len(boundaries) < 2 {
		return fmt.Errorf("$bucket stage 'boundaries' must be an array with at least two elements")
	}
	for _, b := range boundaries {
		switch b.(type) {
		case float64, int:
			// Valid types
		default:
			return fmt.Errorf("$bucket stage 'boundaries' must contain numeric values")
		}
	}

	// Validate 'default' if present
	if defaultVal, ok := params["default"]; ok {
		if _, ok := defaultVal.(string); !ok {
			return fmt.Errorf("$bucket stage 'default' must be a string")
		}
	}

	// Validate 'output' if present
	if output, ok := params["output"]; ok {
		if _, ok := output.(map[string]interface{}); !ok {
			return fmt.Errorf("$bucket stage 'output' must be an object")
		}
		// Further validation of 'output' can be added here
	}

	return nil
}
