package marco

import (
	"fmt"
	"log"
	"math"
	"sort"
)

// cleanGroupByField removes the '$' prefix if present.
func cleanGroupByField(field string) string {
	if len(field) > 0 && field[0] == '$' {
		return field[1:]
	}
	return field
}

// Bucket represents a single bucket with its label, documents, and aggregations.
type Bucket struct {
	Label        string
	Docs         []map[string]interface{}
	Aggregations map[string]interface{}
}

// bucketAutoStage implements the $bucketAuto aggregation stage.
// It automatically determines the bucket boundaries to group documents into a specified number of buckets.
func (db *DB) bucketAutoStage(
	input []map[string]interface{},
	params map[string]interface{},
) ([]map[string]interface{}, error) {
	// Validate parameters first
	if err := db.validateBucketAutoStage(params); err != nil {
		return nil, err
	}

	// Extract and clean 'groupBy' parameter
	groupByRaw, _ := params["groupBy"].(string)
	groupBy := cleanGroupByField(groupByRaw)
	log.Println("Using groupBy field:", groupBy)

	// Extract number of buckets
	bucketsParam := params["buckets"]
	numBuckets, _ := toFloat64(bucketsParam)
	numBucketsInt := int(numBuckets)
	log.Printf("Number of buckets: %d\n", numBucketsInt)

	// Extract output definitions
	output, hasOutput := params["output"].(map[string]interface{})

	// Collect all groupBy values
	values := []float64{}
	for _, doc := range input {
		value, exists := doc[groupBy]
		if !exists {
			log.Printf("Document %v does not have the 'groupBy' field '%v'. Skipping.\n", doc["_id"], groupBy)
			continue
		}
		numericValue, ok := toFloat64(value)
		if !ok {
			log.Printf("Document %v has unsupported 'groupBy' type: %T. Skipping.\n", doc["_id"], value)
			continue
		}
		values = append(values, numericValue)
	}

	if len(values) == 0 {
		return nil, fmt.Errorf("$bucketAuto stage found no valid 'groupBy' values")
	}

	log.Printf("Collected %d valid 'groupBy' values.\n", len(values))

	// Sort the values
	sort.Float64s(values)
	log.Println("Sorted groupBy values.")

	// Determine bucket boundaries using quantiles
	boundaries := []float64{values[0]}
	for i := 1; i < numBucketsInt; i++ {
		// Calculate the quantile position
		pos := math.Round(float64(i) * float64(len(values)-1) / float64(numBucketsInt))
		if pos < 0 {
			pos = 0
		} else if pos >= float64(len(values)) {
			pos = float64(len(values) - 1)
		}
		boundaries = append(boundaries, values[int(pos)])
	}
	boundaries = append(boundaries, values[len(values)-1]+1) // Ensure the last boundary includes the max value

	// Remove duplicate boundaries
	uniqueBoundaries := []float64{}
	prev := math.Inf(-1)
	for _, b := range boundaries {
		if b != prev {
			uniqueBoundaries = append(uniqueBoundaries, b)
			prev = b
		}
	}
	boundaries = uniqueBoundaries

	log.Printf("Determined bucket boundaries: %v\n", boundaries)

	// Prepare buckets
	buckets := []Bucket{}
	for i := 0; i < len(boundaries)-1; i++ {
		label := fmt.Sprintf("[%v, %v)", boundaries[i], boundaries[i+1])
		buckets = append(buckets, Bucket{
			Label:        label,
			Docs:         []map[string]interface{}{},
			Aggregations: make(map[string]interface{}),
		})
	}

	// Assign documents to buckets
	for _, doc := range input {
		value, exists := doc[groupBy]
		if !exists {
			continue
		}

		numericValue, ok := toFloat64(value)
		if !ok {
			// Unsupported type for groupBy
			continue
		}

		// Find the appropriate bucket
		for i := 0; i < len(boundaries)-1; i++ {
			lower := boundaries[i]
			upper := boundaries[i+1]
			if i == len(boundaries)-2 {
				// Include the upper boundary in the last bucket
				if numericValue >= lower && numericValue <= upper {
					buckets[i].Docs = append(buckets[i].Docs, doc)
					log.Printf("Assigned document %v to bucket %d: %v\n", doc["_id"], i, buckets[i].Label)
					break
				}
			} else {
				if numericValue >= lower && numericValue < upper {
					buckets[i].Docs = append(buckets[i].Docs, doc)
					log.Printf("Assigned document %v to bucket %d: %v\n", doc["_id"], i, buckets[i].Label)
					break
				}
			}
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
							if fieldStr, ok := field.(string); ok && fieldStr == "1" {
								// Count documents
								result[key] = len(bucket.Docs)
							} else {
								// Sum specific field
								fieldName, ok := field.(string)
								if !ok {
									return nil, fmt.Errorf("$sum operator requires a string field name or '1'")
								}
								sum := 0.0
								for _, doc := range bucket.Docs {
									val, exists := doc[fieldName]
									if !exists {
										continue
									}
									num, ok := toFloat64(val)
									if !ok {
										continue
									}
									sum += num
								}
								result[key] = sum
							}
						case "$avg":
							// Calculate average of a specific field
							fieldName, ok := field.(string)
							if !ok {
								return nil, fmt.Errorf("$avg operator requires a string field name")
							}
							sum := 0.0
							count := 0
							for _, doc := range bucket.Docs {
								val, exists := doc[fieldName]
								if !exists {
									continue
								}
								num, ok := toFloat64(val)
								if !ok {
									continue
								}
								sum += num
								count++
							}
							if count > 0 {
								result[key] = sum / float64(count)
							} else {
								result[key] = nil
							}
						case "$max":
							// Find maximum value of a specific field
							fieldName, ok := field.(string)
							if !ok {
								return nil, fmt.Errorf("$max operator requires a string field name")
							}
							var maxVal float64
							first := true
							for _, doc := range bucket.Docs {
								val, exists := doc[fieldName]
								if !exists {
									continue
								}
								num, ok := toFloat64(val)
								if !ok {
									continue
								}
								if first || num > maxVal {
									maxVal = num
									first = false
								}
							}
							if !first {
								result[key] = maxVal
							} else {
								result[key] = nil
							}
						case "$min":
							// Find minimum value of a specific field
							fieldName, ok := field.(string)
							if !ok {
								return nil, fmt.Errorf("$min operator requires a string field name")
							}
							var minVal float64
							first := true
							for _, doc := range bucket.Docs {
								val, exists := doc[fieldName]
								if !exists {
									continue
								}
								num, ok := toFloat64(val)
								if !ok {
									continue
								}
								if first || num < minVal {
									minVal = num
									first = false
								}
							}
							if !first {
								result[key] = minVal
							} else {
								result[key] = nil
							}
						default:
							return nil, fmt.Errorf("unsupported aggregation operator in $bucketAuto output: %s", op)
						}
					}
				default:
					return nil, fmt.Errorf("$bucketAuto stage 'output' must be an object")
				}
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// validateBucketAutoStage validates the parameters for the $bucketAuto stage.
func (db *DB) validateBucketAutoStage(params map[string]interface{}) error {
	// Check 'groupBy'
	groupByRaw, ok := params["groupBy"].(string)
	if !ok || groupByRaw == "" {
		return fmt.Errorf("$bucketAuto stage requires a non-empty string 'groupBy' field")
	}
	groupBy := cleanGroupByField(groupByRaw)
	if groupBy == "" {
		return fmt.Errorf("$bucketAuto stage 'groupBy' cannot be empty after cleaning")
	}

	// Check 'buckets'
	buckets, ok := params["buckets"]
	if !ok {
		return fmt.Errorf("$bucketAuto stage requires a 'buckets' field")
	}
	numBuckets, ok := toFloat64(buckets)
	if !ok {
		return fmt.Errorf("$bucketAuto stage 'buckets' must be a number")
	}
	numBucketsInt := int(numBuckets)
	if numBucketsInt <= 0 {
		return fmt.Errorf("$bucketAuto stage 'buckets' must be greater than 0")
	}

	// Validate 'output' if present
	if output, ok := params["output"]; ok {
		outputMap, ok := output.(map[string]interface{})
		if !ok {
			return fmt.Errorf("$bucketAuto stage 'output' must be an object")
		}
		for _, expr := range outputMap {
			exprMap, ok := expr.(map[string]interface{})
			if !ok {
				return fmt.Errorf("$bucketAuto stage 'output' expressions must be objects")
			}
			for op, field := range exprMap {
				switch op {
				case "$sum", "$avg", "$max", "$min":
					if op == "$sum" {
						// $sum can be "1" or a field name
						if fieldStr, ok := field.(string); !ok || (fieldStr != "1" && fieldStr == "") {
							// Must be a field name if not "1"
							if _, ok := field.(string); !ok {
								return fmt.Errorf("$sum operator requires a string field name or '1'")
							}
						}
					} else {
						// Other operators require a field name
						if _, ok := field.(string); !ok {
							return fmt.Errorf("%s operator requires a string field name", op)
						}
					}
				default:
					return fmt.Errorf("unsupported aggregation operator in $bucketAuto output: %s", op)
				}
			}
		}
	}

	return nil
}
