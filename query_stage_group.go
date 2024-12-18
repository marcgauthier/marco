package marco

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
)

// getNestedField(doc, fieldName) -> interface{}
// toFloat64(val) -> (float64, bool)
// Both are assumed to be defined elsewhere, just like in your original code.

// groupStage implements a MongoDB-like group aggregation operation for in-memory document processing.
// It allows grouping documents by a specified key and applying various aggregation operations.
//
// Extended to support these additional operators:
// - $addToSet       (collect unique values into an array)
// - $stdDevPop      (population standard deviation)
// - $stdDevSamp     (sample standard deviation)
// - $mergeObjects   (merge multiple objects into a single object)
// - $accumulator    (placeholder for custom JS-based accumulators)
// - $count          (count the number of documents, alternative to { $sum: 1 })
// - $arrayToObject  (convert an array of [k,v] pairs into an object; placeholder usage)
// - $maxN           (top N values)
// - $minN           (bottom N values)
// - $firstN         (first N values in the original order)
// - $lastN          (last N values in the original order)
//
// Existing operators were:
// - $sum, $avg, $max, $min, $push, $first, $last
//
// Adjust or refine as needed for your use case.

func (db *DB) groupStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	groups := make(map[interface{}][]map[string]interface{})
	aggExpressions := make(map[string]map[string]interface{})
	var groupIDField string

	// Process grouping and aggregation parameters
	for k, v := range params {
		switch k {
		case "_id":
			if idStr, ok := v.(string); ok && strings.HasPrefix(idStr, "$") {
				groupIDField = strings.TrimPrefix(idStr, "$")
			}
		default:
			// Store aggregation expressions for later processing
			if expr, ok := v.(map[string]interface{}); ok {
				aggExpressions[k] = expr
			}
		}
	}

	// Group documents by the specified field
	for _, doc := range input {
		groupValue := doc[groupIDField]
		groups[groupValue] = append(groups[groupValue], doc)
	}

	// Process and aggregate grouped documents
	var results []map[string]interface{}
	for groupValue, groupDocs := range groups {
		groupResult := map[string]interface{}{"_id": groupValue}

		for fieldName, expr := range aggExpressions {
			for op, val := range expr {
				switch op {
				// Existing operators
				case "$sum":
					groupResult[fieldName] = calculateSum(groupDocs, val)
				case "$avg":
					groupResult[fieldName] = calculateAverage(groupDocs, val)
				case "$max":
					groupResult[fieldName] = calculateMax(groupDocs, val)
				case "$min":
					groupResult[fieldName] = calculateMin(groupDocs, val)
				case "$push":
					groupResult[fieldName] = collectValues(groupDocs, val)
				case "$first":
					groupResult[fieldName] = selectFirst(groupDocs, val)
				case "$last":
					groupResult[fieldName] = selectLast(groupDocs, val)

				// New operators
				case "$addToSet":
					groupResult[fieldName] = addToSet(groupDocs, val)
				case "$stdDevPop":
					groupResult[fieldName] = calculateStdDev(groupDocs, val, true)
				case "$stdDevSamp":
					groupResult[fieldName] = calculateStdDev(groupDocs, val, false)
				case "$mergeObjects":
					groupResult[fieldName] = mergeObjects(groupDocs, val)
				case "$accumulator":
					groupResult[fieldName] = runAccumulator( /*not implemented yet: groupDocs, val*/ )
				case "$count":
					groupResult[fieldName] = float64(len(groupDocs))
				case "$arrayToObject":
					groupResult[fieldName] = arrayToObject(groupDocs, val)
				case "$maxN":
					groupResult[fieldName] = maxN(groupDocs, val)
				case "$minN":
					groupResult[fieldName] = minN(groupDocs, val)
				case "$firstN":
					groupResult[fieldName] = firstN(groupDocs, val)
				case "$lastN":
					groupResult[fieldName] = lastN(groupDocs, val)

				default:
					log.Printf("Aggregator %s not implemented", op)
				}
			}
		}

		results = append(results, groupResult)
	}

	return results
}

//------------------------------------------------------------------------------
// Existing aggregator helpers
//------------------------------------------------------------------------------

func calculateSum(docs []map[string]interface{}, val interface{}) float64 {
	var sum float64
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		// Sum values of a specific field
		fieldToSum := strings.TrimPrefix(valStr, "$")
		for _, doc := range docs {
			if number, ok := toFloat64(getNestedField(doc, fieldToSum)); ok {
				sum += number
			}
		}
	} else if floatVal, ok := toFloat64(val); ok {
		// e.g. { $sum: 1 } for count of docs
		sum = float64(len(docs)) * floatVal
	}
	return sum
}

func calculateMax(docs []map[string]interface{}, val interface{}) float64 {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		fieldToMax := strings.TrimPrefix(valStr, "$")
		var maxVal float64
		first := true
		for _, doc := range docs {
			if number, ok := toFloat64(getNestedField(doc, fieldToMax)); ok {
				if first || number > maxVal {
					maxVal = number
					first = false
				}
			}
		}
		if !first {
			return maxVal
		}
	}
	return 0
}

func calculateMin(docs []map[string]interface{}, val interface{}) float64 {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		fieldToMin := strings.TrimPrefix(valStr, "$")
		var minVal float64
		first := true
		for _, doc := range docs {
			if number, ok := toFloat64(getNestedField(doc, fieldToMin)); ok {
				if first || number < minVal {
					minVal = number
					first = false
				}
			}
		}
		if !first {
			return minVal
		}
	}
	return 0
}

func calculateAverage(docs []map[string]interface{}, val interface{}) float64 {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		fieldToAvg := strings.TrimPrefix(valStr, "$")
		var sum float64
		count := 0
		for _, doc := range docs {
			nestedValue := getNestedField(doc, fieldToAvg)
			if number, ok := toFloat64(nestedValue); ok {
				sum += number
				count++
			}
		}
		if count > 0 {
			return sum / float64(count)
		}
	}
	return 0
}

func collectValues(docs []map[string]interface{}, val interface{}) []interface{} {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		fieldToPush := strings.TrimPrefix(valStr, "$")
		var pushArray []interface{}
		for _, doc := range docs {
			v := getNestedField(doc, fieldToPush)
			if v != nil {
				pushArray = append(pushArray, v)
			}
		}
		return pushArray
	}
	return nil
}

func selectFirst(docs []map[string]interface{}, val interface{}) interface{} {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		fieldToFirst := strings.TrimPrefix(valStr, "$")
		if len(docs) > 0 {
			return getNestedField(docs[0], fieldToFirst)
		}
	}
	return nil
}

func selectLast(docs []map[string]interface{}, val interface{}) interface{} {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		fieldToLast := strings.TrimPrefix(valStr, "$")
		if len(docs) > 0 {
			return getNestedField(docs[len(docs)-1], fieldToLast)
		}
	}
	return nil
}

//------------------------------------------------------------------------------
// New aggregator helpers
//------------------------------------------------------------------------------

// $addToSet: Collects unique values of a field into an array.
func addToSet(docs []map[string]interface{}, val interface{}) []interface{} {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		field := strings.TrimPrefix(valStr, "$")
		uniqueMap := make(map[interface{}]struct{})
		for _, doc := range docs {
			v := getNestedField(doc, field)
			if v != nil {
				uniqueMap[v] = struct{}{}
			}
		}
		result := make([]interface{}, 0, len(uniqueMap))
		for k := range uniqueMap {
			result = append(result, k)
		}
		return result
	}
	return nil
}

// $stdDevPop / $stdDevSamp: Standard deviation (population vs sample).
func calculateStdDev(docs []map[string]interface{}, val interface{}, population bool) float64 {
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		field := strings.TrimPrefix(valStr, "$")
		var values []float64
		for _, doc := range docs {
			if number, ok := toFloat64(getNestedField(doc, field)); ok {
				values = append(values, number)
			}
		}
		n := float64(len(values))
		if n == 0 {
			return 0
		}
		// Calculate mean
		var sum float64
		for _, v := range values {
			sum += v
		}
		mean := sum / n

		// Calculate variance
		var variance float64
		for _, v := range values {
			diff := v - mean
			variance += diff * diff
		}
		if population {
			variance = variance / n
		} else if n > 1 {
			variance = variance / (n - 1)
		}
		return math.Sqrt(variance)
	}
	return 0
}

// $mergeObjects: Merge multiple object fields. Simplified top-level merge only.
func mergeObjects(docs []map[string]interface{}, val interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		field := strings.TrimPrefix(valStr, "$")
		for _, doc := range docs {
			obj, _ := getNestedField(doc, field).(map[string]interface{})
			for k, v := range obj {
				merged[k] = v
			}
		}
	}
	return merged
}

// $accumulator: Placeholder for user-defined logic in MongoDB (JavaScript).
func runAccumulator( /*docs []map[string]interface{}, val interface{}*/ ) interface{} {
	// Real MongoDB uses JavaScript to define init, accumulate, merge, finalize, etc.
	// Placeholder: just return a message or a simple sum.
	// If `val` is a map with "accumulateArgs", "init", etc., you could manually interpret them.
	log.Println("Warning: $accumulator aggregator is not fully implemented.")
	return nil
}

// $count: (already handled by default: groupResult[fieldName] = float64(len(groupDocs)) )

// $arrayToObject: Convert an array of [key, value] pairs into a single object. (Placeholder example)
func arrayToObject(docs []map[string]interface{}, val interface{}) interface{} {
	// In real Mongo usage, $arrayToObject often is used inside $push or other pipelines.
	// We'll assume { $arrayToObject: "$someField" } refers to an array in each doc.
	if valStr, ok := val.(string); ok && strings.HasPrefix(valStr, "$") {
		field := strings.TrimPrefix(valStr, "$")
		// We'll only convert the first doc's array as an example.
		if len(docs) > 0 {
			arr, _ := getNestedField(docs[0], field).([]interface{})
			obj := make(map[string]interface{})
			// Expect array in form: [ [k1, v1], [k2, v2], ... ]
			for _, pair := range arr {
				if kv, ok := pair.([]interface{}); ok && len(kv) == 2 {
					keyStr, _ := kv[0].(string)
					obj[keyStr] = kv[1]
				}
			}
			return obj
		}
	}
	return nil
}

// $maxN: Return top N numeric values from the group.
func maxN(docs []map[string]interface{}, val interface{}) []float64 {
	// val should be an object: { n: <int>, input: "$field" }
	params, _ := val.(map[string]interface{})
	nVal, _ := toFloat64(params["n"])
	inputStr, _ := params["input"].(string)
	n := int(nVal)

	if !strings.HasPrefix(inputStr, "$") || n < 1 {
		return nil
	}
	field := strings.TrimPrefix(inputStr, "$")

	// Collect all numeric values
	var allVals []float64
	for _, doc := range docs {
		if number, ok := toFloat64(getNestedField(doc, field)); ok {
			allVals = append(allVals, number)
		}
	}
	// Sort descending
	sort.Slice(allVals, func(i, j int) bool {
		return allVals[i] > allVals[j]
	})
	if len(allVals) > n {
		return allVals[:n]
	}
	return allVals
}

// $minN: Return bottom N numeric values from the group.
func minN(docs []map[string]interface{}, val interface{}) []float64 {
	// val should be an object: { n: <int>, input: "$field" }
	params, _ := val.(map[string]interface{})
	nVal, _ := toFloat64(params["n"])
	inputStr, _ := params["input"].(string)
	n := int(nVal)

	if !strings.HasPrefix(inputStr, "$") || n < 1 {
		return nil
	}
	field := strings.TrimPrefix(inputStr, "$")

	// Collect all numeric values
	var allVals []float64
	for _, doc := range docs {
		if number, ok := toFloat64(getNestedField(doc, field)); ok {
			allVals = append(allVals, number)
		}
	}
	// Sort ascending
	sort.Slice(allVals, func(i, j int) bool {
		return allVals[i] < allVals[j]
	})
	if len(allVals) > n {
		return allVals[:n]
	}
	return allVals
}

// $firstN: Return the first N values (in input order).
func firstN(docs []map[string]interface{}, val interface{}) []interface{} {
	// val should be an object: { n: <int>, input: "$field" }
	params, _ := val.(map[string]interface{})
	nVal, _ := toFloat64(params["n"])
	inputStr, _ := params["input"].(string)
	n := int(nVal)

	if !strings.HasPrefix(inputStr, "$") || n < 1 {
		return nil
	}
	field := strings.TrimPrefix(inputStr, "$")

	var result []interface{}
	count := 0
	for _, doc := range docs {
		if count >= n {
			break
		}
		v := getNestedField(doc, field)
		if v != nil {
			result = append(result, v)
			count++
		}
	}
	return result
}

// $lastN: Return the last N values (in input order).
func lastN(docs []map[string]interface{}, val interface{}) []interface{} {
	// val should be an object: { n: <int>, input: "$field" }
	params, _ := val.(map[string]interface{})
	nVal, _ := toFloat64(params["n"])
	inputStr, _ := params["input"].(string)
	n := int(nVal)

	if !strings.HasPrefix(inputStr, "$") || n < 1 {
		return nil
	}
	field := strings.TrimPrefix(inputStr, "$")

	var allVals []interface{}
	for _, doc := range docs {
		v := getNestedField(doc, field)
		if v != nil {
			allVals = append(allVals, v)
		}
	}
	// Return the last N elements
	size := len(allVals)
	if size > n {
		return allVals[size-n:]
	}
	return allVals
}

func (db *DB) validateGroupStage(params map[string]interface{}) error {

	// By MongoDB spec, $group must have an _id and then aggregations
	// like sum, avg, push, etc. stored in other keys.
	if _, ok := params["_id"]; !ok {
		return fmt.Errorf("$group is missing required field: _id")
	}
	// Optionally validate each aggregator function
	for field, aggValue := range params {
		if field == "_id" {
			continue
		}
		switch v := aggValue.(type) {
		case map[string]interface{}:
			// e.g. { "$sum": "$someField" }, { "$avg": ... }, etc.
			for op := range v {
				if !isValidGroupOperator(op) {
					return fmt.Errorf("$group aggregator %q is not supported", op)
				}
			}
		default:
			return fmt.Errorf("$group field %q must be an aggregator object, got %T", field, v)
		}
	}
	return nil

}
