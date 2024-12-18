package marco

import (
	"fmt"
	"log"
	"math"
	"reflect"
	"regexp"
	"strings"
)

// matchStage filters documents based on specified criteria.
func (db *DB) matchStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	var results []map[string]interface{}
	for _, doc := range input {
		if evaluateMatchExpression(doc, params) {
			results = append(results, doc)
		}
	}
	return results
}

// evaluateMatchExpression is the central expression-evaluation function for $match queries.
// It recursively processes logical operators ($and, $or, $nor) and field-based conditions
// (like {"field": {"$gt": 10}}).
func evaluateMatchExpression(doc map[string]interface{}, expr interface{}) bool {
	switch condition := expr.(type) {
	case map[string]interface{}:
		// Could be a top-level object like {field: condition} or {$and: [...]} or similar.
		for key, val := range condition {
			switch key {
			case "$and":
				andClauses, ok := val.([]interface{})
				if !ok {
					return false
				}
				// All must match
				for _, clause := range andClauses {
					if !evaluateMatchExpression(doc, clause) {
						return false
					}
				}
				return true

			case "$or":
				orClauses, ok := val.([]interface{})
				if !ok {
					return false
				}
				// Any must match
				for _, clause := range orClauses {
					if evaluateMatchExpression(doc, clause) {
						return true
					}
				}
				return false

			case "$nor":
				norClauses, ok := val.([]interface{})
				if !ok {
					return false
				}
				// All must fail
				for _, clause := range norClauses {
					if evaluateMatchExpression(doc, clause) {
						// If any clause matches, $nor fails
						return false
					}
				}
				return true

			default:
				// Treat 'key' as a field name or nested path
				docVal, fieldExists := getNestedFieldExists(doc, key)

				// If val is a map, might contain operators like $gt, $lt, etc.
				opMap, isMap := val.(map[string]interface{})
				if isMap {
					if !evaluateOperators(docVal, fieldExists, opMap) {
						return false
					}
				} else {
					// Direct equality
					if !reflect.DeepEqual(docVal, val) {
						return false
					}
				}
			}
		}
		return true

	case []interface{}:
		// Potentially an array of conditions?
		// Usually $match expressions at top-level aren't arrays except for $and/$or.
		// If needed, treat them as a $and? This is not standard, but you could interpret it if you wish.
		for _, clause := range condition {
			if !evaluateMatchExpression(doc, clause) {
				return false
			}
		}
		return true

	default:
		// Unrecognized expression type
		return false
	}
}

// evaluateOperators checks individual field-level operators like $gt, $lt, $eq, $regex, etc.
// If multiple operators exist on the same field, they all must pass.
func evaluateOperators(value interface{}, valueExists bool, operators map[string]interface{}) bool {
	for opKey, opVal := range operators {
		switch opKey {

		// ---------- Logical Inversions ----------

		case "$not":
			// $not expects an operator expression or direct condition inside
			nestedMap, ok := opVal.(map[string]interface{})
			if ok {
				// If evaluateOperators is true for nested, we invert it
				if evaluateOperators(value, valueExists, nestedMap) {
					return false
				}
			} else {
				// If it's a direct value (like a regex), we interpret in a simplified way.
				// For instance: { field: { $not: /pattern/ } }
				if !handleRegexNot(value, opVal) {
					// handleRegexNot returns 'true' if it matched => $not fails
					return false
				}
			}

		// ---------- Array Operators ----------
		case "$elemMatch":
			// Element match for arrays
			arr, ok := value.([]interface{})
			elemCriteria, critOk := opVal.(map[string]interface{})
			if !ok || !critOk {
				return false
			}
			// Check if any element in the array matches the criteria
			matchFound := false
			for _, elem := range arr {
				elemMap, isMap := elem.(map[string]interface{})
				if !isMap {
					continue
				}
				if evaluateMatchExpression(elemMap, elemCriteria) {
					matchFound = true
					break
				}
			}
			if !matchFound {
				return false
			}

		case "$all":
			// All elements must be present in the array
			arr, ok := value.([]interface{})
			requiredEls, critOk := opVal.([]interface{})
			if !ok || !critOk {
				return false
			}
			for _, requiredEl := range requiredEls {
				found := false
				for _, arrEl := range arr {
					if reflect.DeepEqual(arrEl, requiredEl) {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}

		case "$size":
			// Check array length
			arr, ok := value.([]interface{})
			size, sizeOk := opVal.(float64)
			if !ok || !sizeOk {
				return false
			}
			if float64(len(arr)) != size {
				return false
			}

		// ---------- Regex Operator ----------
	
		case "$regex":
			// Process $regex along with its associated $options
			if !regexMatch(value, opVal, operators) {
				return false
			}
			// Continue to skip processing $options separately
			continue

		case "$options":
			// $options is handled within $regex; skip it
			continue

			
		// ---------- Comparison Operators ----------

		case "$eq":
			if !eqOperator(value, opVal) {
				return false
			}

		case "$ne":
			if eqOperator(value, opVal) {
				return false
			}

		case "$gt":
			valNum, okVal := toFloat64(value)
			opNum, okOp := toFloat64(opVal)
			if !okVal || !okOp || !(valNum > opNum) {
				return false
			}

		case "$gte":
			valNum, okVal := toFloat64(value)
			opNum, okOp := toFloat64(opVal)
			if !okVal || !okOp || !(valNum >= opNum) {
				return false
			}

		case "$lt":
			valNum, okVal := toFloat64(value)
			opNum, okOp := toFloat64(opVal)
			if !okVal || !okOp || !(valNum < opNum) {
				return false
			}

		case "$lte":
			valNum, okVal := toFloat64(value)
			opNum, okOp := toFloat64(opVal)
			if !okVal || !okOp || !(valNum <= opNum) {
				return false
			}

		case "$in":
			arr, ok := opVal.([]interface{})
			if !ok {
				return false
			}
			found := false
			for _, item := range arr {
				if reflect.DeepEqual(value, item) {
					found = true
					break
				}
			}
			if !found {
				return false
			}

		case "$nin":
			arr, ok := opVal.([]interface{})
			if !ok {
				return false
			}
			for _, item := range arr {
				if reflect.DeepEqual(value, item) {
					return false
				}
			}

		case "$exists":
			expectExists, ok := opVal.(bool)
			if !ok {
				return false
			}
			if expectExists && !valueExists {
				return false
			}
			if !expectExists && valueExists {
				return false
			}

		case "$type":
			typeStr, ok := opVal.(string)
			if !ok {
				return false
			}
			if !matchesType(value, typeStr) {
				return false
			}

		case "$mod":
			// $mod takes [divisor, remainder], for numeric fields
			arr, ok := opVal.([]interface{})
			if !ok || len(arr) != 2 {
				return false
			}
			divisor, ok1 := toFloat64(arr[0])
			remainder, ok2 := toFloat64(arr[1])
			if !ok1 || !ok2 {
				return false
			}
			valNum, okVal := toFloat64(value)
			if !okVal {
				return false
			}
			if math.Mod(valNum, divisor) != remainder {
				return false
			}

		case "$expr":
			// Full $expr support requires an expression parser (like in $project).
			// For now, we do a simple placeholder log message:
			log.Println("Warning: $expr is not fully implemented in $match.")
			return false

		default:
			log.Printf("Operator %s not recognized", opKey)
			return false
		}
	}
	return true
}

// eqOperator handles equality with a little extra logic for strings, etc.
func eqOperator(value interface{}, opVal interface{}) bool {
	// Trim strings if desired, or do exact match. Here we'll do a direct DeepEqual match, same as Mongo's basic ==.
	return reflect.DeepEqual(value, opVal)
}

// handleRegexNot is a helper for $not with direct regex usage.
func handleRegexNot(value interface{}, pattern interface{}) bool {
	// Return true if it matches (so the calling code can invert it).
	strVal, okVal := value.(string)
	patStr, okPat := pattern.(string)
	if !okVal || !okPat {
		return false // can't match
	}
	matched, err := regexp.MatchString(patStr, strVal)
	if err != nil {
		return false
	}
	return matched
}

// regexMatch applies $regex and optional $options on 'value'.
func regexMatch(value interface{}, opVal interface{}, operators map[string]interface{}) bool {
	str, ok := value.(string)
	if !ok {
		return false
	}
	pattern, ok := opVal.(string)
	if !ok {
		return false
	}

	// Optional case-insensitive flag
	if caseInsensitive, exists := operators["$options"].(string); exists && strings.Contains(caseInsensitive, "i") {
		pattern = "(?i)" + pattern
	}

	match, err := regexp.MatchString(pattern, str)
	if err != nil {
		return false
	}
	return match
}

// matchesType checks if 'value' has the specified MongoDB type string (e.g., "string", "number", "bool").
func matchesType(value interface{}, typeStr string) bool {
	// reflect.TypeOf(value).Kind().String() => e.g. "float64", "string", "bool", "slice", "map"
	if value == nil {
		// In MongoDB, there's "null" type as well.
		return typeStr == "null"
	}
	actualKind := reflect.TypeOf(value).Kind()
	switch typeStr {
	case "number":
		// Treat float64 or any numeric as 'number'
		return actualKind == reflect.Float64 || isIntegerKind(actualKind)
	case "string":
		return actualKind == reflect.String
	case "bool":
		return actualKind == reflect.Bool
	case "array":
		return actualKind == reflect.Slice
	case "object":
		return actualKind == reflect.Map
	case "null":
		return value == nil
	// Add more as needed, e.g., "date"
	default:
		return false
	}
}

// isIntegerKind checks if kind is an integer type (int, int32, int64, etc.).
func isIntegerKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

func (db *DB) validateMatchStage(params map[string]interface{}) error {

	// If the user wrote `$match: {}`, that might be valid as a no-op, or you might want to forbid it:
	if len(params) == 0 {
		return fmt.Errorf("$match stage must not be empty")
	}

	for field, val := range params {
		// Check if field is a logical operator like $or, $and, $nor at the top-level
		if field == "$or" || field == "$and" || field == "$nor" {
			// $or / $and / $nor expects an array of sub-conditions
			arr, ok := val.([]interface{})
			if !ok {
				return fmt.Errorf("$match operator %q expects an array, got %T", field, val)
			}
			if len(arr) == 0 {
				return fmt.Errorf("$match operator %q array must not be empty", field)
			}

			// Recursively validate each sub-condition in the array
			for i, cond := range arr {
				_, isMap := cond.(map[string]interface{})
				if !isMap {
					return fmt.Errorf("$match operator %q element #%d is not an object, got %T", field, i, cond)
				}
				// Potentially validate each sub-field in condMap (e.g. "status", "age", etc.).
				// For example, you could call a helper function `validateMatchSubCondition(condMap)`.
			}

		} else {
			// Not a top-level logical operator like $or / $and / $nor
			// => interpret `field` as the actual field name, and `val` as either
			// a direct scalar (e.g. field: "active") or an operator map (e.g. field: {"$gt": 30})
			switch valTyped := val.(type) {
			case map[string]interface{}:
				// Check each sub-operator (e.g. $gt, $lt, $eq, etc.)
				for op := range valTyped {
					if !isValidMatchOperator(op) {
						return fmt.Errorf("$match has invalid operator %q for field %q", op, field)
					}
				}
			case string, float64, int, bool:
				// scalar is okay, e.g. "status": "active"
			default:
				// The error that triggered your message:
				// "Error parsing aggregation stages: $match field "$or" has unexpected type []interface {}"
				// Instead of failing, we now specifically handle $or etc. as arrays above.
				return fmt.Errorf("$match field %q has unexpected type %T", field, val)
			}
		}
	}
	return nil

}
