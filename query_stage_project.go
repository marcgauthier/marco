package marco

import (
	"fmt"
	"log"
	"math"
	"strings"
)

// projectStage implements a more complete MongoDB-like $project stage.
//
// Key changes / enhancements over the original code:
// 1. Detect whether the projection is in inclusion or exclusion mode by scanning numeric fields (1 or 0).
// 2. Evaluate every field's value as an expression (even simple string references).
// 3. Process more operators in a generic, recursive expression evaluator.
// 4. Respect _id default inclusion/exclusion rules.
//
// If the user mixes 1 and 0 in the same projection (and it's not just `_id`), we log a warning or error
// to mimic MongoDB's general restriction.
func (db *DB) projectStage(input []map[string]interface{}, params map[string]interface{}) []map[string]interface{} {
	// 1. Determine inclusion or exclusion mode.
	//    In MongoDB, if ANY field is "1" (true), we treat the projection as "include mode" except _id might be explicit.
	//    If ALL numeric fields are "0", it's "exclude mode".
	//    Mixing 1 and 0 in the same doc is invalid except for _id.
	mode, err := determineProjectionMode(params)
	if err != nil {
		log.Printf("Projection error: %v", err)
		// Return original docs or handle error as you wish.
		return input
	}

	var results []map[string]interface{}
	for _, doc := range input {
		// Start by copying the document or building from scratch depending on mode
		var projectedDoc map[string]interface{}

		if mode == "include" {
			// In "include" mode, we start with an empty doc
			projectedDoc = make(map[string]interface{})
		} else {
			// In "exclude" mode, we start with a shallow copy of the entire doc
			// Then we'll remove fields that are explicitly excluded
			projectedDoc = cloneDocument(doc)
		}

		for field, rawSpec := range params {
			// `_id` has special default handling, but we'll treat it just like any other field
			// except we allow mixing 1 or 0 with `_id`.
			switch spec := rawSpec.(type) {
			case float64:
				// Projection spec is numeric, i.e. 1 or 0
				if spec == 1 && mode == "include" {
					// Evaluate expression as a direct field reference or handle deeper logic:
					//   In a "pure" numeric projection, the *field name* itself is used to fetch the doc field.
					//   But let's allow for expression references in the future if needed.
					if val, exists := doc[field]; exists {
						projectedDoc[field] = val
					}
				} else if spec == 0 && mode == "exclude" {
					// Exclude this field from projected doc (only if it exists)
					delete(projectedDoc, field)
				}
				// If spec=1 but we're in exclude mode, or spec=0 in include mode, that was flagged earlier as invalid
				// (except for _id). So no action needed here, we effectively ignore or skip it.
			default:
				// For anything that's not a numeric spec (1/0), treat it as an expression
				// Evaluate the expression and place it into the projected doc.
				value := evaluateExpression(doc, rawSpec)
				projectedDoc[field] = value
			}
		}

		// If _id is not mentioned in the params at all, but we're in "include" mode,
		// we default to including _id. If we're in "exclude" mode and `_id` wasn't explicitly set to 1,
		// then `_id` remains part of the doc (since exclude mode started with a full copy).
		// That logic effectively matches MongoDB.
		if _, exists := params["_id"]; !exists && mode == "include" {
			// In "include" mode, we didn't explicitly mention _id, so let's add it if present
			if val, ok := doc["_id"]; ok {
				projectedDoc["_id"] = val
			}
		}

		results = append(results, projectedDoc)
	}

	return results
}

// determineProjectionMode scans the params for numeric (1/0) fields
// and decides if the projection is "include" or "exclude".
// If there's a mix of 1 and 0 on fields other than _id, that is invalid in MongoDB.
func determineProjectionMode(params map[string]interface{}) (string, error) {
	hasInclusion := false
	hasExclusion := false

	for field, raw := range params {
		spec, ok := raw.(float64)
		if !ok {
			// Not a numeric projection field; skip
			continue
		}
		if field == "_id" {
			// Mixing _id is allowed, skip
			continue
		}
		if spec == 1 {
			hasInclusion = true
		} else if spec == 0 {
			hasExclusion = true
		}

		if hasInclusion && hasExclusion {
			return "", fmt.Errorf("cannot mix inclusion and exclusion in the same projection except for _id")
		}
	}

	if hasInclusion {
		return "include", nil
	}
	// If there's any numeric field and all are 0 (or none are numeric), treat it as exclude
	return "exclude", nil
}

// evaluateExpression tries to parse and evaluate the given 'expr' as either:
// 1) A literal value (string/number/bool).
// 2) A field reference (string starting with '$').
// 3) An operator expression (map with keys like $concat, $add, $cond, etc.).
// 4) A numeric projection is handled outside this function, so we won't see float64==1 or float64==0 here.
func evaluateExpression(doc map[string]interface{}, expr interface{}) interface{} {
	switch val := expr.(type) {
	case string:
		// Check if it's a $field reference
		if strings.HasPrefix(val, "$") {
			return resolveField(doc, strings.TrimPrefix(val, "$"))
		}
		// Otherwise it's just a literal string
		return val

	case float64, bool, int, nil:
		// Basic literal
		return val

	case map[string]interface{}:
		// Potentially an operator expression like { $concat: [...] } or { $add: [...] }
		// We'll parse the first key to see what operator it is.
		// If multiple keys exist, the first one is the primary operator (like Mongo does).
		for op, opVal := range val {
			switch op {
			case "$concat":
				return handleConcat(doc, opVal)
			case "$substr":
				return handleSubstring(doc, opVal)
			case "$dateToString":
				return handleDateToString(doc, opVal)
			case "$add":
				return handleAdd(doc, opVal)
			case "$subtract":
				return handleSubtract(doc, opVal)
			case "$multiply":
				return handleMultiply(doc, opVal)
			case "$divide":
				return handleDivide(doc, opVal)
			case "$mod":
				return handleMod(doc, opVal)
			case "$and":
				return handleAnd(doc, opVal)
			case "$or":
				return handleOr(doc, opVal)
			case "$not":
				return handleNot(doc, opVal)
			case "$cond":
				return handleCond(doc, opVal)
			// Add additional operators here as needed
			default:
				log.Printf("Unhandled operator: %s", op)
				return nil
			}
		}
		return nil

	case []interface{}:
		// Could be an array literal, or an expression array
		// For a direct array, we evaluate each element
		resultArr := make([]interface{}, 0, len(val))
		for _, item := range val {
			resultArr = append(resultArr, evaluateExpression(doc, item))
		}
		return resultArr

	default:
		// Unhandled type
		log.Printf("Unhandled expression type: %T", expr)
		return nil
	}
}

// ---------- Basic Operator Implementations ----------

// handleConcat expects opVal = []interface{}, each item is either a literal or a $field reference
func handleConcat(doc map[string]interface{}, opVal interface{}) string {
	arr, ok := opVal.([]interface{})
	if !ok {
		return ""
	}

	var sb strings.Builder
	for _, item := range arr {
		resolved := evaluateExpression(doc, item)
		if resolvedStr, isString := resolved.(string); isString {
			sb.WriteString(resolvedStr)
		}
	}
	return sb.String()
}

// handleSubstring expects opVal = [ <string expression>, <start>, <length> ]
func handleSubstring(doc map[string]interface{}, opVal interface{}) string {
	arr, ok := opVal.([]interface{})
	if !ok || len(arr) != 3 {
		return ""
	}

	strVal := evaluateExpression(doc, arr[0])
	startVal := evaluateExpression(doc, arr[1])
	lengthVal := evaluateExpression(doc, arr[2])

	s, _ := strVal.(string)
	start, _ := toFloat64(startVal)
	length, _ := toFloat64(lengthVal)

	return extractSubstring(s, int(start), int(length))
}

// handleDateToString expects opVal = { "date": <expr>, "format": <formatStr> }
func handleDateToString(doc map[string]interface{}, opVal interface{}) string {
	config, ok := opVal.(map[string]interface{})
	if !ok {
		return ""
	}
	dateRaw := config["date"]
	formatRaw := config["format"]

	dateVal := evaluateExpression(doc, dateRaw) // Might be a $field ref
	formatStr := evaluateExpression(doc, formatRaw)

	format, _ := formatStr.(string)
	return formatDate(dateVal, format)
}

// Arithmetic
func handleAdd(doc map[string]interface{}, opVal interface{}) interface{} {
	// opVal is typically an array: e.g. [ <expr1>, <expr2>, ... ]
	arr, ok := opVal.([]interface{})
	if !ok {
		return nil
	}
	sum := 0.0
	for _, item := range arr {
		val := evaluateExpression(doc, item)
		f, _ := toFloat64(val)
		sum += f
	}
	return sum
}

func handleSubtract(doc map[string]interface{}, opVal interface{}) interface{} {
	arr, ok := opVal.([]interface{})
	if !ok || len(arr) < 2 {
		return nil
	}
	firstVal := evaluateExpression(doc, arr[0])
	base, _ := toFloat64(firstVal)
	for i := 1; i < len(arr); i++ {
		val := evaluateExpression(doc, arr[i])
		f, _ := toFloat64(val)
		base -= f
	}
	return base
}

func handleMultiply(doc map[string]interface{}, opVal interface{}) interface{} {
	arr, ok := opVal.([]interface{})
	if !ok || len(arr) == 0 {
		return nil
	}
	product := 1.0
	for _, item := range arr {
		val := evaluateExpression(doc, item)
		f, _ := toFloat64(val)
		product *= f
	}
	return product
}

func handleDivide(doc map[string]interface{}, opVal interface{}) interface{} {
	arr, ok := opVal.([]interface{})
	if !ok || len(arr) < 2 {
		return nil
	}
	numerator := evaluateExpression(doc, arr[0])
	denom := evaluateExpression(doc, arr[1])

	numf, _ := toFloat64(numerator)
	denf, _ := toFloat64(denom)
	if denf == 0 {
		// Mimic MongoDB’s behavior, which might throw an error or produce NaN
		return nil
	}
	result := numf / denf

	// If there are more items, chain-divide them
	for i := 2; i < len(arr); i++ {
		nextVal := evaluateExpression(doc, arr[i])
		nf, _ := toFloat64(nextVal)
		if nf == 0 {
			return nil
		}
		result /= nf
	}
	return result
}

func handleMod(doc map[string]interface{}, opVal interface{}) interface{} {
	arr, ok := opVal.([]interface{})
	if !ok || len(arr) != 2 {
		return nil
	}
	leftVal := evaluateExpression(doc, arr[0])
	rightVal := evaluateExpression(doc, arr[1])

	lv, _ := toFloat64(leftVal)
	rv, _ := toFloat64(rightVal)
	if rv == 0 {
		return nil
	}
	return math.Mod(lv, rv)
}

// Logical
func handleAnd(doc map[string]interface{}, opVal interface{}) interface{} {
	arr, ok := opVal.([]interface{})
	if !ok {
		return false
	}
	for _, item := range arr {
		val := evaluateExpression(doc, item)
		boolVal := toBool(val)
		if !boolVal {
			return false
		}
	}
	return true
}

func handleOr(doc map[string]interface{}, opVal interface{}) interface{} {
	arr, ok := opVal.([]interface{})
	if !ok {
		return false
	}
	for _, item := range arr {
		val := evaluateExpression(doc, item)
		boolVal := toBool(val)
		if boolVal {
			return true
		}
	}
	return false
}

func handleNot(doc map[string]interface{}, opVal interface{}) interface{} {
	val := evaluateExpression(doc, opVal)
	boolVal := toBool(val)
	return !boolVal
}

// Conditional
// $cond can have two formats:
// 1) $cond: { if: <expr>, then: <expr>, else: <expr> }
// 2) $cond: [ <if>, <then>, <else> ]
func handleCond(doc map[string]interface{}, opVal interface{}) interface{} {
	switch condVal := opVal.(type) {
	case map[string]interface{}:
		ifExpr := evaluateExpression(doc, condVal["if"])
		thenExpr := condVal["then"]
		elseExpr := condVal["else"]
		if toBool(ifExpr) {
			return evaluateExpression(doc, thenExpr)
		}
		return evaluateExpression(doc, elseExpr)

	case []interface{}:
		// Format: [ <if>, <then>, <else> ]
		if len(condVal) != 3 {
			return nil
		}
		ifExpr := evaluateExpression(doc, condVal[0])
		thenExpr := condVal[1]
		elseExpr := condVal[2]
		if toBool(ifExpr) {
			return evaluateExpression(doc, thenExpr)
		}
		return evaluateExpression(doc, elseExpr)
	}
	return nil
}

// ---------- Utility Functions ----------

func resolveField(doc map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	current := doc
	for i, part := range parts {
		val, exists := current[part]
		if !exists {
			return nil
		}
		if i == len(parts)-1 {
			return val
		}
		nested, ok := val.(map[string]interface{})
		if !ok {
			return nil
		}
		current = nested
	}
	return nil
}

func toBool(val interface{}) bool {
	switch x := val.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	case nil:
		return false
	case string:
		return x != ""
	default:
		return false
	}
}

func (db *DB) validateProjectStage(params map[string]interface{}) error {

	// For $project, each entry typically is 1, 0, or an expression. Minimal validation:
	if len(params) == 0 {
		return fmt.Errorf("$project stage must not be empty")
	}
	for field, val := range params {
		switch v := val.(type) {
		case float64:
			// Usually valid values are 1 or 0
			if v != 1 && v != 0 {
				return fmt.Errorf("$project field %q must be 1 or 0, got %v", field, v)
			}
		case string:
			if !strings.HasPrefix(v, "$") {
				return fmt.Errorf("$project field %q has unexpected type %T", field, v)
			}
		case bool:
			// Sometimes boolean is used in projections as well, that’s fine
		case map[string]interface{}:
			// Expression-based projection, e.g. { "$concat": [...] }, you'd parse more deeply
		default:
			return fmt.Errorf("$project field %q has unexpected type %T", field, v)
		}
	}
	return nil
}
