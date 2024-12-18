package marco

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// AggregationStage represents a single stage in the MongoDB aggregation pipeline
type AggregationStage struct {
	Stage  string
	Params map[string]interface{}
}

// query data using mongo style pipeline aggregation query
func (db *DB) Query(
	collectionName string, // The target collection name
	mongoAggregationPipeline string, // The aggregation pipeline in JSON format
) ([]map[string]interface{}, error) {

	// Parse the aggregation stages using JSON parsing
	stages, err := db.parseAggregationStagesJSON(mongoAggregationPipeline)
	if err != nil {
		return nil, fmt.Errorf("error parsing aggregation stages: %v", err)
	}

	// Retrieve the specified collection
	// Start with a copy of  documents from the specified collection
	stageInput, _ := db.Collection(collectionName)
	if len(stageInput) == 0 {
		return nil, nil
	}

	// Process each stage of the aggregation pipeline
	for _, stage := range stages {

		switch stage.Stage {
		case "$match":
			stageInput = db.matchStage(stageInput, stage.Params)
		case "$project":
			stageInput = db.projectStage(stageInput, stage.Params)
		case "$group":
			stageInput = db.groupStage(stageInput, stage.Params)
		case "$facet":
			stageInput = db.facetStage(stageInput, stage.Params)
		case "$sort":
			stageInput = db.sortStage(stageInput, stage.Params)
		case "$limit":
			stageInput = db.limitStage(stageInput, stage.Params)
			if stageInput == nil {
				return nil, fmt.Errorf("error in $limit stage: invalid limit value")
			}
		case "$skip":
			stageInput = db.skipStage(stageInput, stage.Params)
		case "$lookup":
			stageInput = db.lookupStage(stageInput, stage.Params) // Use docs for lookups
		case "$unwind":
			stageInput = db.unwindStage(stageInput, stage.Params)
		case "$sample":
			stageInput, err = db.sampleStage(stageInput, stage.Params)
			if err != nil {
				return nil, fmt.Errorf("error in $sample stage: %w", err)
			}
		case "$sortByCount":
			stageInput, err = db.sortByCountStage(stageInput, stage.Params)
			if err != nil {
				return nil, fmt.Errorf("error in $sortByCount stage: %w", err)
			}
		case "$unionWith":
			// future feature
		case "$redact":
			// future feature
		case "$graphLookup":
			// future feature
		case "$geoNear":
			// future feature
		case "$fill":
			//

		case "$count":
			stageInput, err = db.countStage(stageInput, stage.Params)
			if err != nil {
				return nil, fmt.Errorf("error in $count stage: %w", err)
			}
		case "$replaceRoot":
			//
		case "$replaceWith":
			//
		case "$set":
			//
		case "$unset":
			stageInput, _ = db.unsetStage(stageInput, stage.Params)

		case "$addFields":
			stageInput, err = db.addFieldsStage(stageInput, stage.Params)
			if err != nil {
				return nil, fmt.Errorf("error in %s stage: %w", stage.Stage, err)
			}
		case "$bucket":
			stageInput, err = db.bucketStage(stageInput, stage.Params)
			if err != nil {
				return nil, fmt.Errorf("error in $bucket stage: %w", err)
			}
		case "$bucketAuto":
			stageInput, err = db.bucketAutoStage(stageInput, stage.Params)
			if err != nil {
				return nil, fmt.Errorf("error in $bucketAuto stage: %w", err)
			}

		default:
			log.Printf("Unsupported aggregation stage: %s", stage.Stage)
		}

		// If no results, break the pipeline
		if len(stageInput) == 0 {
			break
		}
	}

	return stageInput, nil
}

func (db *DB) parseAggregationStagesJSON(query string) ([]AggregationStage, error) {
	// Remove potential whitespace and trim
	query = strings.TrimSpace(query)

	// Handle different input formats - with or without outer []
	if !strings.HasPrefix(query, "[") {
		query = "[" + query + "]"
	}

	var stageData []map[string]interface{}
	if err := json.Unmarshal([]byte(query), &stageData); err != nil {
		return nil, fmt.Errorf("error parsing JSON query at input: %s, error: %v", query, err)
	}

	var stages []AggregationStage
	for _, stageMap := range stageData {
		// Each stage is a map with a single key representing the stage type
		for stageName, params := range stageMap {
			// Convert params to map[string]interface{}
			paramsMap := make(map[string]interface{})
			switch v := params.(type) {
			case map[string]interface{}:
				paramsMap = v
			case string:
				paramsMap["path"] = v // For stages like "$unwind"
			case float64, int, bool:
				paramsMap["value"] = v // For stages with scalar values

			default:
				return nil, fmt.Errorf("invalid parameters for stage %s: %v", stageName, params)
			}

			// Optional: Validate the stage structure
			if err := db.validateStage(stageName, paramsMap); err != nil {
				return nil, err
			}

			stages = append(stages, AggregationStage{
				Stage:  stageName,
				Params: paramsMap,
			})
		}
	}

	return stages, nil
}

// Example validation function
// validateStage checks that stage params have the required fields and acceptable value types.
func (db *DB) validateStage(stageName string, params map[string]interface{}) error {

	switch stageName {

	case "$match":
		return db.validateMatchStage(params)

	case "$project":
		return db.validateProjectStage(params)

	case "$group":
		return db.validateGroupStage(params)

	case "$facet":
		return db.validateFacetStage(params)

	case "$sample":
		return db.validateSampleStage(params)

	case "$sort":
		return db.validateSortStage(params)

	case "$count":
		return db.validateCountStage(params)

	case "$limit":
		return db.validateLimitStage(params)

	case "$sortByCount":
		return db.validateSortByCountStage(params)

	case "$skip":
		return db.validateSkipStage(params)

	case "$bucket":
		return db.validateBucketStage(params)

	case "$bucketAuto":
		return db.validateBucketAutoStage(params)

	case "$lookup":
		return db.validateLookupStage(params)

	case "$unset":
		_, err := db.validateUnsetStage(params)
		return err

	case "$unwind":
		return db.validateUnwindStage(params)

	case "$addFields", "$set":
		return db.validateAddFieldsStage(params)

	default:
		// Return an error (or just skip) for an unrecognized stage.
		return fmt.Errorf("unsupported aggregation stage: %s", stageName)
	}
}

// isValidMatchOperator checks if the provided operator is a valid MongoDB match operator.
func isValidMatchOperator(op string) bool {
	allowed := map[string]bool{
		// Comparison Operators
		"$eq":  true,
		"$gt":  true,
		"$gte": true,
		"$in":  true,
		"$lt":  true,
		"$lte": true,
		"$ne":  true,
		"$nin": true,

		// Logical Operators
		"$and": true,
		"$or":  true,
		"$not": true,
		"$nor": true,

		// Flags
		"$options": true,

		// Element Operators
		"$exists": true,
		"$type":   true,

		// Evaluation Operators
		"$expr":       true,
		"$jsonSchema": true,
		"$mod":        true,
		"$regex":      true,
		"$text":       true,
		"$where":      true,

		// Array Operators
		"$all":       true,
		"$elemMatch": true,
		"$size":      true,

		// Geospatial Operators
		"$geoWithin":     true,
		"$geoIntersects": true,
		"$near":          true,
		"$nearSphere":    true,

		// Bitwise Operators
		"$bitsAllClear": true,
		"$bitsAllSet":   true,
		"$bitsAnyClear": true,
		"$bitsAnySet":   true,

		// Other Operators
		"$comment":         true,
		"$sampleRate":      true,
		"$rand":            true,
		"$meta":            true,
		"$literal":         true,
		"$var":             true,
		"$concat":          true,
		"$substr":          true,
		"$toLower":         true,
		"$toUpper":         true,
		"$trim":            true,
		"$ltrim":           true,
		"$rtrim":           true,
		"$split":           true,
		"$strLenBytes":     true,
		"$strLenCP":        true,
		"$strcasecmp":      true,
		"$substrBytes":     true,
		"$substrCP":        true,
		"$indexOfBytes":    true,
		"$indexOfCP":       true,
		"$toString":        true,
		"$dateToString":    true,
		"$dateFromString":  true,
		"$add":             true,
		"$subtract":        true,
		"$multiply":        true,
		"$divide":          true,
		"$pow":             true,
		"$sqrt":            true,
		"$abs":             true,
		"$ceil":            true,
		"$floor":           true,
		"$trunc":           true,
		"$round":           true,
		"$sin":             true,
		"$cos":             true,
		"$tan":             true,
		"$asin":            true,
		"$acos":            true,
		"$atan":            true,
		"$atan2":           true,
		"$ln":              true,
		"$log":             true,
		"$log10":           true,
		"$exp":             true,
		"$min":             true,
		"$max":             true,
		"$avg":             true,
		"$sum":             true,
		"$stdDevPop":       true,
		"$stdDevSamp":      true,
		"$first":           true,
		"$last":            true,
		"$push":            true,
		"$addToSet":        true,
		"$mergeObjects":    true,
		"$arrayElemAt":     true,
		"$filter":          true,
		"$map":             true,
		"$reduce":          true,
		"$zip":             true,
		"$range":           true,
		"$concatArrays":    true,
		"$arrayToObject":   true,
		"$objectToArray":   true,
		"$setUnion":        true,
		"$setIntersection": true,
		"$setDifference":   true,
		"$setEquals":       true,
		"$setIsSubset":     true,
		"$anyElementTrue":  true,
		"$allElementsTrue": true,
		"$document":        true,
		"$function":        true,
		"$let":             true,
		"$switch":          true,
		"$cond":            true,
		"$ifNull":          true,
		"$isNumber":        true,
		"$isString":        true,
		"$isDate":          true,
		"$isArray":         true,
		"$isObject":        true,
		"$isBool":          true,
	}

	return allowed[op]
}

// isValidGroupOperator checks if the provided operator is a valid MongoDB group operator.
func isValidGroupOperator(op string) bool {
	allowed := map[string]bool{
		// Accumulator Operators
		"$sum":          true,
		"$avg":          true,
		"$min":          true,
		"$max":          true,
		"$push":         true,
		"$addToSet":     true,
		"$first":        true,
		"$last":         true,
		"$stdDevPop":    true,
		"$stdDevSamp":   true,
		"$count":        true, // Available as a separate stage but can be represented as { $sum: 1 }
		"$mergeObjects": true, // Allows merging multiple documents into a single object

		// Newer Operators (Ensure your MongoDB version supports these)
		"$percentile":   true, // MongoDB 5.0+
		"$median":       true, // MongoDB 5.0+ via $percentile
		"$variancePop":  true, // MongoDB 5.0+
		"$varianceSamp": true, // MongoDB 5.0+
		// Add more operators as MongoDB evolves
	}

	return allowed[op]
}

// A small helper to safely cast interface{} to map[string]interface{} if possible
func asMap(val interface{}) map[string]interface{} {
	if casted, ok := val.(map[string]interface{}); ok {
		return casted
	}
	return nil
}
