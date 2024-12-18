package marco

import (
	"fmt"
	"log"
)

// facetStage applies multiple pipelines (facets) to the input dataset and returns the results.
//
// Parameters:
// - input: Slice of input documents to be processed by each pipeline.
// - params: A map where keys are facet names and values are corresponding pipelines (slices of stages).
// - docs: A map representing all collections, used for operations like $lookup.
//
// Returns:
// - A slice containing a single map, where keys are facet names and values are results of corresponding pipelines.
//
// Behavior:
// - Each facet operates independently on the input dataset.
// - Unsupported or invalid pipelines are skipped with an error message.
//
// Example:
// { "facet1": [{"$match": {"field": "value"}}], "facet2": [{"$sort": {"field": 1}}] }
// - facet1: Filters input based on a match condition.
// - facet2: Sorts input by the specified field.
func (db *DB) facetStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	// Initialize the result as a slice with one map (to simulate MongoDB facet output).
	result := []map[string]interface{}{
		make(map[string]interface{}),
	}

	// Iterate through each facet defined in params.
	for facetName, rawPipeline := range params {
		// Assert that rawPipeline is a slice of pipeline stages.
		pipeline, ok := rawPipeline.([]interface{})
		if !ok {
			fmt.Printf("Invalid pipeline for facet %s\n", facetName)
			continue
		}

		// Apply the pipeline to the input data.
		facetResult := db.applyPipeline(input, pipeline)

		// Store the result of the facet in the output map.
		result[0][facetName] = facetResult
	}

	return result
}

// applyPipeline applies a sequence of aggregation stages to an input dataset.
//
// Parameters:
// - input: Slice of input documents to process.
// - pipeline: A slice of stages (maps) to apply sequentially.
// - docs: A map representing all collections, used for operations like $lookup.
//
// Returns:
// - A slice of documents resulting from the pipeline execution.
//
// Supported stages include:
// - $match: Filters documents based on conditions.
// - $project: Transforms documents by including/excluding fields.
// - $group: Groups documents by a specified key.
// - $facet: Runs multiple sub-pipelines and combines results.
// - $sort: Sorts documents by specified fields.
// - $limit: Limits the number of documents in the result.
// - $skip: Skips a specified number of documents.
// - $lookup: Performs a join-like operation with another collection.
// - $unwind: Deconstructs arrays in documents into individual documents.
func (db *DB) applyPipeline(
	input []map[string]interface{},
	pipeline []interface{},
) []map[string]interface{} {
	data := input // Initialize the data with the input dataset.

	// Iterate through each stage in the pipeline.
	for _, stage := range pipeline {
		switch s := stage.(type) {
		case map[string]interface{}:
			// Process each key-value pair in the stage.
			for key, value := range s {
				switch key {
				case "$match":
					// Apply $match stage to filter documents.
					data = db.matchStage(data, value.(map[string]interface{}))
				case "$project":
					// Apply $project stage to transform documents.
					data = db.projectStage(data, value.(map[string]interface{}))
				case "$group":
					// Apply $group stage to group documents by a specified key.
					data = db.groupStage(data, value.(map[string]interface{}))
				case "$facet":
					// Apply $facet stage to process multiple pipelines.
					data = db.facetStage(data, value.(map[string]interface{}))
				case "$sort":
					// Apply $sort stage to sort documents.
					data = db.sortStage(data, value.(map[string]interface{}))
				case "$limit":
					// Apply $limit stage to restrict the number of documents.
					data = db.limitStage(data, value.(map[string]interface{}))
				case "$skip":
					// Apply $skip stage to skip a specified number of documents.
					data = db.skipStage(data, value.(map[string]interface{}))
				case "$lookup":
					// Apply $lookup stage to perform a join-like operation with another collection.
					data = db.lookupStage(data, value.(map[string]interface{}))
				case "$unwind":
					// Apply $unwind stage to deconstruct arrays into individual documents.
					data = db.unwindStage(data, value.(map[string]interface{}))
				default:
					// Log unsupported aggregation stages.
					log.Printf("Unsupported aggregation stage: %s", value.(map[string]interface{}))
				}
			}
		default:
			// Handle invalid stage formats.
			log.Println("Invalid stage format")
		}
	}
	return data
}

func (db *DB) validateFacetStage(params map[string]interface{}) error {

	// By MongoDB spec, $facet is an object where each key is a pipeline array
	// e.g. { "$facet": { "categorizedByTags": [ { "$unwind": "$tags" }, ... ], ... } }
	for facetName, facetPipelines := range params {
		pipelines, ok := facetPipelines.([]interface{})
		if !ok {
			return fmt.Errorf("$facet: each value must be an array of pipeline stages. Facet %q invalid", facetName)
		}
		// We could recursively validate each pipeline stage in the array if we want deeper checks
		for _, stageAny := range pipelines {
			// Each pipeline stage is itself an object with a single key like {"$unwind": {...}}
			stageMap, ok := stageAny.(map[string]interface{})
			if !ok {
				return fmt.Errorf("$facet: pipeline stage must be an object, got %T", stageAny)
			}
			if len(stageMap) != 1 {
				return fmt.Errorf("$facet: pipeline stage must have exactly one operator, got %d keys", len(stageMap))
			}
			// Recursively validate
			for op, opParams := range stageMap {
				if err := db.validateStage(op, asMap(opParams)); err != nil {
					return fmt.Errorf("$facet: sub-stage %q invalid: %v", op, err)
				}
			}
		}
	}
	return nil

}
