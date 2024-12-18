package marco

import (
	"fmt"
	"log"
)

// lookupStage implements a lookup operation similar to MongoDB's $lookup aggregation stage
// It performs a left outer join between two collections based on specified fields
//
// Parameters:
// - input: Primary collection of documents to be augmented
// - params: Configuration for the lookup operation
// - data: Map of available collections for lookup
//
// Lookup Parameters:
// - from: Name of the collection to join from
// - localField: Field in the input collection to match
// - foreignField: Field in the foreign collection to match against
// - as: Name of the field to store matched documents
//
// Returns:
// - Augmented documents with matched foreign collection documents

func (db *DB) lookupStage(
	input []map[string]interface{},
	params map[string]interface{},
) []map[string]interface{} {
	// Validate and extract lookup parameters
	lookupParams, err := validateLookupParams(params)
	if err != nil {
		log.Printf("Lookup parameter validation error: %v", err)
		return input
	}

	// Retrieve the foreign collection
	foreignCollection, err := db.Collection(lookupParams.from)
	if err != nil {
		log.Printf("Foreign collection '%s' not found", lookupParams.from)
		return input
	}

	// Perform the lookup operation
	var results []map[string]interface{}
	for _, doc := range input {
		// Create a deep copy of the original document
		newDoc := deepCopyDocument(doc)

		// Find matching documents in the foreign collection
		matchedDocs := findMatchingDocuments(
			doc,
			foreignCollection,
			lookupParams.localField,
			lookupParams.foreignField,
		)

		// Add matched documents to the specified field
		newDoc[lookupParams.as] = matchedDocs

		results = append(results, newDoc)
	}

	return results
}

// lookupParameters encapsulates the configuration for a lookup operation
type lookupParameters struct {
	from         string
	localField   string
	foreignField string
	as           string
}

// validateLookupParams checks and extracts lookup parameters
func validateLookupParams(params map[string]interface{}) (*lookupParameters, error) {
	// Extract parameters with type checking
	from, ok1 := params["from"].(string)
	localField, ok2 := params["localField"].(string)
	foreignField, ok3 := params["foreignField"].(string)
	as, ok4 := params["as"].(string)

	// Validate all required parameters are present and of correct type
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nil, fmt.Errorf("invalid lookup parameters: missing or incorrect type")
	}

	// Ensure no empty strings
	if from == "" || localField == "" || foreignField == "" || as == "" {
		return nil, fmt.Errorf("lookup parameters cannot be empty strings")
	}

	return &lookupParameters{
		from:         from,
		localField:   localField,
		foreignField: foreignField,
		as:           as,
	}, nil
}

// deepCopyDocument creates a complete copy of a document to prevent unintended mutations
func deepCopyDocument(doc map[string]interface{}) map[string]interface{} {
	newDoc := make(map[string]interface{})
	for k, v := range doc {
		newDoc[k] = v
	}
	return newDoc
}

func findMatchingDocuments(
	doc map[string]interface{},
	foreignCollection []map[string]interface{},
	localField,
	foreignField string,
) []map[string]interface{} {
	var matchedDocs []map[string]interface{}
	localValue, ok := doc[localField]
	if !ok {
		return matchedDocs // Return empty if localField does not exist
	}

	for _, foreignDoc := range foreignCollection {
		if foreignDoc[foreignField] == localValue {
			// Add a deep copy of matched document to avoid mutation issues
			matchedDocs = append(matchedDocs, deepCopyDocument(foreignDoc))
		}
	}

	return matchedDocs
}

func (db *DB) validateLookupStage(params map[string]interface{}) error {

	requiredFields := []string{"from", "localField", "foreignField", "as"}
	for _, field := range requiredFields {
		if _, ok := params[field]; !ok {
			return fmt.Errorf("$lookup is missing required field: %q", field)
		}
		// Optionally check they are strings:
		if _, isString := params[field].(string); !isString {
			return fmt.Errorf("$lookup field %q must be a string", field)
		}
	}
	return nil

}
