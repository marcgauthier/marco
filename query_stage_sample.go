package marco

import (
	"errors"
	"log"
	"math"
	"math/rand"
	"time"
)

// init initializes the random seed.
func init() {
	rand.Seed(time.Now().UnixNano())
}

// sampleStage implements a random sampling operation similar to MongoDB's $sample stage.
// It selects a specified number of random documents from the input slice.
//
// Parameters:
// - input: Slice of documents to be processed.
// - params: A map containing the 'size' parameter.
//
// Returns:
// - A slice of randomly selected documents.
// - An error if the 'size' parameter is invalid or if the sample size exceeds input size.
//
// Behavior:
// - If no valid size is provided, returns an error.
// - If size is greater than input length, returns all documents in random order.
func (db *DB) sampleStage(
	input []map[string]interface{},
	params map[string]interface{},
) ([]map[string]interface{}, error) {
	// Extract the 'size' parameter
	sizeVal, ok := params["size"]
	if !ok {
		return nil, errors.New("$sample stage requires a 'size' parameter")
	}

	size, ok := toFloat64(sizeVal)
	if !ok {
		return nil, errors.New("$sample 'size' parameter must be a number")
	}

	// Convert size to integer
	n := int(math.Max(0, math.Floor(size)))

	if n == 0 {
		log.Println("Warning: $sample size is 0, returning empty result")
		return []map[string]interface{}{}, nil
	}

	if n >= len(input) {
		// Shuffle the entire input and return
		shuffled := make([]map[string]interface{}, len(input))
		copy(shuffled, input)
		rand.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		return shuffled, nil
	}

	// Implement Fisher-Yates shuffle to get a random sample without replacement
	sampled := make([]map[string]interface{}, n)
	temp := make([]map[string]interface{}, len(input))
	copy(temp, input)

	for i := 0; i < n; i++ {
		j := rand.Intn(len(temp))
		sampled[i] = temp[j]
		// Remove the selected element
		temp[j] = temp[len(temp)-1]
		temp = temp[:len(temp)-1]
	}

	return sampled, nil
}

// validateSampleStage validates the parameters for the $sample stage.
//
// Parameters:
// - params: A map containing the 'size' parameter.
//
// Returns:
// - An error if validation fails; otherwise, nil.
func (db *DB) validateSampleStage(params map[string]interface{}) error {
	// Check for the presence of 'size'
	sizeVal, ok := params["size"]
	if !ok {
		return errors.New("$sample stage requires a 'size' parameter")
	}

	// Ensure 'size' is a number
	size, ok := toFloat64(sizeVal)
	if !ok {
		return errors.New("$sample 'size' parameter must be a number")
	}

	// Ensure 'size' is positive
	if size <= 0 {
		return errors.New("$sample 'size' parameter must be a positive number")
	}

	return nil
}
