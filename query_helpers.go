package marco

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Helper function for date formatting
func formatDate(date interface{}, format string) string {
	// This is a simplified implementation
	// In a real-world scenario, you'd want more robust date parsing
	switch v := date.(type) {
	case string:
		// Try to parse the string date
		parsedTime, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return v // Return original if can't parse
		}
		switch format {
		case "%Y-%m-%d":
			return parsedTime.Format("2006-01-02")
		case "%H:%M:%S":
			return parsedTime.Format("15:04:05")
		case "%Y-%m-%d %H:%M:%S":
			return parsedTime.Format("2006-01-02 15:04:05")
		default:
			return parsedTime.Format("2006-01-02")
		}
	case time.Time:
		switch format {
		case "%Y-%m-%d":
			return v.Format("2006-01-02")
		case "%H:%M:%S":
			return v.Format("15:04:05")
		case "%Y-%m-%d %H:%M:%S":
			return v.Format("2006-01-02 15:04:05")
		default:
			return v.Format("2006-01-02")
		}
	default:
		return fmt.Sprintf("%v", date)
	}
}

// Helper function for substring
func extractSubstring(str interface{}, start, length int) string {
	strVal := fmt.Sprintf("%v", str)
	if start < 0 || start >= len(strVal) {
		return ""
	}

	end := start + length
	if end > len(strVal) {
		end = len(strVal)
	}

	return strVal[start:end]
}

// toFloat64 is a helper to cast an interface{} to float64
func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case string:
		if num, err := strconv.ParseFloat(v, 64); err == nil {
			return num, true
		}
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
	return 0, false
}

// getNestedField retrieves a nested field value using dot notation
func getNestedField(doc map[string]interface{}, field string) interface{} {
	// Split the field by dot for nested lookup
	parts := strings.Split(field, ".")
	current := interface{}(doc)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]interface{}:
			current = v[part]
		case []map[string]interface{}:
			// If it's an array of maps, try to find the field in each
			var results []interface{}
			for _, item := range v {
				val := item[part]
				results = append(results, val)
			}
			current = results
		default:
			return nil
		}

		// If at any point the current value is nil, return nil
		if current == nil {
			return nil
		}
	}

	return current
}

// Enhanced getNestedField to return whether the field exists
func getNestedFieldExists(doc map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	current := doc
	for i, part := range parts {
		val, exists := current[part]
		if !exists {
			return nil, false // Field does not exist
		}
		// If this is the last part of the path, return the value
		if i == len(parts)-1 {
			return val, true
		}
		// Traverse deeper if the value is a nested map
		if nestedMap, ok := val.(map[string]interface{}); ok {
			current = nestedMap
		} else {
			return nil, false // Invalid path
		}
	}
	return nil, false // Should not reach here
}
