package modules

import (
	"fmt"
	"regexp"
)

// isValidIdentifier checks if a string is a valid SQL identifier.
// It allows alphanumeric characters and underscores.
// It does NOT allow spaces or special characters to prevent SQL injection.
func isValidIdentifier(s string) bool {
	if len(s) == 0 || len(s) > 63 { // PostgreSQL identifier limit is usually 63 bytes
		return false
	}
	// Regex: Start with letter or underscore, followed by letters, numbers, or underscores.
	// This is a strict subset of valid SQL identifiers, but safe for general use.
	validIdentifier := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	return validIdentifier.MatchString(s)
}

// validateMapKeys checks if all keys in the map are valid identifiers.
func validateMapKeys(data map[string]interface{}) error {
	for key := range data {
		if !isValidIdentifier(key) {
			return fmt.Errorf("invalid column name/identifier: '%s'", key)
		}
	}
	return nil
}
