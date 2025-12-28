package modules

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// EnableCache initializes the in-memory cache for the table.
// It sets the TTL (Time-To-Live) for cached items and initializes the cache storage.
// If CacheMax is not set, it defaults to 1000 items.
// Note: CacheKey must be defined in the Table struct before calling this method.
func (t *Table) EnableCache(ttl time.Duration) {
	t.Cached = true
	t.CacheTTL = ttl
	if t.CacheMax == 0 {
		t.CacheMax = 1000 // Default to 1000 if not set
	}
	// t.CacheKey should be set in the Table struct initialization
	t.CacheData = NewMemoryCache(t.CacheMax)
}

// getCacheKey retrieves the value of the configured CacheKey from the query arguments.
// It searches for the CacheKey in map arguments or key-value pairs.
//
// Example: If CacheKey = "id"
//   - getCacheKey(map[string]interface{}{"id": 5}) -> "5", nil
//   - getCacheKey("id", 5) -> "5", nil
//
// Returns an error if caching is disabled, CacheKey is undefined, or the key is not found.
func (t *Table) getCacheKey(whereArgs ...interface{}) (string, error) {
	if !t.Cached {
		return "", fmt.Errorf("caching is not enabled for this table")
	}
	if t.CacheKey == "" {
		return "", fmt.Errorf("CacheKey is not defined for this table")
	}

	// 1. Check inside maps (Standard PgGo usage)
	for _, arg := range whereArgs {
		if m, ok := arg.(map[string]interface{}); ok {
			if val, found := m[t.CacheKey]; found {
				return fmt.Sprintf("%v", val), nil
			}
		}
	}

	// 2. Check for key-value pairs (User's requested pattern)
	for i := 0; i < len(whereArgs)-1; i += 2 {
		if key, ok := whereArgs[i].(string); ok && key == t.CacheKey {
			return fmt.Sprintf("%v", whereArgs[i+1]), nil
		}
	}

	if t.DebugMode {
		log.Printf("DEBUG: CacheKey '%s' not found in whereArgs: %v\n", t.CacheKey, whereArgs)
	}
	return "", fmt.Errorf("CacheKey '%s' not found in whereArgs", t.CacheKey)
}

// setCache sets the cache for the given key and value.
func (t *Table) setCache(key string, value interface{}) error {
	if !t.Cached || t.CacheData == nil {
		return nil // Cache not enabled, ignore
	}

	data, err := json.Marshal(value)
	if err != nil {
		if t.DebugMode {
			log.Println("DEBUG: Failed to marshal cache data:", err)
		}
		return fmt.Errorf("failed to marshal cache data: %w", err)
	}

	t.CacheData.Set(key, data, t.CacheTTL)
	if t.DebugMode {
		log.Printf("DEBUG: Cache Set Key: %s\n", key)
	}
	return nil
}

// getCacheValue retrieves a value from the cache and unmarshals it into the target.
// Returns true if found, false otherwise.
// returns error if unmarshaling fails.
// example usage:
//
//	var user map[string]interface{}
//	found, err := UsersTable.getCacheValue("5", &user)
//	if
func (t *Table) getCacheValue(key string, target interface{}) (bool, error) {
	if !t.Cached || t.CacheData == nil {
		return false, nil
	}

	data, found := t.CacheData.Get(key)
	if !found {
		if t.DebugMode {
			log.Printf("DEBUG: Cache Miss Key: %s\n", key)
		}
		return false, nil
	}

	err := json.Unmarshal(data, target) // unmarshal into provided target
	if err != nil {
		if t.DebugMode {
			log.Println("DEBUG: Failed to unmarshal cache data:", err)
		}
		return false, fmt.Errorf("failed to unmarshal cache data: %w", err)
	}

	if t.DebugMode {
		log.Printf("DEBUG: Cache Hit Key: %s\n", key)
	}
	return true, nil
}

func (t *Table) deleteCache(key string) error {
	if !t.Cached || t.CacheData == nil {
		return nil // Cache not enabled, ignore
	}

	if t.DebugMode {
		log.Printf("DEBUG: Deleting Cache Key: %s\n", key)
	}
	t.CacheData.Delete(key)
	return nil
}

func (t *Table) invalidateCache() error {
	if !t.Cached || t.CacheData == nil {
		return nil // Cache not enabled, ignore
	}
	if t.DebugMode {
		log.Println("DEBUG: Invalidating (Clearing) Cache")
	}
	t.CacheData.Clear()
	return nil
}
