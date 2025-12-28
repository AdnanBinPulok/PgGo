package modules

// clearCache invalidates all items in the table's in-memory cache.
// It does nothing if caching is not enabled or initialized.
func (t *Table) clearCache() error {
	if !t.Cached || t.CacheData == nil {
		return nil // Cache not enabled, ignore
	}
	t.CacheData.Clear()
	return nil
}
