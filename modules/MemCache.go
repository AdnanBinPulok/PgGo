package modules

import (
	"container/list"
	"sync"
	"time"
)

// CacheItem represents a single item in the cache.
type CacheItem struct {
	Key        string
	Value      []byte
	Expiration int64
}

// MemoryCache is a simple in-memory cache implementation with LRU eviction.
type MemoryCache struct {
	items     map[string]*list.Element
	evictList *list.List
	mu        sync.RWMutex
	maxSize   int
}

// NewMemoryCache creates a new instance of MemoryCache.
func NewMemoryCache(maxSize int) *MemoryCache {
	return &MemoryCache{
		items:     make(map[string]*list.Element),
		evictList: list.New(),
		maxSize:   maxSize,
	}
}

// Set adds an item to the cache with a TTL.
func (c *MemoryCache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	expiration := time.Now().Add(ttl).UnixNano()

	// Check if item exists
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*CacheItem).Value = value
		ent.Value.(*CacheItem).Expiration = expiration
		return
	}

	// Add new item
	ent := &CacheItem{Key: key, Value: value, Expiration: expiration}
	entry := c.evictList.PushFront(ent)
	c.items[key] = entry

	// Evict if needed
	if c.maxSize > 0 && c.evictList.Len() > c.maxSize {
		c.removeOldest()
	}
}

// removeOldest removes the oldest item from the cache.
func (c *MemoryCache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement removes an element from the cache.
func (c *MemoryCache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*CacheItem)
	delete(c.items, kv.Key)
}

// Get retrieves an item from the cache.
func (c *MemoryCache) Get(key string) ([]byte, bool) {
	c.mu.Lock() // Lock instead of RLock because we might move element to front
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		if time.Now().UnixNano() > ent.Value.(*CacheItem).Expiration {
			c.removeElement(ent)
			return nil, false
		}
		c.evictList.MoveToFront(ent)
		return ent.Value.(*CacheItem).Value, true
	}
	return nil, false
}

// Delete removes an item from the cache.
func (c *MemoryCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
	}
}

// Clear removes all items from the cache.
func (c *MemoryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*list.Element)
	c.evictList.Init()
}
