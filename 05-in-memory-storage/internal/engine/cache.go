package engine

import (
	"sync"
	"time"
)

type Cache[V any] struct {
	items map[string]*Item[V]
	mu    sync.RWMutex
}

func NewCache[V any]() *Cache[V] {
	cache := &Cache[V]{
		items: make(map[string]*Item[V]),
	}
	go cache.cleanupLoop()
	return cache
}

func (c *Cache[V]) Set(key string, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item := NewItem(value, ttl)
	c.items[key] = item
}

func (c *Cache[V]) Get(key string) (V, bool) {
	var zero V

	c.mu.RLock()
	item, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return zero, false
	}

	// Jika expired, hapus dan return not found
	if item.IsExpired() {
		c.mu.Lock()
		delete(c.items, key)
		c.mu.Unlock()
		return zero, false
	}

	return item.Value, true
}

func (c *Cache[V]) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.items[key]; exists {
		delete(c.items, key)
		return true
	}
	return false
}

func (c *Cache[V]) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanupExpired()
	}
}

func (c *Cache[V]) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, item := range c.items {
		if item.IsExpired() {
			delete(c.items, key)
		}
	}
}

// GetItem returns the item and its TTL info
func (c *Cache[V]) GetItem(key string) (V, time.Duration, bool) {
	var zero V

	c.mu.RLock()
	item, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return zero, 0, false
	}

	if item.IsExpired() {
		c.mu.Lock()
		delete(c.items, key)
		c.mu.Unlock()
		return zero, 0, false
	}

	ttl := item.TTL()
	return item.Value, ttl, true
}

// TTL returns the TTL of a key in seconds
func (c *Cache[V]) TTL(key string) int64 {
	c.mu.RLock()
	item, exists := c.items[key]
	c.mu.RUnlock()

	if !exists {
		return -2 // Key doesn't exist
	}

	if item.IsExpired() {
		c.mu.Lock()
		delete(c.items, key)
		c.mu.Unlock()
		return -2
	}

	ttl := item.TTL()
	if ttl < 0 {
		return -1 // No TTL
	}

	return int64(ttl.Seconds())
}
