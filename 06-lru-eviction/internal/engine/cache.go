package engine

import (
	"log"
	"pendem/internal/config"
	"sync"
	"time"
)

type Cache[V any] struct {
	evictor Evictor[V]
	policy  string
	mu      sync.RWMutex
}

func NewCache[V any](cfg config.EngineConfig, logger *log.Logger) *Cache[V] {
	var evictor Evictor[V]
	policy := cfg.EvictionPolicy
	if policy == "" {
		policy = "lru"
	}

	switch policy {
	case "lru":
		evictor = NewLRUWithMemory[V](cfg.EvictorCapacity, cfg.MaxMemory, logger)
	case "lfu":
		// Future: NewLFUWithMemory[V](cfg.EvictorCapacity, cfg.MaxMemory, logger)
		logger.Printf("LFU not implemented yet, falling back to LRU")
		evictor = NewLRUWithMemory[V](cfg.EvictorCapacity, cfg.MaxMemory, logger)
		policy = "lru"
	case "ttl":
		// Future: NewTTLEvictor[V](cfg.MaxMemory, logger)
		logger.Printf("TTL eviction not implemented yet, falling back to LRU")
		evictor = NewLRUWithMemory[V](cfg.EvictorCapacity, cfg.MaxMemory, logger)
		policy = "lru"
	default:
		logger.Printf("Unknown eviction policy '%s', using LRU", cfg.EvictionPolicy)
		evictor = NewLRUWithMemory[V](cfg.EvictorCapacity, cfg.MaxMemory, logger)
		policy = "lru"
	}

	cache := &Cache[V]{
		evictor: evictor,
		policy:  policy,
	}
	go cache.cleanupLoop()
	return cache
}

func (c *Cache[V]) Set(key string, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item := NewItem(value, ttl)
	c.evictor.Add(key, item)
}

func (c *Cache[V]) Get(key string) (V, bool) {
	var zero V

	c.mu.RLock()
	item, exists := c.evictor.Get(key)
	c.mu.RUnlock()

	if !exists {
		return zero, false
	}

	// Jika expired, hapus dan return not found
	if item.IsExpired() {
		c.mu.Lock()
		c.evictor.Remove(key)
		c.mu.Unlock()
		return zero, false
	}

	return item.Value, true
}

func (c *Cache[V]) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.evictor.Remove(key)
}

// TTL returns the TTL of a key in seconds
func (c *Cache[V]) TTL(key string) int64 {
	c.mu.RLock()
	item, exists := c.evictor.Get(key)
	c.mu.RUnlock()

	if !exists {
		return -2 // Key doesn't exist
	}

	if item.IsExpired() {
		c.mu.Lock()
		c.evictor.Remove(key)
		c.mu.Unlock()
		return -2
	}

	ttl := item.TTL()
	if ttl < 0 {
		return -1 // No TTL
	}

	return int64(ttl.Seconds())
}

func (c *Cache[V]) MemoryUsage() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictor.MemoryUsage()
}

func (c *Cache[V]) Policy() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.policy
}

// Size returns the number of items currently in cache
func (c *Cache[V]) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictor.Len()
}

// MaxCapacity returns the maximum capacity
func (c *Cache[V]) MaxCapacity() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictor.MaxCapacity()
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

	c.evictor.ForEach(func(key string, item *Item[V]) bool {
		if item.IsExpired() {
			c.evictor.Remove(key)
		}
		return true
	})
}
