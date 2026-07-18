// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"sync"
)

// SubqueryCache is the interface for caching subquery inner step results.
// The cache stores raw bytes — serialization is handled by the caller.
// This allows caching both float samples and native histograms.
//
// For production, implement with memcached (or any distributed cache).
// For tests, use NewMockSubqueryCache().
type SubqueryCache interface {
	// Get retrieves a cached entry by key. Returns nil if not found or expired.
	Get(key string) []byte
	// GetMulti retrieves multiple entries in a single round trip.
	// Returns a map of found keys to their values. Missing keys are absent from the map.
	GetMulti(keys []string) map[string][]byte
	// Put stores an entry with the given key. The cache implementation may apply
	// a TTL as a safety net, but callers should use explicit Delete for eviction.
	Put(key string, value []byte)
	// Delete removes a specific key from the cache.
	Delete(key string)
	// Stats returns cache statistics for observability.
	Stats() CacheStats
}

// CacheStats holds observability data about cache usage.
type CacheStats struct {
	Entries   int
	SizeBytes int64
	Hits      int64
	Misses    int64
	Evictions int64
}

// MockSubqueryCache is a simple in-memory cache for unit tests.
// It implements SubqueryCache with no TTL, no size limits, and no network.
type MockSubqueryCache struct {
	mu    sync.RWMutex
	store map[string][]byte

	// Stats.
	hits    int64
	misses  int64
}

func NewMockSubqueryCache() *MockSubqueryCache {
	return &MockSubqueryCache{
		store: make(map[string][]byte),
	}
}

func (c *MockSubqueryCache) Get(key string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.store[key]
	if !ok {
		c.misses++
		return nil
	}
	c.hits++
	return entry
}

func (c *MockSubqueryCache) GetMulti(keys []string) map[string][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string][]byte, len(keys))
	for _, key := range keys {
		if entry, ok := c.store[key]; ok {
			c.hits++
			result[key] = entry
		} else {
			c.misses++
		}
	}
	return result
}

func (c *MockSubqueryCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]byte, len(value))
	copy(cp, value)
	c.store[key] = cp
}

func (c *MockSubqueryCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, key)
}

func (c *MockSubqueryCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var size int64
	for _, v := range c.store {
		size += int64(len(v))
	}
	return CacheStats{
		Entries:   len(c.store),
		SizeBytes: size,
		Hits:      c.hits,
		Misses:    c.misses,
	}
}

// --- End of cache implementation ---
