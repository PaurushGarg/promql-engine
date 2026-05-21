// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"strings"
	"sync"
	"time"
)

// SubqueryCache is the interface for caching subquery inner step results.
// For POC, use NewLocalSubqueryCache(). For production, implement with memcached/Redis.
type SubqueryCache interface {
	// Get retrieves a cached step result. Returns nil if not found.
	Get(key string) []float64
	// Put stores a step result.
	Put(key string, values []float64)
	// GetLatestTimestamp returns the highest cached timestamp for a given key prefix.
	// Returns -1 if no entries exist for this prefix.
	GetLatestTimestamp(keyPrefix string) int64
	// Stats returns cache statistics for observability.
	Stats() CacheStats
}

// CacheStats holds observability data about cache usage.
type CacheStats struct {
	Entries    int
	SizeBytes  int64
	Hits       int64
	Misses     int64
	Evictions  int64
}

// LocalSubqueryCacheConfig configures the local cache.
type LocalSubqueryCacheConfig struct {
	MaxSizeBytes int64         // Upper bound on total cache size. 0 = unlimited.
	TTL          time.Duration // Entries older than TTL are evicted. 0 = no expiry.
}

// LocalSubqueryCache is a simple in-memory cache for POC/testing.
// It persists across query evaluations when passed via query.Options.
type LocalSubqueryCache struct {
	mu     sync.RWMutex
	store  map[string]cacheEntry
	latest map[string]int64
	config LocalSubqueryCacheConfig

	// Stats.
	hits      int64
	misses    int64
	evictions int64
	sizeBytes int64
}

type cacheEntry struct {
	values    []float64
	createdAt time.Time
}

func NewLocalSubqueryCache() *LocalSubqueryCache {
	return NewLocalSubqueryCacheWithConfig(LocalSubqueryCacheConfig{
		MaxSizeBytes: 2 * 1024 * 1024 * 1024, // 2GB default
		TTL:          3 * 24 * time.Hour,      // 3 days default
	})
}

func NewLocalSubqueryCacheWithConfig(cfg LocalSubqueryCacheConfig) *LocalSubqueryCache {
	return &LocalSubqueryCache{
		store:  make(map[string]cacheEntry),
		latest: make(map[string]int64),
		config: cfg,
	}
}

func (c *LocalSubqueryCache) Get(key string) []float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.store[key]
	if !ok {
		c.misses++
		return nil
	}
	// Check TTL.
	if c.config.TTL > 0 && time.Since(entry.createdAt) > c.config.TTL {
		c.misses++
		c.sizeBytes -= int64(len(entry.values) * 8)
		c.evictions++
		delete(c.store, key)
		return nil
	}
	c.hits++
	return entry.values
}

func (c *LocalSubqueryCache) Put(key string, values []float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entrySize := int64(len(values) * 8)

	// Enforce size limit — skip if adding this would exceed.
	if c.config.MaxSizeBytes > 0 && c.sizeBytes+entrySize > c.config.MaxSizeBytes {
		// Don't cache — would exceed limit.
		return
	}

	// Remove old entry if replacing.
	if old, exists := c.store[key]; exists {
		c.sizeBytes -= int64(len(old.values) * 8)
	}

	cp := make([]float64, len(values))
	copy(cp, values)
	c.store[key] = cacheEntry{values: cp, createdAt: time.Now()}
	c.sizeBytes += entrySize

	// Update latest timestamp tracking.
	lastColon := strings.LastIndex(key, ":")
	if lastColon > 0 {
		prefix := key[:lastColon]
		var ts int64
		for _, ch := range key[lastColon+1:] {
			if ch >= '0' && ch <= '9' {
				ts = ts*10 + int64(ch-'0')
			}
		}
		if ts > c.latest[prefix] {
			c.latest[prefix] = ts
		}
	}
}

func (c *LocalSubqueryCache) GetLatestTimestamp(keyPrefix string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ts, ok := c.latest[keyPrefix]; ok {
		return ts
	}
	return -1
}

func (c *LocalSubqueryCache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Entries:   len(c.store),
		SizeBytes: c.sizeBytes,
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}
