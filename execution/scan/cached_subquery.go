// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

const (
	// minStepsToCache is the minimum number of subquery steps required to enable caching.
	// Below this threshold, the overhead of caching outweighs the savings.
	// Default: 2 (cache any subquery with 2+ steps). Increase for production if needed.
	minStepsToCache int64 = 2

	// maxCacheableSeriesPerStep is the maximum number of series per step that will be cached.
	// Steps with more series than this are skipped to avoid excessive memory/cache usage.
	maxCacheableSeriesPerStep = 10000
)

// cachedSubqueryOperator wraps the subquery's inner operator with caching.
// It holds two inner operators:
//   - narrowInner: covers only the uncached time range (new steps since last evaluation)
//   - fullInner: covers the full time range (fallback for cold start)
//
// On the first call to Next(), it checks the cache to decide which path to use.
// If cache has data from a previous evaluation, it serves cached steps first,
// then delegates to narrowInner for new steps.
// If cache is empty (cold start), it delegates entirely to fullInner and caches results.
type cachedSubqueryOperator struct {
	narrowInner model.VectorOperator
	fullInner   model.VectorOperator
	cache       query.SubqueryCache

	keyPrefix string
	mint      int64
	maxt      int64
	step      int64

	seriesOnce sync.Once
	series     []labels.Labels
	seriesErr  error

	// Decision state.
	decided  bool
	useCache bool

	// Cache-read state: steps served from cache.
	cachedSteps []cachedStepEntry
	cacheIdx    int

	// Track whether we've exhausted cached steps and switched to narrowInner.
	servingFromNarrow bool
}

type cachedStepEntry struct {
	t       int64
	samples []float64
}

// NewCachedSubqueryOperator creates a cached operator with both narrow and full inner operators.
// If cache is nil, returns fullInner directly (no caching).
func NewCachedSubqueryOperator(
	fullInner model.VectorOperator,
	narrowInner model.VectorOperator,
	opts *query.Options,
	innerExpr logicalplan.Node,
) model.VectorOperator {
	if opts.SubqueryCache == nil {
		return fullInner
	}
	step := opts.Step.Milliseconds()
	if step == 0 {
		step = 1
	}

	// Skip caching if the range is too short to benefit.
	totalSteps := (opts.End.UnixMilli() - opts.Start.UnixMilli()) / step
	if totalSteps < minStepsToCache {
		return fullInner
	}

	return &cachedSubqueryOperator{
		narrowInner: narrowInner,
		fullInner:   fullInner,
		cache:       opts.SubqueryCache,
		keyPrefix:   fmt.Sprintf("sq:%s:%016x", opts.TenantID, logicalplan.NodeFingerprint(innerExpr)),
		mint:        opts.Start.UnixMilli(),
		maxt:        opts.End.UnixMilli(),
		step:        step,
	}
}

func (c *cachedSubqueryOperator) String() string {
	return fmt.Sprintf("[cachedSubquery] %s", c.fullInner.String())
}

func (c *cachedSubqueryOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{c.fullInner, c.narrowInner}
}

func (c *cachedSubqueryOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	c.seriesOnce.Do(func() {
		c.series, c.seriesErr = c.fullInner.Series(ctx)
	})
	return c.series, c.seriesErr
}

func (c *cachedSubqueryOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	// Decide once which path to take.
	if !c.decided {
		c.decided = true
		c.decide()
	}

	if !c.useCache {
		// Cold start: use full inner, cache everything.
		return c.nextFromFull(ctx, buf)
	}

	// Warm path: serve from cached steps, then from narrowInner.
	return c.nextFromCacheAndNarrow(ctx, buf)
}

func (c *cachedSubqueryOperator) decide() {
	// Load all cached steps for our range.
	for t := c.mint; t <= c.maxt; t += c.step {
		key := c.cacheKey(t)
		if vals := c.cache.Get(key); vals != nil {
			c.cachedSteps = append(c.cachedSteps, cachedStepEntry{t: t, samples: vals})
		} else {
			// First miss — everything from here is uncached.
			break
		}
	}
	// Use cache path if we have at least one cached step.
	c.useCache = len(c.cachedSteps) > 0
}

func (c *cachedSubqueryOperator) nextFromCacheAndNarrow(ctx context.Context, buf []model.StepVector) (int, error) {
	n := 0

	// Serve from cached steps first.
	for n < len(buf) && c.cacheIdx < len(c.cachedSteps) {
		entry := c.cachedSteps[c.cacheIdx]
		buf[n].Reset(entry.t)
		ids := make([]uint64, len(entry.samples))
		for j := range ids {
			ids[j] = uint64(j)
		}
		buf[n].AppendSamples(ids, entry.samples)
		n++
		c.cacheIdx++
	}

	if n == len(buf) {
		return n, nil
	}

	// Cached steps exhausted — pull remaining from narrowInner.
	if !c.servingFromNarrow {
		c.servingFromNarrow = true
	}
	innerBuf := buf[n:]
	vecN, err := c.narrowInner.Next(ctx, innerBuf)
	if err != nil {
		return n, err
	}

	// Cache newly computed steps (skip if cardinality too high).
	for i := 0; i < vecN; i++ {
		if len(innerBuf[i].Samples) <= maxCacheableSeriesPerStep {
			c.cache.Put(c.cacheKey(innerBuf[i].T), innerBuf[i].Samples)
		}
	}

	// Cleanup: remove entries that have fallen out of the window.
	c.cache.DeleteBefore(c.keyPrefix, c.mint)

	return n + vecN, nil
}

func (c *cachedSubqueryOperator) nextFromFull(ctx context.Context, buf []model.StepVector) (int, error) {
	vecN, err := c.fullInner.Next(ctx, buf)
	if err != nil {
		return 0, err
	}

	// Cache all produced steps for next evaluation (skip if cardinality too high).
	for i := 0; i < vecN; i++ {
		if len(buf[i].Samples) <= maxCacheableSeriesPerStep {
			c.cache.Put(c.cacheKey(buf[i].T), buf[i].Samples)
		}
	}

	return vecN, nil
}

func (c *cachedSubqueryOperator) cacheKey(timestamp int64) string {
	return fmt.Sprintf("%s:%d", c.keyPrefix, timestamp)
}
