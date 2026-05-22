// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
	"github.com/thanos-io/promql-engine/storage"

	promstorage "github.com/prometheus/prometheus/storage"
)

const maxCacheableSeriesPerStep = 10000

// ChunkedRangeOperator evaluates a *_over_time(metric[range]) by splitting the range
// into aligned chunks, evaluating each chunk independently, and merging results.
// The trailing edge (gap between last aligned chunk and evaluation time) is handled
// by evaluating one final chunk covering the remaining duration.
//
// When a SubqueryCache is configured, aligned chunk results are cached between
// evaluations. Only cache misses and the trailing edge are evaluated fresh.
type ChunkedRangeOperator struct {
	node     *logicalplan.ChunkedRangeSelector
	opts     *query.Options
	scanners storage.Scanners
	hints    promstorage.SelectHints

	// Cache (optional).
	cache    query.SubqueryCache
	cacheKey string

	seriesOnce sync.Once
	series     []labels.Labels
	seriesErr  error

	evaluated bool
}

func NewChunkedRangeOperator(
	node *logicalplan.ChunkedRangeSelector,
	opts *query.Options,
	scanners storage.Scanners,
	hints promstorage.SelectHints,
) *ChunkedRangeOperator {
	var cacheKey string
	if opts.SubqueryCache != nil {
		cacheKey = fmt.Sprintf("cr:%s:%016x", opts.TenantID, logicalplan.NodeFingerprint(node))
	}
	return &ChunkedRangeOperator{
		node:     node,
		opts:     opts,
		scanners: scanners,
		hints:    hints,
		cache:    opts.SubqueryCache,
		cacheKey: cacheKey,
	}
}

func (c *ChunkedRangeOperator) String() string {
	return fmt.Sprintf("[chunkedRange] %s(%s[%s])", c.node.Func.Name, c.node.VectorSelector.String(), c.node.Range)
}

func (c *ChunkedRangeOperator) Explain() (next []model.VectorOperator) {
	return nil
}

func (c *ChunkedRangeOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	c.seriesOnce.Do(func() {
		// Discover series using the full range.
		h := c.hints
		h.Start = c.opts.Start.UnixMilli() - c.node.Range.Milliseconds()
		h.End = c.opts.End.UnixMilli()
		h.Range = c.node.Range.Milliseconds()

		matrixNode := logicalplan.MatrixSelector{
			VectorSelector: c.node.VectorSelector,
			Range:          c.node.Range,
			OriginalString: c.node.String(),
		}
		funcNode := logicalplan.FunctionCall{Func: c.node.Func, Args: []logicalplan.Node{&matrixNode}}

		op, err := c.scanners.NewMatrixSelector(ctx, c.opts, h, matrixNode, funcNode)
		if err != nil {
			c.seriesErr = err
			return
		}
		c.series, c.seriesErr = op.Series(ctx)
	})
	return c.series, c.seriesErr
}

func (c *ChunkedRangeOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if c.evaluated {
		return 0, nil
	}
	c.evaluated = true

	evalTime := c.opts.Start.UnixMilli()
	rangeMs := c.node.Range.Milliseconds()
	chunkMs := c.node.ChunkSize.Milliseconds()
	rangeStart := evalTime - rangeMs

	// Merged results: seriesID → accumulated value.
	merged := make(map[uint64]float64)
	counts := make(map[uint64]int) // for avg: track how many chunks contributed

	// Evaluate aligned chunks.
	firstChunkEnd := ((rangeStart / chunkMs) + 1) * chunkMs
	for chunkEnd := firstChunkEnd; chunkEnd <= evalTime; chunkEnd += chunkMs {
		if err := c.evaluateAndMerge(ctx, chunkEnd, time.Duration(chunkMs)*time.Millisecond, merged, counts); err != nil {
			return 0, err
		}
	}

	// Trailing edge: if evalTime is not on a chunk boundary.
	lastAlignedChunk := (evalTime / chunkMs) * chunkMs
	if lastAlignedChunk < evalTime {
		trailingMs := evalTime - lastAlignedChunk
		if err := c.evaluateAndMerge(ctx, evalTime, time.Duration(trailingMs)*time.Millisecond, merged, counts); err != nil {
			return 0, err
		}
	}

	// For avg: divide accumulated sum by count.
	if c.node.MergeOp == "avg" {
		for id := range merged {
			if counts[id] > 0 {
				merged[id] /= float64(counts[id])
			}
		}
	}

	// Build output StepVector.
	buf[0].Reset(evalTime)
	for id, val := range merged {
		buf[0].AppendSample(id, val)
	}

	// Cleanup: remove cached entries that have fallen out of the range window.
	if c.cache != nil {
		c.cache.DeleteBefore(c.cacheKey, rangeStart)
	}

	return 1, nil
}

// evaluateAndMerge evaluates one chunk and merges into the accumulated result.
// For aligned chunks, it checks the cache first. The trailing edge is never cached.
func (c *ChunkedRangeOperator) evaluateAndMerge(ctx context.Context, timestamp int64, duration time.Duration, merged map[uint64]float64, counts map[uint64]int) error {
	isAligned := duration == c.node.ChunkSize

	// Try cache for aligned chunks.
	if isAligned && c.cache != nil {
		key := fmt.Sprintf("%s:%d", c.cacheKey, timestamp)
		if cached := c.cache.Get(key); cached != nil {
			// Cache hit — merge cached values.
			for i := 0; i+1 < len(cached); i += 2 {
				id := uint64(cached[i])
				val := cached[i+1]
				c.mergeValue(merged, counts, id, val)
			}
			return nil
		}
	}

	chunkOpts := &query.Options{
		Start:                    time.UnixMilli(timestamp),
		End:                      time.UnixMilli(timestamp),
		Step:                     0,
		StepsBatch:               1,
		LookbackDelta:            c.opts.LookbackDelta,
		ExtLookbackDelta:         c.opts.ExtLookbackDelta,
		NoStepSubqueryIntervalFn: c.opts.NoStepSubqueryIntervalFn,
		DecodingConcurrency:      c.opts.DecodingConcurrency,
		SampleTracker:            c.opts.SampleTracker,
	}

	chunkHints := c.hints
	chunkHints.Start = timestamp - duration.Milliseconds()
	chunkHints.End = timestamp
	chunkHints.Range = duration.Milliseconds()

	matrixNode := logicalplan.MatrixSelector{
		VectorSelector: c.node.VectorSelector,
		Range:          duration,
		OriginalString: c.node.VectorSelector.String() + "[" + duration.String() + "]",
	}
	funcNode := logicalplan.FunctionCall{Func: c.node.Func, Args: []logicalplan.Node{&matrixNode}}

	op, err := c.scanners.NewMatrixSelector(ctx, chunkOpts, chunkHints, matrixNode, funcNode)
	if err != nil {
		return err
	}

	// Get series for ID mapping.
	_, err = op.Series(ctx)
	if err != nil {
		return err
	}

	// Evaluate the chunk.
	chunkBuf := make([]model.StepVector, 1)
	n, err := op.Next(ctx, chunkBuf)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	// Merge chunk result into accumulated result.
	sv := chunkBuf[0]
	for i, id := range sv.SampleIDs {
		val := sv.Samples[i]
		c.mergeValue(merged, counts, id, val)
	}

	// Cache aligned chunk results for next evaluation.
	if isAligned && c.cache != nil && len(sv.SampleIDs) <= maxCacheableSeriesPerStep {
		// Store as flat [id, val, id, val, ...] pairs.
		flat := make([]float64, 0, len(sv.SampleIDs)*2)
		for i, id := range sv.SampleIDs {
			flat = append(flat, float64(id), sv.Samples[i])
		}
		key := fmt.Sprintf("%s:%d", c.cacheKey, timestamp)
		c.cache.Put(key, flat)
	}

	return nil
}

func (c *ChunkedRangeOperator) mergeValue(merged map[uint64]float64, counts map[uint64]int, id uint64, val float64) {
	existing, exists := merged[id]
	switch c.node.MergeOp {
	case "max":
		if !exists || val > existing {
			merged[id] = val
		}
	case "min":
		if !exists || val < existing {
			merged[id] = val
		}
	case "sum":
		merged[id] = existing + val
	case "avg":
		merged[id] = existing + val // accumulate sum, divide later
		counts[id]++
	case "last":
		merged[id] = val // last chunk's value wins
	default:
		if !exists {
			merged[id] = val
		}
	}
	if c.node.MergeOp != "avg" && !exists {
		_ = math.Inf(0) // just to use math import
	}
}
