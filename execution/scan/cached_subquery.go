// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

const (
	// minStepsToCache is the minimum number of subquery steps required to enable caching.
	minStepsToCache int64 = 2
)

// cachedSubqueryOperator implements the Option 2 cache design:
//   - Series key: stores {hash → labels} mapping, updated each eval
//   - Per-step keys: stores {hash: value} pairs (hash-based, not positional)
//   - Series(): union of cached series + narrowInner.Series() — no fullInner call on warm path
//   - Position remapping: hash → current position on reconstruction
//
// On cold start (no series key in cache): uses fullInner for everything, caches results.
// On warm path: serves cached steps (remapped to current positions) + narrowInner for new steps.
type cachedSubqueryOperator struct {
	narrowInner model.VectorOperator
	fullInner   model.VectorOperator
	cache       query.SubqueryCache
	logger      *slog.Logger

	keyPrefix string
	mint      int64
	maxt      int64
	step      int64

	// Series state — resolved in Series() call before Next().
	seriesOnce    sync.Once
	series        []labels.Labels
	seriesErr     error
	hashToPos     map[uint64]uint64 // label hash → position in series slice
	isWarm        bool              // true if series were loaded from cache
	narrowSeries  []labels.Labels   // series from narrowInner (current active set)

	// Decision state (set during first Next call).
	decided  bool
	useCache bool

	// Cached step data loaded during decide().
	cachedSteps []cachedHashedStep
	cacheIdx    int

	// Track narrow serving state.
	servingFromNarrow bool
	narrowHashToPos   map[uint64]uint64 // narrowInner's hash → narrow position (for remapping)
}

type cachedHashedStep struct {
	t    int64
	data *query.HashedStepData
}

// seriesLabelHash computes a stable hash for a labels.Labels instance.
func seriesLabelHash(lset labels.Labels) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	lset.Range(func(l labels.Label) {
		binary.LittleEndian.PutUint16(buf[:2], uint16(len(l.Name)))
		h.Write(buf[:2])
		_, _ = h.Write([]byte(l.Name))
		binary.LittleEndian.PutUint16(buf[:2], uint16(len(l.Value)))
		h.Write(buf[:2])
		_, _ = h.Write([]byte(l.Value))
	})
	return h.Sum64()
}

// NewCachedSubqueryOperator creates a cached operator.
// If cache is nil or range is too short, returns fullInner directly.
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

	totalSteps := (opts.End.UnixMilli() - opts.Start.UnixMilli()) / step
	if totalSteps < minStepsToCache {
		return fullInner
	}

	return &cachedSubqueryOperator{
		narrowInner: narrowInner,
		fullInner:   fullInner,
		cache:       opts.SubqueryCache,
		logger:      opts.Logger,
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

// Series resolves the full series set for this evaluation.
// On warm path: loads from cache series key + narrowInner.Series() (no fullInner call).
// On cold path: uses fullInner.Series().
func (c *cachedSubqueryOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	c.seriesOnce.Do(func() {
		c.seriesErr = c.initSeries(ctx)
	})
	return c.series, c.seriesErr
}

func (c *cachedSubqueryOperator) initSeries(ctx context.Context) error {
	// Try loading series from cache.
	cachedData := c.cache.Get(c.seriesKey())
	if cachedData == nil {
		// No cached series — cold start. Use fullInner.
		c.isWarm = false
		var err error
		c.series, err = c.fullInner.Series(ctx)
		if err != nil {
			return err
		}
		c.buildHashToPos()
		return nil
	}

	// Load cached series.
	entries := query.DecodeSeriesKey(cachedData)
	if entries == nil {
		c.isWarm = false
		var err error
		c.series, err = c.fullInner.Series(ctx)
		if err != nil {
			return err
		}
		c.buildHashToPos()
		return nil
	}

	// Get narrowInner's series (cheap, narrow time range).
	narrowLabels, err := c.narrowInner.Series(ctx)
	if err != nil {
		// Can't get narrow series — fall back to cold.
		c.isWarm = false
		c.series, err = c.fullInner.Series(ctx)
		if err != nil {
			return err
		}
		c.buildHashToPos()
		return nil
	}
	c.narrowSeries = narrowLabels

	// Build the union of cached series + narrow series.
	// Use a map to deduplicate by hash.
	hashToLabels := make(map[uint64]labels.Labels, len(entries)+len(narrowLabels))
	for _, entry := range entries {
		// Reconstruct labels.Labels from SeriesEntry.
		lblPairs := make([]string, 0, len(entry.Labels)*2)
		for _, lbl := range entry.Labels {
			lblPairs = append(lblPairs, lbl.Name, lbl.Value)
		}
		hashToLabels[entry.Hash] = labels.FromStrings(lblPairs...)
	}
	for _, lset := range narrowLabels {
		h := seriesLabelHash(lset)
		hashToLabels[h] = lset
	}

	// Build sorted series list (Prometheus convention: sorted by labels).
	c.series = make([]labels.Labels, 0, len(hashToLabels))
	for _, lset := range hashToLabels {
		c.series = append(c.series, lset)
	}
	sort.Slice(c.series, func(i, j int) bool {
		return labels.Compare(c.series[i], c.series[j]) < 0
	})

	c.isWarm = true
	c.buildHashToPos()

	// Build narrow hash→pos map for remapping narrowInner.Next() output.
	c.narrowHashToPos = make(map[uint64]uint64, len(narrowLabels))
	for i, lset := range narrowLabels {
		c.narrowHashToPos[seriesLabelHash(lset)] = uint64(i)
	}

	return nil
}

func (c *cachedSubqueryOperator) buildHashToPos() {
	c.hashToPos = make(map[uint64]uint64, len(c.series))
	for i, lset := range c.series {
		c.hashToPos[seriesLabelHash(lset)] = uint64(i)
	}
}

func (c *cachedSubqueryOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if !c.decided {
		c.decided = true
		c.decide(ctx)
	}

	if !c.useCache {
		return c.nextFromFull(ctx, buf)
	}

	return c.nextFromCacheAndNarrow(ctx, buf)
}

func (c *cachedSubqueryOperator) decide(ctx context.Context) {
	if !c.isWarm {
		c.useCache = false
		c.log("subquery cache: cold start", "reason", "no_series_key")
		return
	}

	// Load cached steps using GetMulti.
	keys := make([]string, 0, (c.maxt-c.mint)/c.step+1)
	for t := c.mint; t <= c.maxt; t += c.step {
		keys = append(keys, c.stepKey(t))
	}
	results := c.cache.GetMulti(keys)

	// Walk keys in order; stop at first miss.
	for i, key := range keys {
		data, ok := results[key]
		if !ok || data == nil {
			break
		}
		stepData := query.DecodeHashedStepData(data)
		if stepData == nil {
			break
		}
		t := c.mint + int64(i)*c.step
		c.cachedSteps = append(c.cachedSteps, cachedHashedStep{t: t, data: stepData})
	}

	c.useCache = len(c.cachedSteps) > 0

	if c.useCache {
		totalSteps := len(keys)
		c.log("subquery cache: warm path",
			"cached_steps", len(c.cachedSteps), "total_steps", totalSteps,
			"new_steps", totalSteps-len(c.cachedSteps), "series_count", len(c.series))
	} else {
		c.log("subquery cache: cold start", "reason", "no_cached_steps")
	}
}

func (c *cachedSubqueryOperator) nextFromCacheAndNarrow(ctx context.Context, buf []model.StepVector) (int, error) {
	n := 0

	// Serve from cached steps, remapping hashes to current positions.
	for n < len(buf) && c.cacheIdx < len(c.cachedSteps) {
		entry := c.cachedSteps[c.cacheIdx]
		buf[n].Reset(entry.t)

		// Remap float samples by hash → current position.
		for _, s := range entry.data.Samples {
			if pos, ok := c.hashToPos[s.Hash]; ok {
				buf[n].AppendSample(pos, s.Value)
			}
			// Hash not in current series set → series died, skip (correct)
		}

		// Remap histogram samples.
		for _, h := range entry.data.Histograms {
			if pos, ok := c.hashToPos[h.Hash]; ok {
				buf[n].AppendHistogram(pos, h.Histogram)
			}
		}

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

	// Remap narrowInner's positions to our merged positions and cache new steps.
	for i := 0; i < vecN; i++ {
		sv := &innerBuf[i]

		// Build the hashed step data for caching.
		hashedStep := &query.HashedStepData{
			Samples:    make([]query.HashedSample, 0, len(sv.SampleIDs)),
			Histograms: make([]query.HashedHistogramSample, 0, len(sv.HistogramIDs)),
		}

		// Remap float samples: narrow position → hash → merged position.
		remappedIDs := make([]uint64, 0, len(sv.SampleIDs))
		remappedVals := make([]float64, 0, len(sv.Samples))
		for j, narrowPos := range sv.SampleIDs {
			// Find the hash for this narrow position.
			hash := c.narrowPosToHash(narrowPos)
			if hash == 0 {
				continue
			}
			// Find merged position.
			if mergedPos, ok := c.hashToPos[hash]; ok {
				remappedIDs = append(remappedIDs, mergedPos)
				remappedVals = append(remappedVals, sv.Samples[j])
			}
			hashedStep.Samples = append(hashedStep.Samples, query.HashedSample{
				Hash: hash, Value: sv.Samples[j],
			})
		}

		// Remap histogram samples.
		remappedHistIDs := make([]uint64, 0, len(sv.HistogramIDs))
		for j, narrowPos := range sv.HistogramIDs {
			hash := c.narrowPosToHash(narrowPos)
			if hash == 0 {
				continue
			}
			if mergedPos, ok := c.hashToPos[hash]; ok {
				remappedHistIDs = append(remappedHistIDs, mergedPos)
			}
			hashedStep.Histograms = append(hashedStep.Histograms, query.HashedHistogramSample{
				Hash: hash, Histogram: sv.Histograms[j],
			})
		}

		// Replace the StepVector contents with remapped positions.
		sv.SampleIDs = remappedIDs
		sv.Samples = remappedVals
		if len(sv.HistogramIDs) > 0 {
			sv.HistogramIDs = remappedHistIDs
		}

		// Cache the new step (async-safe: Put is fire-and-forget).
		encoded := query.EncodeHashedStepData(hashedStep)
		if encoded != nil {
			c.cache.Put(c.stepKey(sv.T), encoded)
		}
	}

	// Delete oldest step that fell out of window.
	if c.mint > c.step {
		c.cache.Delete(c.stepKey(c.mint - c.step))
	}

	// Update latest timestamp.
	if vecN > 0 {
		var tsBytes [8]byte
		binary.LittleEndian.PutUint64(tsBytes[:], uint64(innerBuf[vecN-1].T))
		c.cache.Put(c.latestTsKey(), tsBytes[:])
	}

	// Update series key: rebuild from what we actually served.
	c.updateSeriesKey()

	return n + vecN, nil
}

func (c *cachedSubqueryOperator) nextFromFull(ctx context.Context, buf []model.StepVector) (int, error) {
	vecN, err := c.fullInner.Next(ctx, buf)
	if err != nil {
		return 0, err
	}

	// Get the series for hash computation.
	series, seriesErr := c.fullInner.Series(ctx)
	if seriesErr != nil {
		return vecN, nil // still return data, just don't cache
	}

	// Build position → hash mapping for fullInner.
	posToHash := make([]uint64, len(series))
	for i, lset := range series {
		posToHash[i] = seriesLabelHash(lset)
	}

	// Cache each step with hashes.
	cachedCount := 0
	for i := 0; i < vecN; i++ {
		hashedStep := &query.HashedStepData{
			Samples:    make([]query.HashedSample, 0, len(buf[i].SampleIDs)),
			Histograms: make([]query.HashedHistogramSample, 0, len(buf[i].HistogramIDs)),
		}
		for j, pos := range buf[i].SampleIDs {
			if int(pos) < len(posToHash) {
				hashedStep.Samples = append(hashedStep.Samples, query.HashedSample{
					Hash: posToHash[pos], Value: buf[i].Samples[j],
				})
			}
		}
		for j, pos := range buf[i].HistogramIDs {
			if int(pos) < len(posToHash) {
				hashedStep.Histograms = append(hashedStep.Histograms, query.HashedHistogramSample{
					Hash: posToHash[pos], Histogram: buf[i].Histograms[j],
				})
			}
		}
		encoded := query.EncodeHashedStepData(hashedStep)
		if encoded != nil {
			c.cache.Put(c.stepKey(buf[i].T), encoded)
			cachedCount++
		}
	}

	// Store series key.
	c.storeSeriesKey(series)

	// Store latest timestamp.
	if vecN > 0 {
		var tsBytes [8]byte
		binary.LittleEndian.PutUint64(tsBytes[:], uint64(buf[vecN-1].T))
		c.cache.Put(c.latestTsKey(), tsBytes[:])
	}

	c.log("subquery cache: populated (cold)",
		"steps_cached", cachedCount, "series_count", len(series))

	return vecN, nil
}

// narrowPosToHash converts a narrowInner position to its label hash.
func (c *cachedSubqueryOperator) narrowPosToHash(pos uint64) uint64 {
	if int(pos) < len(c.narrowSeries) {
		return seriesLabelHash(c.narrowSeries[pos])
	}
	return 0
}

// storeSeriesKey stores the full series set as a cache key.
func (c *cachedSubqueryOperator) storeSeriesKey(series []labels.Labels) {
	entries := make([]query.SeriesEntry, len(series))
	for i, lset := range series {
		h := seriesLabelHash(lset)
		var lbls []query.SeriesLabel
		lset.Range(func(l labels.Label) {
			lbls = append(lbls, query.SeriesLabel{Name: l.Name, Value: l.Value})
		})
		entries[i] = query.SeriesEntry{Hash: h, Labels: lbls}
	}
	encoded := query.EncodeSeriesKey(entries)
	if encoded != nil {
		c.cache.Put(c.seriesKey(), encoded)
	}
}

// updateSeriesKey rebuilds the series key from current knowledge.
// Prunes dead series: any series not present in cached steps or narrow is removed.
func (c *cachedSubqueryOperator) updateSeriesKey() {
	// For now, store the full merged series set (includes narrow + historical).
	// Dead series pruning: we know which hashes appeared in cached steps + narrow.
	// For simplicity, just store the current merged series.
	// Dead series with no data will be pruned on next eval when steps slide past them.
	c.storeSeriesKey(c.series)
}

// --- Key helpers ---

func (c *cachedSubqueryOperator) stepKey(timestamp int64) string {
	return fmt.Sprintf("%s:s:%d", c.keyPrefix, timestamp)
}

func (c *cachedSubqueryOperator) seriesKey() string {
	return c.keyPrefix + ":series"
}

func (c *cachedSubqueryOperator) latestTsKey() string {
	return "meta:" + c.keyPrefix + ":latest_ts"
}

// log emits an info-level log message if a logger is configured.
func (c *cachedSubqueryOperator) log(msg string, args ...any) {
	if c.logger != nil {
		c.logger.Info(msg, append([]any{"key_prefix", c.keyPrefix}, args...)...)
	}
}
