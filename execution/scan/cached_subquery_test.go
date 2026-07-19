// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package scan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// mockInnerOperator produces predictable StepVectors for testing.
type mockInnerOperator struct {
	steps   []model.StepVector
	current int
	series  []labels.Labels
	called  bool
}

func (m *mockInnerOperator) Next(_ context.Context, buf []model.StepVector) (int, error) {
	m.called = true
	if m.current >= len(m.steps) {
		return 0, nil
	}
	n := min(len(buf), len(m.steps)-m.current)
	for i := 0; i < n; i++ {
		buf[i] = m.steps[m.current+i]
	}
	m.current += n
	return n, nil
}

func (m *mockInnerOperator) Series(_ context.Context) ([]labels.Labels, error) {
	return m.series, nil
}

func (m *mockInnerOperator) Explain() []model.VectorOperator { return nil }
func (m *mockInnerOperator) String() string                  { return "mockInner" }

func TestCachedSubqueryOperator_ColdStart(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 42}

	series := []labels.Labels{
		labels.FromStrings("pod", "a"),
		labels.FromStrings("pod", "b"),
	}

	fullInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0, 1}, Samples: []float64{10.0, 20.0}},
			{T: 2000, SampleIDs: []uint64{0, 1}, Samples: []float64{11.0, 21.0}},
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{12.0, 22.0}},
		},
		series: series,
	}
	narrowInner := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: series,
	}

	opts := &query.Options{
		Start:         time.UnixMilli(1000),
		End:           time.UnixMilli(3000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)

	// Series() should work.
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(s))

	// Next() should use fullInner (cold start).
	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)
	testutil.Equals(t, true, fullInner.called)

	// Verify steps are cached.
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))
	testutil.Assert(t, cache.Get(keyPrefix+":s:1000") != nil)
	testutil.Assert(t, cache.Get(keyPrefix+":s:2000") != nil)
	testutil.Assert(t, cache.Get(keyPrefix+":s:3000") != nil)
	// Series key should be cached.
	testutil.Assert(t, cache.Get(keyPrefix+":series") != nil)
}

func TestCachedSubqueryOperator_WarmPath(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 42}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	series := []labels.Labels{
		labels.FromStrings("pod", "a"),
		labels.FromStrings("pod", "b"),
	}

	// Pre-populate cache: simulate previous cold start cached steps T=1000, T=2000.
	hashA := seriesLabelHash(series[0])
	hashB := seriesLabelHash(series[1])

	// Store series key.
	seriesEntries := []query.SeriesEntry{
		{Hash: hashA, Labels: []query.SeriesLabel{{Name: "pod", Value: "a"}}},
		{Hash: hashB, Labels: []query.SeriesLabel{{Name: "pod", Value: "b"}}},
	}
	cache.Put(keyPrefix+":series", query.EncodeSeriesKey(seriesEntries))

	// Store step data with hashes.
	step1 := &query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashA, Value: 10.0}, {Hash: hashB, Value: 20.0}},
	}
	step2 := &query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashA, Value: 11.0}, {Hash: hashB, Value: 21.0}},
	}
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(step1))
	cache.Put(keyPrefix+":s:2000", query.EncodeHashedStepData(step2))

	// This evaluation needs steps [1000, 2000, 3000].
	// Steps 1000, 2000 are cached. Step 3000 is new from narrowInner.
	fullInner := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: series,
	}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{12.0, 22.0}},
		},
		series: series,
	}

	opts := &query.Options{
		Start:         time.UnixMilli(1000),
		End:           time.UnixMilli(3000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)

	// Series() should load from cache.
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(s))

	// Next() should use warm path.
	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)

	// fullInner should NOT have been called (warm path).
	testutil.Equals(t, false, fullInner.called)
	// narrowInner should have been called for new step.
	testutil.Equals(t, true, narrowInner.called)

	// Verify data correctness.
	testutil.Equals(t, int64(1000), buf[0].T)
	testutil.Equals(t, int64(2000), buf[1].T)
	testutil.Equals(t, int64(3000), buf[2].T)

	// New step should be cached.
	testutil.Assert(t, cache.Get(keyPrefix+":s:3000") != nil)
}

func TestCachedSubqueryOperator_SeriesChurn(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 42}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	// Previous eval had series [A, B]. Now B is dead and C is new.
	seriesOld := []labels.Labels{
		labels.FromStrings("pod", "a"),
		labels.FromStrings("pod", "b"),
	}
	seriesNarrow := []labels.Labels{
		labels.FromStrings("pod", "a"),
		labels.FromStrings("pod", "c"), // new series
	}

	hashA := seriesLabelHash(seriesOld[0])
	hashB := seriesLabelHash(seriesOld[1])
	hashC := seriesLabelHash(seriesNarrow[1])

	// Pre-populate cache with old series.
	seriesEntries := []query.SeriesEntry{
		{Hash: hashA, Labels: []query.SeriesLabel{{Name: "pod", Value: "a"}}},
		{Hash: hashB, Labels: []query.SeriesLabel{{Name: "pod", Value: "b"}}},
	}
	cache.Put(keyPrefix+":series", query.EncodeSeriesKey(seriesEntries))

	// Cached steps have data for A and B.
	step1 := &query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashA, Value: 10.0}, {Hash: hashB, Value: 20.0}},
	}
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(step1))

	// narrowInner has A and C (B died, C is new).
	fullInner := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: append(seriesOld, seriesNarrow[1]), // not used on warm
	}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 2000, SampleIDs: []uint64{0, 1}, Samples: []float64{11.0, 31.0}}, // A=11, C=31
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{12.0, 32.0}}, // A=12, C=32
		},
		series: seriesNarrow, // [A, C]
	}

	opts := &query.Options{
		Start:         time.UnixMilli(1000),
		End:           time.UnixMilli(3000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)

	// Series() should return union [A, B, C] (B from cache, C from narrow).
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 3, len(s))

	// Find positions.
	posA := uint64(0)
	posB := uint64(0)
	posC := uint64(0)
	for i, lset := range s {
		h := seriesLabelHash(lset)
		switch h {
		case hashA:
			posA = uint64(i)
		case hashB:
			posB = uint64(i)
		case hashC:
			posC = uint64(i)
		}
	}
	_ = posB // B exists in series but may have no data in new steps

	// Next() should serve cached step T=1000 (has A and B data) + narrow steps T=2000,T=3000 (have A and C).
	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n) // 3 steps: 1 cached + 2 narrow

	// Step T=1000 (from cache): A should be present, B should be present (historical).
	found := make(map[uint64]float64)
	for j, id := range buf[0].SampleIDs {
		found[id] = buf[0].Samples[j]
	}
	testutil.Equals(t, 10.0, found[posA])
	testutil.Equals(t, 20.0, found[posB]) // B's historical data preserved

	// Step T=2000 (from narrow): A and C should be present, remapped to merged positions.
	found2 := make(map[uint64]float64)
	for j, id := range buf[1].SampleIDs {
		found2[id] = buf[1].Samples[j]
	}
	testutil.Equals(t, 11.0, found2[posA])
	testutil.Equals(t, 31.0, found2[posC])

	// fullInner should NOT have been called.
	testutil.Equals(t, false, fullInner.called)
}

func TestCachedSubqueryOperator_NilCache(t *testing.T) {
	fullInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0}, Samples: []float64{9.0}},
		},
		series: []labels.Labels{labels.FromStrings("a", "1")},
	}

	opts := &query.Options{
		Start:         time.UnixMilli(1000),
		End:           time.UnixMilli(3000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: nil, // no cache
		TenantID:      "tenant1",
	}

	// Should return fullInner directly.
	op := NewCachedSubqueryOperator(fullInner, nil, opts, &logicalplan.NumberLiteral{Val: 1})
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 1, len(s))

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 1, n)
}

// TestCachedSubqueryOperator_MultiEvalSimulation simulates a full ruler lifecycle:
// cold start → warm → warm, verifying correctness across a sliding window.
func TestCachedSubqueryOperator_MultiEvalSimulation(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 99}

	series := []labels.Labels{
		labels.FromStrings("cluster", "a"),
		labels.FromStrings("cluster", "b"),
	}

	// --- Eval 1: Cold start. Range [1000, 5000], step=1000 ---
	fullInner1 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0, 1}, Samples: []float64{10.0, 20.0}},
			{T: 2000, SampleIDs: []uint64{0, 1}, Samples: []float64{11.0, 21.0}},
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{12.0, 22.0}},
			{T: 4000, SampleIDs: []uint64{0, 1}, Samples: []float64{13.0, 23.0}},
			{T: 5000, SampleIDs: []uint64{0, 1}, Samples: []float64{14.0, 24.0}},
		},
		series: series,
	}
	narrowInner1 := &mockInnerOperator{steps: []model.StepVector{}, series: series}

	opts1 := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(5000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op1 := NewCachedSubqueryOperator(fullInner1, narrowInner1, opts1, innerExpr)
	_, err := op1.Series(context.Background())
	testutil.Ok(t, err)
	buf := make([]model.StepVector, 10)
	n, err := op1.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, n)
	testutil.Equals(t, true, fullInner1.called)

	// --- Eval 2: Warm. Range [2000, 6000]. Steps 2000-5000 cached, 6000 new ---
	fullInner2 := &mockInnerOperator{steps: []model.StepVector{}, series: series}
	narrowInner2 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 6000, SampleIDs: []uint64{0, 1}, Samples: []float64{15.0, 25.0}},
		},
		series: series,
	}

	opts2 := &query.Options{
		Start: time.UnixMilli(2000), End: time.UnixMilli(6000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op2 := NewCachedSubqueryOperator(fullInner2, narrowInner2, opts2, innerExpr)
	s2, err := op2.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(s2))

	buf2 := make([]model.StepVector, 10)
	n, err = op2.Next(context.Background(), buf2)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, n)
	testutil.Equals(t, false, fullInner2.called)  // warm path!
	testutil.Equals(t, true, narrowInner2.called) // fetched new step

	// Verify values: steps 2000-5000 from cache, 6000 from narrow.
	testutil.Equals(t, int64(2000), buf2[0].T)
	testutil.Equals(t, int64(6000), buf2[4].T)

	// --- Eval 3: Warm. Range [3000, 7000]. Steps 3000-6000 cached, 7000 new ---
	fullInner3 := &mockInnerOperator{steps: []model.StepVector{}, series: series}
	narrowInner3 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 7000, SampleIDs: []uint64{0, 1}, Samples: []float64{16.0, 26.0}},
		},
		series: series,
	}

	opts3 := &query.Options{
		Start: time.UnixMilli(3000), End: time.UnixMilli(7000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op3 := NewCachedSubqueryOperator(fullInner3, narrowInner3, opts3, innerExpr)
	_, err = op3.Series(context.Background())
	testutil.Ok(t, err)

	buf3 := make([]model.StepVector, 10)
	n, err = op3.Next(context.Background(), buf3)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, n)
	testutil.Equals(t, false, fullInner3.called)
	testutil.Equals(t, true, narrowInner3.called)
	testutil.Equals(t, int64(3000), buf3[0].T)
	testutil.Equals(t, int64(7000), buf3[4].T)
}

// TestCachedSubqueryOperator_DeadSeriesHistoricalData verifies that a series which died
// (not in narrowInner) still has its historical cached data served correctly.
func TestCachedSubqueryOperator_DeadSeriesHistoricalData(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 55}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	seriesA := labels.FromStrings("pod", "a")
	seriesB := labels.FromStrings("pod", "b") // will be dead
	hashA := seriesLabelHash(seriesA)
	hashB := seriesLabelHash(seriesB)

	// Cache has series A and B, with data for both at T=1000 and T=2000.
	seriesEntries := []query.SeriesEntry{
		{Hash: hashA, Labels: []query.SeriesLabel{{Name: "pod", Value: "a"}}},
		{Hash: hashB, Labels: []query.SeriesLabel{{Name: "pod", Value: "b"}}},
	}
	cache.Put(keyPrefix+":series", query.EncodeSeriesKey(seriesEntries))
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(&query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashA, Value: 10.0}, {Hash: hashB, Value: 20.0}},
	}))
	cache.Put(keyPrefix+":s:2000", query.EncodeHashedStepData(&query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashA, Value: 11.0}, {Hash: hashB, Value: 21.0}},
	}))

	// narrowInner only sees A (B is dead).
	narrowSeries := []labels.Labels{seriesA}
	fullInner := &mockInnerOperator{steps: []model.StepVector{}, series: []labels.Labels{seriesA, seriesB}}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 3000, SampleIDs: []uint64{0}, Samples: []float64{12.0}}, // only A
		},
		series: narrowSeries,
	}

	opts := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(3000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)

	// Series should include both A and B (B has cached historical data).
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 2, len(s))

	// Get positions.
	cOp := op.(*cachedSubqueryOperator)
	posA := cOp.hashToPos[hashA]
	posB := cOp.hashToPos[hashB]

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)

	// Step T=1000: both A and B should have data (from cache).
	samplesT1000 := make(map[uint64]float64)
	for j, id := range buf[0].SampleIDs {
		samplesT1000[id] = buf[0].Samples[j]
	}
	testutil.Equals(t, 10.0, samplesT1000[posA])
	testutil.Equals(t, 20.0, samplesT1000[posB])

	// Step T=3000: only A should have data (from narrow, B is dead).
	samplesT3000 := make(map[uint64]float64)
	for j, id := range buf[2].SampleIDs {
		samplesT3000[id] = buf[2].Samples[j]
	}
	testutil.Equals(t, 12.0, samplesT3000[posA])
	_, bExists := samplesT3000[posB]
	testutil.Equals(t, false, bExists) // B has no data at T=3000

	// fullInner should NOT have been called.
	testutil.Equals(t, false, fullInner.called)
}

// TestCachedSubqueryOperator_SharedCache verifies that two rules with the same inner
// expression share cache entries (same keyPrefix).
func TestCachedSubqueryOperator_SharedCache(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	// Same inner expression for both rules.
	innerExpr := &logicalplan.NumberLiteral{Val: 77}

	series := []labels.Labels{labels.FromStrings("ns", "prod")}

	// Rule 1: cold start, populates cache.
	fullInner1 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0}, Samples: []float64{5.0}},
			{T: 2000, SampleIDs: []uint64{0}, Samples: []float64{6.0}},
			{T: 3000, SampleIDs: []uint64{0}, Samples: []float64{7.0}},
		},
		series: series,
	}
	narrowInner1 := &mockInnerOperator{steps: []model.StepVector{}, series: series}

	opts := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(3000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op1 := NewCachedSubqueryOperator(fullInner1, narrowInner1, opts, innerExpr)
	_, _ = op1.Series(context.Background())
	buf := make([]model.StepVector, 10)
	n, _ := op1.Next(context.Background(), buf)
	testutil.Equals(t, 3, n)
	testutil.Equals(t, true, fullInner1.called)

	// Rule 2: same expression, same time range — should hit cache (warm path).
	fullInner2 := &mockInnerOperator{steps: []model.StepVector{}, series: series}
	narrowInner2 := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: series,
	}

	op2 := NewCachedSubqueryOperator(fullInner2, narrowInner2, opts, innerExpr)
	_, _ = op2.Series(context.Background())
	buf2 := make([]model.StepVector, 10)
	n, _ = op2.Next(context.Background(), buf2)
	testutil.Equals(t, 3, n)
	testutil.Equals(t, false, fullInner2.called) // cache hit — no full fetch!

	// Verify same values.
	testutil.Equals(t, []float64{5.0}, buf2[0].Samples)
	testutil.Equals(t, []float64{6.0}, buf2[1].Samples)
	testutil.Equals(t, []float64{7.0}, buf2[2].Samples)
}

// TestCachedSubqueryOperator_SeriesKeyEvicted verifies graceful fallback to cold start
// when the series key is evicted but step keys might still exist.
func TestCachedSubqueryOperator_SeriesKeyEvicted(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 33}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	series := []labels.Labels{labels.FromStrings("x", "1")}
	hashX := seriesLabelHash(series[0])

	// Step keys exist but series key is missing (evicted).
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(&query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashX, Value: 99.0}},
	}))
	// NOTE: No series key stored — simulates LRU eviction.

	fullInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0}, Samples: []float64{99.0}},
			{T: 2000, SampleIDs: []uint64{0}, Samples: []float64{100.0}},
			{T: 3000, SampleIDs: []uint64{0}, Samples: []float64{101.0}},
		},
		series: series,
	}
	narrowInner := &mockInnerOperator{steps: []model.StepVector{}, series: series}

	opts := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(3000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)
	_, err := op.Series(context.Background())
	testutil.Ok(t, err)

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)
	// Should fall back to fullInner (cold start) since series key was evicted.
	testutil.Equals(t, true, fullInner.called)
}

// TestCachedSubqueryOperator_NarrowPositionRemapping verifies that narrowInner's
// positions are correctly remapped to the merged Series() positions when the
// narrow series set has different ordering.
func TestCachedSubqueryOperator_NarrowPositionRemapping(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 88}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	// Cached series: [A, B, C] in sorted order.
	seriesA := labels.FromStrings("pod", "a")
	seriesB := labels.FromStrings("pod", "b")
	seriesC := labels.FromStrings("pod", "c")
	hashA := seriesLabelHash(seriesA)
	hashB := seriesLabelHash(seriesB)
	hashC := seriesLabelHash(seriesC)

	seriesEntries := []query.SeriesEntry{
		{Hash: hashA, Labels: []query.SeriesLabel{{Name: "pod", Value: "a"}}},
		{Hash: hashB, Labels: []query.SeriesLabel{{Name: "pod", Value: "b"}}},
		{Hash: hashC, Labels: []query.SeriesLabel{{Name: "pod", Value: "c"}}},
	}
	cache.Put(keyPrefix+":series", query.EncodeSeriesKey(seriesEntries))

	// Cached step at T=1000 has all three.
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(&query.HashedStepData{
		Samples: []query.HashedSample{
			{Hash: hashA, Value: 1.0},
			{Hash: hashB, Value: 2.0},
			{Hash: hashC, Value: 3.0},
		},
	}))

	// narrowInner returns [B, C] only (A might be temporarily absent in narrow window).
	// Note: narrow positions are 0=B, 1=C.
	narrowSeries := []labels.Labels{seriesB, seriesC}
	fullInner := &mockInnerOperator{steps: []model.StepVector{}, series: []labels.Labels{seriesA, seriesB, seriesC}}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 2000, SampleIDs: []uint64{0, 1}, Samples: []float64{20.0, 30.0}}, // narrow pos 0=B, 1=C
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{21.0, 31.0}}, // narrow pos 0=B, 1=C
		},
		series: narrowSeries,
	}

	opts := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(3000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, 3, len(s)) // A, B, C all present (A from cache, B/C from both)

	// Get merged positions.
	cOp := op.(*cachedSubqueryOperator)
	posA := cOp.hashToPos[hashA]
	posB := cOp.hashToPos[hashB]
	posC := cOp.hashToPos[hashC]

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n) // 3 steps: 1 cached + 2 narrow

	// Step T=1000 (from cache): all three series present.
	t1000 := make(map[uint64]float64)
	for j, id := range buf[0].SampleIDs {
		t1000[id] = buf[0].Samples[j]
	}
	testutil.Equals(t, 1.0, t1000[posA])
	testutil.Equals(t, 2.0, t1000[posB])
	testutil.Equals(t, 3.0, t1000[posC])

	// Step T=2000 (from narrow, remapped): B and C present at MERGED positions.
	t2000 := make(map[uint64]float64)
	for j, id := range buf[1].SampleIDs {
		t2000[id] = buf[1].Samples[j]
	}
	// B's value (20.0) should be at merged posB, not narrow pos 0.
	testutil.Equals(t, 20.0, t2000[posB])
	// C's value (30.0) should be at merged posC, not narrow pos 1.
	testutil.Equals(t, 30.0, t2000[posC])
	// A should NOT be present (narrow didn't have A).
	_, aExists := t2000[posA]
	testutil.Equals(t, false, aExists)

	testutil.Equals(t, false, fullInner.called)
}

// TestCachedSubqueryOperator_SecondNextReturnsZero verifies the operator terminates
// correctly after all steps are exhausted.
func TestCachedSubqueryOperator_SecondNextReturnsZero(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 11}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	series := []labels.Labels{labels.FromStrings("m", "1")}
	hashM := seriesLabelHash(series[0])

	// Pre-populate cache.
	seriesEntries := []query.SeriesEntry{
		{Hash: hashM, Labels: []query.SeriesLabel{{Name: "m", Value: "1"}}},
	}
	cache.Put(keyPrefix+":series", query.EncodeSeriesKey(seriesEntries))
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(&query.HashedStepData{
		Samples: []query.HashedSample{{Hash: hashM, Value: 5.0}},
	}))

	fullInner := &mockInnerOperator{steps: []model.StepVector{}, series: series}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 2000, SampleIDs: []uint64{0}, Samples: []float64{6.0}},
			{T: 3000, SampleIDs: []uint64{0}, Samples: []float64{7.0}},
		},
		series: series,
	}

	opts := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(3000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)
	_, _ = op.Series(context.Background())

	// First Next: returns 3 steps (1 cached + 2 narrow).
	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)

	// Second Next: should return 0 (no more data).
	n, err = op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 0, n)
}

// TestCachedSubqueryOperator_HighCardinality tests with many series to ensure
// hash-based remapping works at scale.
func TestCachedSubqueryOperator_HighCardinality(t *testing.T) {
	cache := query.NewMockSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 500}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	numSeries := 1000
	series := make([]labels.Labels, numSeries)
	entries := make([]query.SeriesEntry, numSeries)
	for i := 0; i < numSeries; i++ {
		lset := labels.FromStrings("pod", fmt.Sprintf("pod-%04d", i))
		series[i] = lset
		h := seriesLabelHash(lset)
		entries[i] = query.SeriesEntry{
			Hash:   h,
			Labels: []query.SeriesLabel{{Name: "pod", Value: fmt.Sprintf("pod-%04d", i)}},
		}
	}

	// Store series key.
	cache.Put(keyPrefix+":series", query.EncodeSeriesKey(entries))

	// Store a cached step with all 1000 series.
	samples := make([]query.HashedSample, numSeries)
	for i := 0; i < numSeries; i++ {
		samples[i] = query.HashedSample{Hash: entries[i].Hash, Value: float64(i)}
	}
	cache.Put(keyPrefix+":s:1000", query.EncodeHashedStepData(&query.HashedStepData{Samples: samples}))

	// narrowInner returns same series with new steps.
	narrowSamples := make([]uint64, numSeries)
	narrowVals := make([]float64, numSeries)
	for i := 0; i < numSeries; i++ {
		narrowSamples[i] = uint64(i)
		narrowVals[i] = float64(i + 1000)
	}
	narrowSamples2 := make([]uint64, numSeries)
	narrowVals2 := make([]float64, numSeries)
	for i := 0; i < numSeries; i++ {
		narrowSamples2[i] = uint64(i)
		narrowVals2[i] = float64(i + 2000)
	}

	fullInner := &mockInnerOperator{steps: []model.StepVector{}, series: series}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 2000, SampleIDs: narrowSamples, Samples: narrowVals},
			{T: 3000, SampleIDs: narrowSamples2, Samples: narrowVals2},
		},
		series: series,
	}

	opts := &query.Options{
		Start: time.UnixMilli(1000), End: time.UnixMilli(3000),
		Step: time.Millisecond * 1000, StepsBatch: 10,
		SubqueryCache: cache, TenantID: "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)
	s, err := op.Series(context.Background())
	testutil.Ok(t, err)
	testutil.Equals(t, numSeries, len(s))

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n) // 3 steps: 1 cached + 2 narrow

	// Verify step T=1000 has 1000 samples.
	testutil.Equals(t, numSeries, len(buf[0].SampleIDs))
	// Verify step T=2000 has 1000 samples.
	testutil.Equals(t, numSeries, len(buf[1].SampleIDs))
	// Verify step T=3000 has 1000 samples.
	testutil.Equals(t, numSeries, len(buf[2].SampleIDs))

	// fullInner not called (warm path).
	testutil.Equals(t, false, fullInner.called)
}
