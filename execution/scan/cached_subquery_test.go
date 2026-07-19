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
