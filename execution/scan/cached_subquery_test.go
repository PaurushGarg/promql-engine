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
	cache := query.NewLocalSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 42}

	fullInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0, 1}, Samples: []float64{1.0, 2.0}},
			{T: 2000, SampleIDs: []uint64{0, 1}, Samples: []float64{3.0, 4.0}},
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{5.0, 6.0}},
		},
		series: []labels.Labels{labels.FromStrings("a", "1")},
	}
	narrowInner := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: []labels.Labels{labels.FromStrings("a", "1")},
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

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)
	testutil.Equals(t, []float64{1.0, 2.0}, buf[0].Samples)
	testutil.Equals(t, []float64{3.0, 4.0}, buf[1].Samples)
	testutil.Equals(t, []float64{5.0, 6.0}, buf[2].Samples)

	// fullInner should have been used.
	testutil.Equals(t, true, fullInner.called)
	// narrowInner should NOT have been used.
	testutil.Equals(t, false, narrowInner.called)

	// Cache should be populated.
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))
	testutil.Assert(t, cache.Get(keyPrefix+":1000") != nil)
	testutil.Assert(t, cache.Get(keyPrefix+":2000") != nil)
	testutil.Assert(t, cache.Get(keyPrefix+":3000") != nil)
}

func TestCachedSubqueryOperator_WarmCache(t *testing.T) {
	cache := query.NewLocalSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 42}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	// Pre-populate cache (simulating previous evaluation).
	cache.Put(keyPrefix+":2000", []float64{3.0, 4.0})
	cache.Put(keyPrefix+":3000", []float64{5.0, 6.0})

	// This evaluation needs steps [2000, 3000, 4000].
	// Steps 2000, 3000 are cached. Step 4000 is new.
	fullInner := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: []labels.Labels{labels.FromStrings("a", "1")},
	}
	narrowInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 4000, SampleIDs: []uint64{0, 1}, Samples: []float64{7.0, 8.0}},
		},
		series: []labels.Labels{labels.FromStrings("a", "1")},
	}

	opts := &query.Options{
		Start:         time.UnixMilli(2000),
		End:           time.UnixMilli(4000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, innerExpr)

	buf := make([]model.StepVector, 10)
	n, err := op.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 3, n)
	testutil.Equals(t, []float64{3.0, 4.0}, buf[0].Samples)
	testutil.Equals(t, []float64{5.0, 6.0}, buf[1].Samples)
	testutil.Equals(t, []float64{7.0, 8.0}, buf[2].Samples)

	// fullInner should NOT have been used.
	testutil.Equals(t, false, fullInner.called)
	// narrowInner should have been used for the new step.
	testutil.Equals(t, true, narrowInner.called)

	// New step should be cached.
	testutil.Assert(t, cache.Get(keyPrefix+":4000") != nil)
}

func TestCachedSubqueryOperator_NilCache(t *testing.T) {
	fullInner := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0}, Samples: []float64{9.0}},
		},
	}
	narrowInner := &mockInnerOperator{}

	opts := &query.Options{
		Start:         time.UnixMilli(1000),
		End:           time.UnixMilli(1000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: nil,
		TenantID:      "tenant1",
	}

	op := NewCachedSubqueryOperator(fullInner, narrowInner, opts, &logicalplan.NumberLiteral{Val: 1})

	// Should return fullInner directly (no wrapping).
	_, isInner := op.(*mockInnerOperator)
	testutil.Equals(t, true, isInner)
}

// TestCachedSubqueryOperator_RulerSimulation simulates multiple ruler evaluations
// with the same cache instance, verifying the full lifecycle:
// 1. Cold start (full inner used)
// 2. Warm evaluation (cache hit + narrow inner for new step)
// 3. Delayed evaluation (cache hit + narrow inner for multiple new steps)
func TestCachedSubqueryOperator_RulerSimulation(t *testing.T) {
	cache := query.NewLocalSubqueryCache()
	innerExpr := &logicalplan.NumberLiteral{Val: 99}
	keyPrefix := fmt.Sprintf("sq:tenant1:%016x", logicalplan.NodeFingerprint(innerExpr))

	// --- Evaluation 1: Cold start (range [1000, 5000], step=1000) ---
	fullInner1 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 1000, SampleIDs: []uint64{0, 1}, Samples: []float64{10.0, 20.0}},
			{T: 2000, SampleIDs: []uint64{0, 1}, Samples: []float64{11.0, 21.0}},
			{T: 3000, SampleIDs: []uint64{0, 1}, Samples: []float64{12.0, 22.0}},
			{T: 4000, SampleIDs: []uint64{0, 1}, Samples: []float64{13.0, 23.0}},
			{T: 5000, SampleIDs: []uint64{0, 1}, Samples: []float64{14.0, 24.0}},
		},
		series: []labels.Labels{labels.FromStrings("cluster", "a"), labels.FromStrings("cluster", "b")},
	}
	narrowInner1 := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: []labels.Labels{labels.FromStrings("cluster", "a"), labels.FromStrings("cluster", "b")},
	}

	opts1 := &query.Options{
		Start:         time.UnixMilli(1000),
		End:           time.UnixMilli(5000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op1 := NewCachedSubqueryOperator(fullInner1, narrowInner1, opts1, innerExpr)
	buf := make([]model.StepVector, 10)
	n, err := op1.Next(context.Background(), buf)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, n)
	testutil.Equals(t, true, fullInner1.called)   // Full inner was used (cold start)
	testutil.Equals(t, false, narrowInner1.called) // Narrow was not used

	// Verify cache is populated.
	testutil.Assert(t, cache.Get(keyPrefix+":1000") != nil)
	testutil.Assert(t, cache.Get(keyPrefix+":5000") != nil)
	testutil.Equals(t, int64(5000), cache.GetLatestTimestamp(keyPrefix))

	// --- Evaluation 2: Warm (range [2000, 6000], step=1000) ---
	// Steps 2000-5000 should be cached. Step 6000 is new.
	fullInner2 := &mockInnerOperator{
		steps:  []model.StepVector{}, // Should not be used
		series: []labels.Labels{labels.FromStrings("cluster", "a"), labels.FromStrings("cluster", "b")},
	}
	narrowInner2 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 6000, SampleIDs: []uint64{0, 1}, Samples: []float64{15.0, 25.0}},
		},
		series: []labels.Labels{labels.FromStrings("cluster", "a"), labels.FromStrings("cluster", "b")},
	}

	opts2 := &query.Options{
		Start:         time.UnixMilli(2000),
		End:           time.UnixMilli(6000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op2 := NewCachedSubqueryOperator(fullInner2, narrowInner2, opts2, innerExpr)
	buf2 := make([]model.StepVector, 10)
	n, err = op2.Next(context.Background(), buf2)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, n)

	// Verify cached steps served correctly.
	testutil.Equals(t, []float64{11.0, 21.0}, buf2[0].Samples) // t=2000 from cache
	testutil.Equals(t, []float64{12.0, 22.0}, buf2[1].Samples) // t=3000 from cache
	testutil.Equals(t, []float64{13.0, 23.0}, buf2[2].Samples) // t=4000 from cache
	testutil.Equals(t, []float64{14.0, 24.0}, buf2[3].Samples) // t=5000 from cache
	testutil.Equals(t, []float64{15.0, 25.0}, buf2[4].Samples) // t=6000 from narrow

	// Verify narrow was used, full was not.
	testutil.Equals(t, false, fullInner2.called)
	testutil.Equals(t, true, narrowInner2.called)

	// Verify new step is cached.
	testutil.Equals(t, int64(6000), cache.GetLatestTimestamp(keyPrefix))

	// --- Evaluation 3: Delayed by 2 steps (range [4000, 8000], step=1000) ---
	// Steps 4000-6000 cached. Steps 7000, 8000 are new.
	fullInner3 := &mockInnerOperator{
		steps:  []model.StepVector{},
		series: []labels.Labels{labels.FromStrings("cluster", "a"), labels.FromStrings("cluster", "b")},
	}
	narrowInner3 := &mockInnerOperator{
		steps: []model.StepVector{
			{T: 7000, SampleIDs: []uint64{0, 1}, Samples: []float64{16.0, 26.0}},
			{T: 8000, SampleIDs: []uint64{0, 1}, Samples: []float64{17.0, 27.0}},
		},
		series: []labels.Labels{labels.FromStrings("cluster", "a"), labels.FromStrings("cluster", "b")},
	}

	opts3 := &query.Options{
		Start:         time.UnixMilli(4000),
		End:           time.UnixMilli(8000),
		Step:          time.Millisecond * 1000,
		StepsBatch:    10,
		SubqueryCache: cache,
		TenantID:      "tenant1",
	}

	op3 := NewCachedSubqueryOperator(fullInner3, narrowInner3, opts3, innerExpr)
	buf3 := make([]model.StepVector, 10)
	n, err = op3.Next(context.Background(), buf3)
	testutil.Ok(t, err)
	testutil.Equals(t, 5, n)

	// Verify: 3 from cache, 2 from narrow.
	testutil.Equals(t, []float64{13.0, 23.0}, buf3[0].Samples) // t=4000 from cache
	testutil.Equals(t, []float64{14.0, 24.0}, buf3[1].Samples) // t=5000 from cache
	testutil.Equals(t, []float64{15.0, 25.0}, buf3[2].Samples) // t=6000 from cache
	testutil.Equals(t, []float64{16.0, 26.0}, buf3[3].Samples) // t=7000 from narrow
	testutil.Equals(t, []float64{17.0, 27.0}, buf3[4].Samples) // t=8000 from narrow

	testutil.Equals(t, false, fullInner3.called)
	testutil.Equals(t, true, narrowInner3.called)
	testutil.Equals(t, int64(8000), cache.GetLatestTimestamp(keyPrefix))
}
