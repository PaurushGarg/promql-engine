// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

// TestSubqueryCacheEndToEnd verifies that the subquery cache produces correct results
// across multiple evaluations, and that the cache is actually being used.
func TestSubqueryCacheEndToEnd(t *testing.T) {
	load := `load 30s
  http_requests_total{pod="nginx-1", cluster="a"} 1+1x200
  http_requests_total{pod="nginx-2", cluster="a"} 2+2x200
  http_requests_total{pod="nginx-3", cluster="b"} 5+5x200`

	storage := promqltest.LoadedStorage(t, load)
	defer storage.Close()

	cases := []struct {
		name  string
		query string
	}{
		{
			name:  "sum by + avg_over_time subquery (optimizer fires)",
			query: `sum by (cluster) (avg_over_time(http_requests_total[3m:30s]))`,
		},
		{
			name:  "max by + max_over_time subquery (optimizer fires)",
			query: `max by (cluster) (max_over_time(http_requests_total[3m:30s]))`,
		},
		{
			name:  "avg_over_time with inner aggregation (cache only, no optimizer)",
			query: `avg_over_time(sum by (cluster) (http_requests_total)[3m:30s])`,
		},
		// Phase 2: ChunkedRangeOperator with caching.
		{
			name:  "phase2: max_over_time plain range with cache",
			query: `max_over_time(http_requests_total[30m])`,
		},
		{
			name:  "phase2: sum_over_time plain range with cache",
			query: `sum_over_time(http_requests_total[30m])`,
		},
		{
			name:  "phase2: max by + max_over_time with cache",
			query: `max by (cluster) (max_over_time(http_requests_total[30m]))`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset cache for each test case.
			testCache := query.NewLocalSubqueryCache()

			opts := promql.EngineOpts{
				Timeout:    time.Hour,
				MaxSamples: 1e10,
			}

			// --- Evaluation 1: t=600s (cold cache) ---
			ts1 := time.Unix(600, 0)

			// Run with Prometheus engine (ground truth).
			promEngine := promql.NewEngine(opts)
			promQ, err := promEngine.NewInstantQuery(context.Background(), storage, nil, tc.query, ts1)
			testutil.Ok(t, err)
			promResult := promQ.Exec(context.Background())
			testutil.Ok(t, promResult.Err)
			promQ.Close()

			// Run with Thanos engine + cache.
			thanosEngine := New(Opts{
				EngineOpts:        opts,
				LogicalOptimizers: logicalplan.AllOptimizers,
			})
			thanosQ, err := thanosEngine.NewInstantQuery(context.Background(), storage, &QueryOpts{
				SubqueryCache: testCache,
				TenantID:      "test-tenant",
			}, tc.query, ts1)
			testutil.Ok(t, err)
			thanosResult := thanosQ.Exec(context.Background())
			testutil.Ok(t, thanosResult.Err)
			thanosQ.Close()

			// Verify correctness: Thanos with cache == Prometheus.
			testutil.Equals(t, promResult.String(), thanosResult.String())

			// Verify cache was populated.
			stats1 := testCache.Stats()
			t.Logf("After eval 1 (cold): entries=%d, size=%d bytes, hits=%d, misses=%d",
				stats1.Entries, stats1.SizeBytes, stats1.Hits, stats1.Misses)
			testutil.Assert(t, stats1.Entries > 0, "cache should have entries after first eval")

			// --- Evaluation 2: t=660s (warm cache, 1 new step) ---
			ts2 := time.Unix(660, 0)

			promQ2, err := promEngine.NewInstantQuery(context.Background(), storage, nil, tc.query, ts2)
			testutil.Ok(t, err)
			promResult2 := promQ2.Exec(context.Background())
			testutil.Ok(t, promResult2.Err)
			promQ2.Close()

			thanosQ2, err := thanosEngine.NewInstantQuery(context.Background(), storage, &QueryOpts{
				SubqueryCache: testCache,
				TenantID:      "test-tenant",
			}, tc.query, ts2)
			testutil.Ok(t, err)
			thanosResult2 := thanosQ2.Exec(context.Background())
			testutil.Ok(t, thanosResult2.Err)
			thanosQ2.Close()

			// Verify correctness: second eval also matches Prometheus.
			testutil.Equals(t, promResult2.String(), thanosResult2.String())

			// Verify cache was used (hits should increase).
			stats2 := testCache.Stats()
			t.Logf("After eval 2 (warm): entries=%d, size=%d bytes, hits=%d, misses=%d",
				stats2.Entries, stats2.SizeBytes, stats2.Hits, stats2.Misses)
			testutil.Assert(t, stats2.Hits > stats1.Hits, "should have cache hits on second eval")

			// --- Evaluation 3: t=720s (warm cache, verify continued correctness) ---
			ts3 := time.Unix(720, 0)

			promQ3, err := promEngine.NewInstantQuery(context.Background(), storage, nil, tc.query, ts3)
			testutil.Ok(t, err)
			promResult3 := promQ3.Exec(context.Background())
			testutil.Ok(t, promResult3.Err)
			promQ3.Close()

			thanosQ3, err := thanosEngine.NewInstantQuery(context.Background(), storage, &QueryOpts{
				SubqueryCache: testCache,
				TenantID:      "test-tenant",
			}, tc.query, ts3)
			testutil.Ok(t, err)
			thanosResult3 := thanosQ3.Exec(context.Background())
			testutil.Ok(t, thanosResult3.Err)
			thanosQ3.Close()

			testutil.Equals(t, promResult3.String(), thanosResult3.String())

			stats3 := testCache.Stats()
			t.Logf("After eval 3 (warm): entries=%d, size=%d bytes, hits=%d, misses=%d",
				stats3.Entries, stats3.SizeBytes, stats3.Hits, stats3.Misses)
			testutil.Assert(t, stats3.Hits > stats2.Hits, "should have more cache hits on third eval")
		})
	}
}
