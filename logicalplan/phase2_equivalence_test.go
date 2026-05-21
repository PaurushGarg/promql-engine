// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"context"
	"testing"
	"time"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
)

// TestPhase2DecompositionEquivalence verifies that range-to-subquery decompositions
// produce identical results to the original query. This validates which *_over_time
// functions can be safely split into chunks for caching.
func TestPhase2DecompositionEquivalence(t *testing.T) {
	cases := []struct {
		name     string
		load     string
		original string
		rewrite  string
		equal    bool
	}{
		{
			name: "max_over_time: max(max(chunks)) == max(all)",
			load: `load 30s
  cpu{host="h1"} 10+5x40
  cpu{host="h2"} 20+3x40`,
			original: `max_over_time(cpu[6m])`,
			rewrite:  `max_over_time(max_over_time(cpu[2m])[6m:2m])`,
			equal:    true,
		},
		{
			name: "min_over_time: min(min(chunks)) == min(all)",
			load: `load 30s
  cpu{host="h1"} 100-1x40
  cpu{host="h2"} 200-2x40`,
			original: `min_over_time(cpu[6m])`,
			rewrite:  `min_over_time(min_over_time(cpu[2m])[6m:2m])`,
			equal:    true,
		},
		{
			name: "sum_over_time: sum(sum(chunks)) == sum(all)",
			load: `load 30s
  requests{svc="api"} 1+1x40
  requests{svc="web"} 2+2x40`,
			original: `sum_over_time(requests[6m])`,
			rewrite:  `sum_over_time(sum_over_time(requests[2m])[6m:2m])`,
			equal:    true,
		},
		{
			name: "avg_over_time: avg(avg(chunks)) == avg(all) with fixed interval",
			load: `load 30s
  cpu{host="h1"} 10+5x40`,
			original: `avg_over_time(cpu[6m])`,
			rewrite:  `avg_over_time(avg_over_time(cpu[2m])[6m:2m])`,
			equal:    true, // Equal when chunks have same sample count (fixed scrape interval)
		},
		// Debug: test with the exact parameters that fail in the e2e test.
		{
			name: "max_over_time 30m range with 10m chunks at t=600",
			load: `load 30s
  metric{pod="a"} 1+1x200`,
			original: `max_over_time(metric[30m])`,
			rewrite:  `max_over_time(max_over_time(metric[10m])[30m:10m])`,
			equal:    true,
		},
		{
			name: "count_over_time: sum(count(chunks)) == count(all)",
			load: `load 30s
  requests{svc="api"} 1+1x40
  requests{svc="web"} 2+2x40`,
			original: `count_over_time(requests[6m])`,
			rewrite:  `sum_over_time(count_over_time(requests[2m])[6m:2m])`,
			equal:    true,
		},
		{
			name: "last_over_time: last of last chunk == last of all",
			load: `load 30s
  cpu{host="h1"} 10+5x40
  cpu{host="h2"} 20+3x40`,
			original: `last_over_time(cpu[6m])`,
			rewrite:  `last_over_time(last_over_time(cpu[2m])[6m:2m])`,
			equal:    true,
		},
		{
			name: "present_over_time: max(present(chunks)) == present(all)",
			load: `load 30s
  cpu{host="h1"} 10+5x40`,
			original: `present_over_time(cpu[6m])`,
			rewrite:  `max_over_time(present_over_time(cpu[2m])[6m:2m])`,
			equal:    true,
		},
		{
			name: "stddev_over_time: NOT decomposable",
			load: `load 30s
  cpu{host="h1"} 10+5x40`,
			original: `stddev_over_time(cpu[6m])`,
			rewrite:  `stddev_over_time(stddev_over_time(cpu[2m])[6m:2m])`,
			equal:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, tc.load)
			defer storage.Close()

			opts := promql.EngineOpts{
				Timeout:    time.Hour,
				MaxSamples: 1e10,
			}
			eng := promql.NewEngine(opts)

			queryTime := time.Unix(600, 0)

			q1, err := eng.NewInstantQuery(context.Background(), storage, nil, tc.original, queryTime)
			testutil.Ok(t, err)
			res1 := q1.Exec(context.Background())
			testutil.Ok(t, res1.Err)
			q1.Close()

			q2, err := eng.NewInstantQuery(context.Background(), storage, nil, tc.rewrite, queryTime)
			testutil.Ok(t, err)
			res2 := q2.Exec(context.Background())
			testutil.Ok(t, res2.Err)
			q2.Close()

			if tc.equal {
				testutil.Equals(t, res1.String(), res2.String())
			} else {
				if res1.String() == res2.String() {
					t.Errorf("expected results to differ but they are equal: %s", res1.String())
				}
			}
		})
	}
}
