// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/query"
)

func TestRangeToSubqueryOptimizer(t *testing.T) {
	cache := query.NewLocalSubqueryCache()
	opts := &query.Options{
		SubqueryCache: cache,
		TenantID:      "test",
	}

	cases := []struct {
		name     string
		input    string
		expected string
		rewrite  bool // whether optimizer should fire
	}{
		{
			name:     "max_over_time 30m",
			input:    `max_over_time(metric[30m])`,
			expected: `max_over_time(max_over_time(metric[1m0s])[30m:1m])`,
			rewrite:  true,
		},
		{
			name:     "min_over_time 1h",
			input:    `min_over_time(metric[1h])`,
			expected: `min_over_time(min_over_time(metric[1m0s])[1h:1m])`,
			rewrite:  true,
		},
		{
			name:     "sum_over_time 30m",
			input:    `sum_over_time(metric[30m])`,
			expected: `sum_over_time(sum_over_time(metric[1m0s])[30m:1m])`,
			rewrite:  true,
		},
		{
			name:     "count_over_time 30m (outer becomes sum_over_time)",
			input:    `count_over_time(metric[30m])`,
			expected: `sum_over_time(count_over_time(metric[1m0s])[30m:1m])`,
			rewrite:  true,
		},
		{
			name:     "range too small - no rewrite",
			input:    `max_over_time(metric[1m])`,
			expected: `max_over_time(metric[1m])`,
			rewrite:  false,
		},
		{
			name:     "rate - not decomposable",
			input:    `rate(metric[30m])`,
			expected: `rate(metric[30m])`,
			rewrite:  false,
		},
		{
			name:     "no cache - no rewrite",
			input:    `max_over_time(metric[30m])`,
			expected: `max_over_time(metric[30m])`,
			rewrite:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.input)
			testutil.Ok(t, err)

			plan := replacePrometheusNodes(expr)
			optimizer := RangeToSubqueryOptimizer{}

			var testOpts *query.Options
			if tc.name == "no cache - no rewrite" {
				testOpts = &query.Options{} // no cache
			} else {
				testOpts = opts
			}

			optimized, _ := optimizer.Optimize(plan, testOpts)
			t.Logf("Input:  %s", tc.input)
			t.Logf("Output: %s", optimized.String())
			testutil.Equals(t, tc.expected, optimized.String())
		})
	}
}
