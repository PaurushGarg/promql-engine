// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/query"
)

func TestSubqueryCacheOptimizer(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "sum + avg_over_time subquery",
			input:    `sum by (cluster) (avg_over_time(rate(metric[1m])[2d:1m]))`,
			expected: `avg_over_time(sum by (cluster) (rate(metric[1m]))[2d:1m])`,
		},
		{
			name:     "sum + sum_over_time subquery",
			input:    `sum by (cluster) (sum_over_time(metric[2d:1m]))`,
			expected: `sum_over_time(sum by (cluster) (metric)[2d:1m])`,
		},
		{
			name:     "max + max_over_time subquery",
			input:    `max by (host) (max_over_time(cpu_usage{env="prod"}[1h:5m]))`,
			expected: `max_over_time(max by (host) (cpu_usage{env="prod"})[1h:5m])`,
		},
		{
			name:     "min + min_over_time subquery",
			input:    `min by (region) (min_over_time(latency[6h:1m]))`,
			expected: `min_over_time(min by (region) (latency)[6h:1m])`,
		},
		{
			name:     "count + count_over_time does not commute",
			input:    `count by (app) (count_over_time(requests[1d:5m]))`,
			expected: `count by (app) (count_over_time(requests[1d:5m]))`,
		},
		// Non-commutative cases — should NOT be rewritten.
		{
			name:     "avg aggregation does not commute",
			input:    `avg by (cluster) (avg_over_time(metric[2d:1m]))`,
			expected: `avg by (cluster) (avg_over_time(metric[2d:1m]))`,
		},
		{
			name:     "sum + max_over_time does not commute",
			input:    `sum by (cluster) (max_over_time(metric[2d:1m]))`,
			expected: `sum by (cluster) (max_over_time(metric[2d:1m]))`,
		},
		{
			name:     "quantile aggregation does not commute",
			input:    `quantile by (cluster) (0.9, avg_over_time(metric[2d:1m]))`,
			expected: `quantile by (cluster) (0.9, avg_over_time(metric[2d:1m]))`,
		},
		{
			name:     "topk does not commute",
			input:    `topk(5, avg_over_time(metric[2d:1m]))`,
			expected: `topk(5, avg_over_time(metric[2d:1m]))`,
		},
		// Not a subquery — plain range selector, should NOT be rewritten.
		{
			name:     "plain range selector not rewritten",
			input:    `sum by (cluster) (rate(metric[5m]))`,
			expected: `sum by (cluster) (rate(metric[5m]))`,
		},
		// Aggregation without grouping.
		{
			name:     "sum without grouping + avg_over_time",
			input:    `sum(avg_over_time(metric[2d:1m]))`,
			expected: `avg_over_time(sum(metric)[2d:1m])`,
		},
		// Without clause.
		{
			name:     "sum without (host) + avg_over_time",
			input:    `sum without (host) (avg_over_time(metric[2d:1m]))`,
			expected: `avg_over_time(sum without (host) (metric)[2d:1m])`,
		},
	}

	opts := &query.Options{}
	optimizer := SubqueryCacheOptimizer{}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.input)
			testutil.Ok(t, err)

			plan := replacePrometheusNodes(expr)
			optimized, _ := optimizer.Optimize(plan, opts)
			testutil.Equals(t, tc.expected, optimized.String())
		})
	}
}
