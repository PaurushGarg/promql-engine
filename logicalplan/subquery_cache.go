// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/util/annotations"
)

// SubqueryCacheOptimizer pushes a commutative outer aggregation inside a subquery.
// This reduces the per-step cardinality of the subquery's inner expression,
// enabling efficient caching of intermediate results between ruler evaluations.
//
// Example rewrite:
//
//	sum by (cluster) (avg_over_time(rate(metric[1m])[2d:1m]))
//	→ avg_over_time((sum by (cluster)(rate(metric[1m])))[2d:1m])
//
// The rewrite is only applied when the aggregation and over_time function commute:
//   - sum  + avg_over_time, sum_over_time
//   - min  + min_over_time
//   - max  + max_over_time
//   - count + count_over_time
//   - group + group_over_time (identity)
type SubqueryCacheOptimizer struct{}

func (SubqueryCacheOptimizer) Optimize(plan Node, _ *query.Options) (Node, annotations.Annotations) {
	TraverseBottomUp(nil, &plan, func(_, current *Node) bool {
		agg, ok := (*current).(*Aggregation)
		if !ok {
			return false
		}
		rewritten := tryPushAggInsideSubquery(agg)
		if rewritten != nil {
			*current = rewritten
		}
		return false
	})
	return plan, nil
}

// tryPushAggInsideSubquery checks if the aggregation wraps a commutative
// over_time(subquery) and returns the rewritten node, or nil if not applicable.
func tryPushAggInsideSubquery(agg *Aggregation) Node {
	// Pattern: Aggregation.Expr must be FunctionCall(*_over_time)
	funcCall, ok := agg.Expr.(*FunctionCall)
	if !ok {
		return nil
	}
	if !isOverTimeFunc(funcCall.Func.Name) {
		return nil
	}
	// The over_time function's first arg must be a Subquery.
	if len(funcCall.Args) < 1 {
		return nil
	}
	subq, ok := funcCall.Args[0].(*Subquery)
	if !ok {
		return nil
	}
	// Check commutativity.
	if !commutes(agg.Op, funcCall.Func.Name) {
		return nil
	}

	// Rewrite: push aggregation inside the subquery.
	// Before: Aggregation(FunctionCall(Subquery(inner)))
	// After:  FunctionCall(Subquery(Aggregation(inner)))
	innerAgg := &Aggregation{
		Op:       agg.Op,
		Expr:     subq.Expr,
		Param:    agg.Param,
		Grouping: agg.Grouping,
		Without:  agg.Without,
	}
	newSubq := &Subquery{
		Expr:           innerAgg,
		Range:          subq.Range,
		OriginalOffset: subq.OriginalOffset,
		Offset:         subq.Offset,
		Timestamp:      subq.Timestamp,
		Step:           subq.Step,
		StartOrEnd:     subq.StartOrEnd,
	}
	newFunc := &FunctionCall{
		Func: funcCall.Func,
		Args: make([]Node, len(funcCall.Args)),
	}
	newFunc.Args[0] = newSubq
	// Copy any additional args (e.g., quantile parameter) — though for
	// commutative pairs these won't exist.
	for i := 1; i < len(funcCall.Args); i++ {
		newFunc.Args[i] = funcCall.Args[i]
	}
	return newFunc
}

// commutes returns true if the aggregation op and over_time function
// produce the same result regardless of evaluation order.
func commutes(op parser.ItemType, funcName string) bool {
	switch op {
	case parser.SUM:
		return funcName == "avg_over_time" || funcName == "sum_over_time" || funcName == "last_over_time"
	case parser.MIN:
		return funcName == "min_over_time" || funcName == "last_over_time"
	case parser.MAX:
		return funcName == "max_over_time" || funcName == "last_over_time"
	case parser.GROUP:
		return funcName == "group_over_time"
	}
	return false
}

func isOverTimeFunc(name string) bool {
	switch name {
	case "avg_over_time", "sum_over_time", "min_over_time", "max_over_time",
		"count_over_time", "group_over_time", "last_over_time",
		"present_over_time", "quantile_over_time", "stddev_over_time",
		"stdvar_over_time":
		return true
	}
	return false
}
