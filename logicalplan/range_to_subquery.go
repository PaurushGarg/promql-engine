// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/util/annotations"
)

// RangeToSubqueryOptimizer rewrites plain *_over_time(metric[range]) into an
// equivalent subquery form that enables per-chunk caching.
//
// Example rewrite:
//
//	max_over_time(metric[1h]) → max_over_time(max_over_time(metric[10m])[1h:10m])
//
// This is only applied when:
//   - A SubqueryCache is configured (ruler workload)
//   - The function is decomposable (max, min, sum, avg, count, last, present)
//   - The range is large enough to benefit from chunking (> 2 * chunk size)
//
// The chunk size is fixed at the subquery step (default evaluation interval).
type RangeToSubqueryOptimizer struct{}

// Minimum range to bother chunking — must be at least 2x the chunk size.
const minChunks = 2

func (RangeToSubqueryOptimizer) Optimize(plan Node, opts *query.Options) (Node, annotations.Annotations) {
	// This optimizer rewrites range selectors into subqueries for caching.
	// Currently only fires when SubqueryCache is configured, as the primary
	// benefit is enabling caching of per-chunk results.
	// Note: cache integration for optimizer-generated subqueries needs the
	// cache wrapper to skip caching during the first evaluation (cold start
	// within the same query execution). This is a known TODO.
	if opts == nil || opts.SubqueryCache == nil {
		return plan, nil
	}

	chunkSize := defaultChunkSize(opts)

	TraverseBottomUp(nil, &plan, func(_, current *Node) bool {
		funcCall, ok := (*current).(*FunctionCall)
		if !ok {
			return false
		}
		rewritten := tryRangeToSubquery(funcCall, chunkSize)
		if rewritten != nil {
			*current = rewritten
		}
		return false
	})
	return plan, nil
}

func tryRangeToSubquery(funcCall *FunctionCall, chunkSize time.Duration) Node {
	decomp := decomposition(funcCall.Func.Name)
	if decomp == nil {
		return nil
	}

	// The function's first arg must be a MatrixSelector (plain range selector).
	if len(funcCall.Args) < 1 {
		return nil
	}
	matrix, ok := funcCall.Args[0].(*MatrixSelector)
	if !ok {
		return nil
	}

	// Only chunk if range is large enough.
	if matrix.Range < chunkSize*time.Duration(minChunks) {
		return nil
	}

	// Build: outerFunc(innerFunc(metric[chunk])[range:chunk])
	innerMatrix := &MatrixSelector{
		VectorSelector: matrix.VectorSelector.Clone().(*VectorSelector),
		Range:          chunkSize,
		OriginalString: matrix.VectorSelector.String() + "[" + chunkSize.String() + "]",
	}
	innerFunc := &FunctionCall{
		Func: parser.Function{Name: decomp.innerFunc},
		Args: []Node{innerMatrix},
	}
	if f, exists := parser.Functions[decomp.innerFunc]; exists {
		innerFunc.Func = *f
	}

	subq := &Subquery{
		Expr:  innerFunc,
		Range: matrix.Range,
		Step:  chunkSize,
	}

	outerFunc := &FunctionCall{
		Func: funcCall.Func,
		Args: []Node{subq},
	}
	if decomp.outerFunc != funcCall.Func.Name {
		if f, exists := parser.Functions[decomp.outerFunc]; exists {
			outerFunc.Func = *f
		}
	}

	return outerFunc
}

type decompositionRule struct {
	innerFunc string // function applied per chunk
	outerFunc string // function applied over chunk results
}

// decomposition returns the decomposition rule for a function, or nil if not decomposable.
func decomposition(funcName string) *decompositionRule {
	switch funcName {
	case "max_over_time":
		return &decompositionRule{innerFunc: "max_over_time", outerFunc: "max_over_time"}
	case "min_over_time":
		return &decompositionRule{innerFunc: "min_over_time", outerFunc: "min_over_time"}
	case "sum_over_time":
		return &decompositionRule{innerFunc: "sum_over_time", outerFunc: "sum_over_time"}
	case "avg_over_time":
		return &decompositionRule{innerFunc: "avg_over_time", outerFunc: "avg_over_time"}
	case "count_over_time":
		return &decompositionRule{innerFunc: "count_over_time", outerFunc: "sum_over_time"}
	case "last_over_time":
		return &decompositionRule{innerFunc: "last_over_time", outerFunc: "last_over_time"}
	case "present_over_time":
		return &decompositionRule{innerFunc: "present_over_time", outerFunc: "max_over_time"}
	}
	return nil
}

func defaultChunkSize(opts *query.Options) time.Duration {
	// Use 1 minute as default chunk size — matches typical ruler evaluation interval.
	// This ensures subquery steps align with evaluation timestamps, preventing
	// boundary mismatches where the last chunk misses trailing data.
	return 1 * time.Minute
}
