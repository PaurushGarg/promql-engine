// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package logicalplan

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

const ChunkedRangeSelectorNode NodeType = "chunked_range_selector"

// ChunkedRangeSelector is a logical plan node that represents a *_over_time(metric[range])
// evaluated in chunks. It replaces the normal MatrixSelector+FunctionCall pair when the
// range is large enough to benefit from chunked evaluation and caching.
//
// At execution time, the ChunkedRangeOperator evaluates the inner function per chunk,
// merges chunk results using the appropriate merge operation, and handles the trailing
// edge (gap between last aligned chunk and evaluation time).
type ChunkedRangeSelector struct {
	// The original vector selector (metric with label matchers).
	VectorSelector *VectorSelector

	// The original range (e.g., 1h for max_over_time(metric[1h])).
	Range time.Duration

	// The chunk size for splitting the range.
	ChunkSize time.Duration

	// The *_over_time function to apply per chunk and for the trailing edge.
	Func parser.Function

	// The merge operation to combine chunk results.
	// For max_over_time: merge = max across chunks
	// For min_over_time: merge = min across chunks
	// For sum_over_time: merge = sum across chunks
	// For count_over_time: merge = sum across chunks (sum of counts = total count)
	// For avg_over_time: merge = weighted average (needs sum + count)
	MergeOp string

	// OriginalString for rendering.
	OriginalString string
}

func (c *ChunkedRangeSelector) Clone() Node {
	clone := *c
	clone.VectorSelector = c.VectorSelector.Clone().(*VectorSelector)
	return &clone
}

func (c *ChunkedRangeSelector) Children() []*Node {
	var vs Node = c.VectorSelector
	return []*Node{&vs}
}

func (c *ChunkedRangeSelector) String() string {
	if c.OriginalString != "" {
		return c.OriginalString
	}
	return fmt.Sprintf("%s(%s[%s])", c.Func.Name, c.VectorSelector.String(), c.Range)
}

func (c *ChunkedRangeSelector) ReturnType() parser.ValueType { return parser.ValueTypeVector }
func (c *ChunkedRangeSelector) Type() NodeType               { return ChunkedRangeSelectorNode }
