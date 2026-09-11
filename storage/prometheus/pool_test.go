// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectorPoolSharesSelectorsAcrossRangeWindows(t *testing.T) {
	pool := NewSelectorPool(nil)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
		labels.MustNewMatcher(labels.MatchEqual, "pod", "nginx-1"),
	}

	maxt := int64(600000)
	step := int64(30000)

	// First selector: rate(metric[5m]) → mint = 600000 - 300000 = 300000
	hints1 := storage.SelectHints{Start: 300000, End: maxt, Step: step, Func: "rate", Range: 300000}
	sel1 := pool.GetFilteredSelector(300000, maxt, step, matchers, nil, hints1)

	// Second selector: rate(metric[2m]) → mint = 600000 - 120000 = 480000
	hints2 := storage.SelectHints{Start: 480000, End: maxt, Step: step, Func: "rate", Range: 120000}
	sel2 := pool.GetFilteredSelector(480000, maxt, step, matchers, nil, hints2)

	// Both should share the same underlying selector.
	require.Equal(t, sel1, sel2, "selectors with same matchers but different range windows should share one cache entry")

	// The cached selector should use the wider mint (300000, not 480000).
	require.Equal(t, int64(300000), pool.selectors[hashMatchers(matchers, maxt, hints1)].hints.Start,
		"cached selector should use the widest (earliest) mint")
}

func TestSelectorPoolSharesAcrossDifferentFunctions(t *testing.T) {
	pool := NewSelectorPool(nil)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
	}

	maxt := int64(600000)
	step := int64(30000)

	// rate(metric[5m])
	hints1 := storage.SelectHints{Start: 300000, End: maxt, Step: step, Func: "rate", Range: 300000}
	sel1 := pool.GetFilteredSelector(300000, maxt, step, matchers, nil, hints1)

	// avg_over_time(metric[2m])
	hints2 := storage.SelectHints{Start: 480000, End: maxt, Step: step, Func: "avg_over_time", Range: 120000}
	sel2 := pool.GetFilteredSelector(480000, maxt, step, matchers, nil, hints2)

	// Should share — TSDB returns identical data regardless of Func.
	require.Equal(t, sel1, sel2, "selectors with different functions should share one cache entry")

	// Mint should be widened to the earlier value.
	require.Equal(t, int64(300000), pool.selectors[hashMatchers(matchers, maxt, hints1)].hints.Start)
}

func TestSelectorPoolSeparatesSeriesFunc(t *testing.T) {
	pool := NewSelectorPool(nil)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
	}

	maxt := int64(600000)
	step := int64(30000)

	// Normal rate query
	hints1 := storage.SelectHints{Start: 300000, End: maxt, Step: step, Func: "rate", Range: 300000}
	pool.GetFilteredSelector(300000, maxt, step, matchers, nil, hints1)

	// "series" metadata query — should NOT share because TSDB skips chunk loading
	hints2 := storage.SelectHints{Start: 300000, End: maxt, Step: step, Func: "series", Range: 300000}
	pool.GetFilteredSelector(300000, maxt, step, matchers, nil, hints2)

	// Should have two separate entries in the pool.
	require.Equal(t, 2, len(pool.selectors), "series func should get a separate cache entry")
}

func TestSelectorPoolSeparatesDifferentMatchers(t *testing.T) {
	pool := NewSelectorPool(nil)

	maxt := int64(600000)
	step := int64(30000)

	matchersA := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
	}
	matchersB := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_responses_total"),
	}

	hints := storage.SelectHints{Start: 300000, End: maxt, Step: step, Func: "rate", Range: 300000}
	pool.GetFilteredSelector(300000, maxt, step, matchersA, nil, hints)
	pool.GetFilteredSelector(300000, maxt, step, matchersB, nil, hints)

	// Different metrics should always be separate.
	require.Equal(t, 2, len(pool.selectors), "different matchers should get separate cache entries")
}
