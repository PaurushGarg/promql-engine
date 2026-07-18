// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"encoding/binary"
	"math"

	"github.com/golang/snappy"
)

// EncodeFloats serializes a []float64 to compressed bytes.
// Format: snappy([]uint64 bit patterns as little-endian bytes).
func EncodeFloats(vals []float64) []byte {
	if len(vals) == 0 {
		return nil
	}
	raw := make([]byte, len(vals)*8)
	for i, v := range vals {
		binary.LittleEndian.PutUint64(raw[i*8:], math.Float64bits(v))
	}
	return snappy.Encode(nil, raw)
}

// DecodeFloats deserializes compressed bytes back to []float64.
func DecodeFloats(data []byte) []float64 {
	if len(data) == 0 {
		return nil
	}
	raw, err := snappy.Decode(nil, data)
	if err != nil {
		return nil
	}
	if len(raw)%8 != 0 {
		return nil
	}
	vals := make([]float64, len(raw)/8)
	for i := range vals {
		vals[i] = math.Float64frombits(binary.LittleEndian.Uint64(raw[i*8:]))
	}
	return vals
}
