// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"encoding/binary"
	"math"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/histogram"
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

// StepData holds the cacheable data from a StepVector — both float samples and histograms.
type StepData struct {
	SampleIDs    []uint64
	Samples      []float64
	HistogramIDs []uint64
	Histograms   []*histogram.FloatHistogram
}

// EncodeStepData serializes a StepData to compressed bytes.
// Format: snappy(header + float_samples + histograms)
//
// Header: [numSamples:uint32][numHistograms:uint32]
// Float section: [sampleID:uint64, value:float64] * numSamples
// Histogram section: for each histogram:
//
//	[histogramID:uint64][encodedHistogramLength:uint32][encodedHistogram...]
func EncodeStepData(data *StepData) []byte {
	if data == nil || (len(data.Samples) == 0 && len(data.Histograms) == 0) {
		return nil
	}

	// Estimate buffer size.
	size := 8 // header
	size += len(data.Samples) * 16
	size += len(data.Histograms) * 256 // rough estimate per histogram

	buf := make([]byte, 0, size)

	// Header.
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data.Samples)))
	binary.LittleEndian.PutUint32(header[4:], uint32(len(data.Histograms)))
	buf = append(buf, header[:]...)

	// Float samples: [id:uint64][value:float64] pairs.
	var pair [16]byte
	for i, val := range data.Samples {
		id := uint64(0)
		if i < len(data.SampleIDs) {
			id = data.SampleIDs[i]
		}
		binary.LittleEndian.PutUint64(pair[:8], id)
		binary.LittleEndian.PutUint64(pair[8:], math.Float64bits(val))
		buf = append(buf, pair[:]...)
	}

	// Histograms: [id:uint64][length:uint32][encoded histogram bytes].
	for i, h := range data.Histograms {
		id := uint64(0)
		if i < len(data.HistogramIDs) {
			id = data.HistogramIDs[i]
		}
		var idBuf [8]byte
		binary.LittleEndian.PutUint64(idBuf[:], id)
		buf = append(buf, idBuf[:]...)

		hBytes := encodeFloatHistogram(h)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(hBytes)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, hBytes...)
	}

	return snappy.Encode(nil, buf)
}

// DecodeStepData deserializes compressed bytes back to StepData.
func DecodeStepData(data []byte) *StepData {
	if len(data) == 0 {
		return nil
	}
	raw, err := snappy.Decode(nil, data)
	if err != nil || len(raw) < 8 {
		return nil
	}

	numSamples := binary.LittleEndian.Uint32(raw[:4])
	numHistograms := binary.LittleEndian.Uint32(raw[4:8])
	offset := 8

	result := &StepData{}

	// Decode float samples.
	if numSamples > 0 {
		needed := int(numSamples) * 16
		if offset+needed > len(raw) {
			return nil
		}
		result.SampleIDs = make([]uint64, numSamples)
		result.Samples = make([]float64, numSamples)
		for i := uint32(0); i < numSamples; i++ {
			result.SampleIDs[i] = binary.LittleEndian.Uint64(raw[offset:])
			result.Samples[i] = math.Float64frombits(binary.LittleEndian.Uint64(raw[offset+8:]))
			offset += 16
		}
	}

	// Decode histograms.
	if numHistograms > 0 {
		result.HistogramIDs = make([]uint64, 0, numHistograms)
		result.Histograms = make([]*histogram.FloatHistogram, 0, numHistograms)
		for i := uint32(0); i < numHistograms; i++ {
			if offset+12 > len(raw) {
				break
			}
			id := binary.LittleEndian.Uint64(raw[offset:])
			hLen := binary.LittleEndian.Uint32(raw[offset+8:])
			offset += 12
			if offset+int(hLen) > len(raw) {
				break
			}
			h := decodeFloatHistogram(raw[offset : offset+int(hLen)])
			offset += int(hLen)
			if h != nil {
				result.HistogramIDs = append(result.HistogramIDs, id)
				result.Histograms = append(result.Histograms, h)
			}
		}
	}

	return result
}

// encodeFloatHistogram serializes a single FloatHistogram to bytes.
// Format: [counterResetHint:int32][schema:int32][zeroThreshold:float64][zeroCount:float64]
//
//	[count:float64][sum:float64]
//	[numPosSpans:uint32][[offset:int32][length:uint32]]...
//	[numNegSpans:uint32][[offset:int32][length:uint32]]...
//	[numPosBuckets:uint32][float64...]
//	[numNegBuckets:uint32][float64...]
//	[numCustomValues:uint32][float64...]
func encodeFloatHistogram(h *histogram.FloatHistogram) []byte {
	if h == nil {
		return nil
	}
	// Estimate size.
	size := 4 + 4 + 8 + 8 + 8 + 8 // fixed fields
	size += 4 + len(h.PositiveSpans)*8
	size += 4 + len(h.NegativeSpans)*8
	size += 4 + len(h.PositiveBuckets)*8
	size += 4 + len(h.NegativeBuckets)*8
	size += 4 + len(h.CustomValues)*8

	buf := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint32(buf[offset:], uint32(int32(h.CounterResetHint)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:], uint32(h.Schema))
	offset += 4
	binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(h.ZeroThreshold))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(h.ZeroCount))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(h.Count))
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(h.Sum))
	offset += 8

	// Positive spans.
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(h.PositiveSpans)))
	offset += 4
	for _, s := range h.PositiveSpans {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(s.Offset))
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:], s.Length)
		offset += 4
	}

	// Negative spans.
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(h.NegativeSpans)))
	offset += 4
	for _, s := range h.NegativeSpans {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(s.Offset))
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:], s.Length)
		offset += 4
	}

	// Positive buckets.
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(h.PositiveBuckets)))
	offset += 4
	for _, b := range h.PositiveBuckets {
		binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(b))
		offset += 8
	}

	// Negative buckets.
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(h.NegativeBuckets)))
	offset += 4
	for _, b := range h.NegativeBuckets {
		binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(b))
		offset += 8
	}

	// Custom values.
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(h.CustomValues)))
	offset += 4
	for _, v := range h.CustomValues {
		binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(v))
		offset += 8
	}

	return buf[:offset]
}

// decodeFloatHistogram deserializes bytes back to a FloatHistogram.
func decodeFloatHistogram(data []byte) *histogram.FloatHistogram {
	if len(data) < 40 { // minimum: fixed fields
		return nil
	}
	offset := 0
	h := &histogram.FloatHistogram{}

	h.CounterResetHint = histogram.CounterResetHint(int32(binary.LittleEndian.Uint32(data[offset:])))
	offset += 4
	h.Schema = int32(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	h.ZeroThreshold = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	h.ZeroCount = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	h.Count = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	h.Sum = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Positive spans.
	if offset+4 > len(data) {
		return h
	}
	numPosSpans := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	h.PositiveSpans = make([]histogram.Span, numPosSpans)
	for i := uint32(0); i < numPosSpans; i++ {
		if offset+8 > len(data) {
			return h
		}
		h.PositiveSpans[i].Offset = int32(binary.LittleEndian.Uint32(data[offset:]))
		h.PositiveSpans[i].Length = binary.LittleEndian.Uint32(data[offset+4:])
		offset += 8
	}

	// Negative spans.
	if offset+4 > len(data) {
		return h
	}
	numNegSpans := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	h.NegativeSpans = make([]histogram.Span, numNegSpans)
	for i := uint32(0); i < numNegSpans; i++ {
		if offset+8 > len(data) {
			return h
		}
		h.NegativeSpans[i].Offset = int32(binary.LittleEndian.Uint32(data[offset:]))
		h.NegativeSpans[i].Length = binary.LittleEndian.Uint32(data[offset+4:])
		offset += 8
	}

	// Positive buckets.
	if offset+4 > len(data) {
		return h
	}
	numPosBuckets := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	h.PositiveBuckets = make([]float64, numPosBuckets)
	for i := uint32(0); i < numPosBuckets; i++ {
		if offset+8 > len(data) {
			return h
		}
		h.PositiveBuckets[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
	}

	// Negative buckets.
	if offset+4 > len(data) {
		return h
	}
	numNegBuckets := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	h.NegativeBuckets = make([]float64, numNegBuckets)
	for i := uint32(0); i < numNegBuckets; i++ {
		if offset+8 > len(data) {
			return h
		}
		h.NegativeBuckets[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
	}

	// Custom values.
	if offset+4 > len(data) {
		return h
	}
	numCustom := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	h.CustomValues = make([]float64, numCustom)
	for i := uint32(0); i < numCustom; i++ {
		if offset+8 > len(data) {
			return h
		}
		h.CustomValues[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8
	}

	return h
}
