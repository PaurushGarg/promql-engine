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

// --- Hash-based step and series key encoding for Option 2 cache design ---

// HashedSample represents a single series value keyed by label hash.
type HashedSample struct {
	Hash  uint64
	Value float64
}

// HashedHistogramSample represents a single histogram value keyed by label hash.
type HashedHistogramSample struct {
	Hash      uint64
	Histogram *histogram.FloatHistogram
}

// HashedStepData holds per-step data keyed by label hash (not positional).
// This is the format stored in per-step cache keys.
type HashedStepData struct {
	Samples    []HashedSample
	Histograms []HashedHistogramSample
}

// EncodeHashedStepData serializes a HashedStepData to compressed bytes.
// Format: snappy([numSamples:u32][numHistograms:u32]
//
//	[hash:u64, value:f64]... (samples)
//	[hash:u64, histLen:u32, histBytes...]... (histograms))
func EncodeHashedStepData(data *HashedStepData) []byte {
	if data == nil || (len(data.Samples) == 0 && len(data.Histograms) == 0) {
		return nil
	}

	size := 8 + len(data.Samples)*16 + len(data.Histograms)*256
	buf := make([]byte, 0, size)

	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(data.Samples)))
	binary.LittleEndian.PutUint32(header[4:], uint32(len(data.Histograms)))
	buf = append(buf, header[:]...)

	// Samples: [hash:u64][value:f64] pairs.
	var pair [16]byte
	for _, s := range data.Samples {
		binary.LittleEndian.PutUint64(pair[:8], s.Hash)
		binary.LittleEndian.PutUint64(pair[8:], math.Float64bits(s.Value))
		buf = append(buf, pair[:]...)
	}

	// Histograms: [hash:u64][length:u32][encoded histogram bytes].
	for _, h := range data.Histograms {
		var hdr [12]byte
		binary.LittleEndian.PutUint64(hdr[:8], h.Hash)
		hBytes := encodeFloatHistogram(h.Histogram)
		binary.LittleEndian.PutUint32(hdr[8:], uint32(len(hBytes)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, hBytes...)
	}

	return snappy.Encode(nil, buf)
}

// DecodeHashedStepData deserializes compressed bytes back to HashedStepData.
func DecodeHashedStepData(data []byte) *HashedStepData {
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

	result := &HashedStepData{}

	// Decode samples.
	if numSamples > 0 {
		needed := int(numSamples) * 16
		if offset+needed > len(raw) {
			return nil
		}
		result.Samples = make([]HashedSample, numSamples)
		for i := uint32(0); i < numSamples; i++ {
			result.Samples[i].Hash = binary.LittleEndian.Uint64(raw[offset:])
			result.Samples[i].Value = math.Float64frombits(binary.LittleEndian.Uint64(raw[offset+8:]))
			offset += 16
		}
	}

	// Decode histograms.
	if numHistograms > 0 {
		result.Histograms = make([]HashedHistogramSample, 0, numHistograms)
		for i := uint32(0); i < numHistograms; i++ {
			if offset+12 > len(raw) {
				break
			}
			hash := binary.LittleEndian.Uint64(raw[offset:])
			hLen := binary.LittleEndian.Uint32(raw[offset+8:])
			offset += 12
			if offset+int(hLen) > len(raw) {
				break
			}
			h := decodeFloatHistogram(raw[offset : offset+int(hLen)])
			offset += int(hLen)
			if h != nil {
				result.Histograms = append(result.Histograms, HashedHistogramSample{Hash: hash, Histogram: h})
			}
		}
	}

	return result
}

// SeriesEntry is one entry in the series key: a label hash mapped to its full labels.
type SeriesEntry struct {
	Hash   uint64
	Labels []SeriesLabel
}

// SeriesLabel is a single label key-value pair.
type SeriesLabel struct {
	Name  string
	Value string
}

// EncodeSeriesKey serializes a list of SeriesEntry to compressed bytes.
// Format: snappy([numSeries:u32]
//
//	[hash:u64][numLabels:u16][nameLen:u16][name...][valueLen:u16][value...]... per series)
func EncodeSeriesKey(entries []SeriesEntry) []byte {
	if len(entries) == 0 {
		return nil
	}

	size := 4 + len(entries)*100 // rough estimate
	buf := make([]byte, 0, size)

	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], uint32(len(entries)))
	buf = append(buf, numBuf[:]...)

	for _, entry := range entries {
		var hashBuf [8]byte
		binary.LittleEndian.PutUint64(hashBuf[:], entry.Hash)
		buf = append(buf, hashBuf[:]...)

		var nlBuf [2]byte
		binary.LittleEndian.PutUint16(nlBuf[:], uint16(len(entry.Labels)))
		buf = append(buf, nlBuf[:]...)

		for _, lbl := range entry.Labels {
			// Name length + name bytes.
			binary.LittleEndian.PutUint16(nlBuf[:], uint16(len(lbl.Name)))
			buf = append(buf, nlBuf[:]...)
			buf = append(buf, lbl.Name...)
			// Value length + value bytes.
			binary.LittleEndian.PutUint16(nlBuf[:], uint16(len(lbl.Value)))
			buf = append(buf, nlBuf[:]...)
			buf = append(buf, lbl.Value...)
		}
	}

	return snappy.Encode(nil, buf)
}

// DecodeSeriesKey deserializes compressed bytes back to []SeriesEntry.
func DecodeSeriesKey(data []byte) []SeriesEntry {
	if len(data) == 0 {
		return nil
	}
	raw, err := snappy.Decode(nil, data)
	if err != nil || len(raw) < 4 {
		return nil
	}

	numSeries := binary.LittleEndian.Uint32(raw[:4])
	offset := 4

	entries := make([]SeriesEntry, 0, numSeries)
	for i := uint32(0); i < numSeries; i++ {
		if offset+10 > len(raw) {
			break
		}
		hash := binary.LittleEndian.Uint64(raw[offset:])
		offset += 8
		numLabels := binary.LittleEndian.Uint16(raw[offset:])
		offset += 2

		labels := make([]SeriesLabel, 0, numLabels)
		for j := uint16(0); j < numLabels; j++ {
			if offset+2 > len(raw) {
				return entries
			}
			nameLen := binary.LittleEndian.Uint16(raw[offset:])
			offset += 2
			if offset+int(nameLen) > len(raw) {
				return entries
			}
			name := string(raw[offset : offset+int(nameLen)])
			offset += int(nameLen)

			if offset+2 > len(raw) {
				return entries
			}
			valueLen := binary.LittleEndian.Uint16(raw[offset:])
			offset += 2
			if offset+int(valueLen) > len(raw) {
				return entries
			}
			value := string(raw[offset : offset+int(valueLen)])
			offset += int(valueLen)

			labels = append(labels, SeriesLabel{Name: name, Value: value})
		}

		entries = append(entries, SeriesEntry{Hash: hash, Labels: labels})
	}

	return entries
}
