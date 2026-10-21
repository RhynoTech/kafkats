import { describe, expect, it } from 'vitest'

import { CompressionRatioEstimator, RECORD_BATCH_HEADER_BYTES } from '@/producer/compression-ratio-estimator.js'
import { CompressionType } from '@/protocol/records/compression.js'

const HEADER = RECORD_BATCH_HEADER_BYTES

/** Encoded batch size for `recordBytes` records that compress to `ratio`. */
function encodedAt(ratio: number, recordBytes = 100_000): number {
	return HEADER + ratio * recordBytes
}

describe('CompressionRatioEstimator', () => {
	it('starts at 1.0 for every topic and codec', () => {
		const estimator = new CompressionRatioEstimator()
		expect(estimator.ratio('a', CompressionType.Snappy)).toBe(1)
		expect(estimator.ratio('b', CompressionType.Zstd)).toBe(1)
	})

	it('takes the ratio of each observed batch, better or worse', () => {
		const estimator = new CompressionRatioEstimator()
		expect(estimator.observe('t', CompressionType.Snappy, 100_000, encodedAt(0.3))).toBeCloseTo(0.3)
		expect(estimator.ratio('t', CompressionType.Snappy)).toBeCloseTo(0.3)
		expect(estimator.observe('t', CompressionType.Snappy, 100_000, encodedAt(0.31))).toBeCloseTo(0.31)
		expect(estimator.observe('t', CompressionType.Snappy, 100_000, encodedAt(0.9))).toBeCloseTo(0.9)
		expect(estimator.observe('t', CompressionType.Snappy, 100_000, encodedAt(0.25))).toBeCloseTo(0.25)
	})

	it('keeps topics and codecs apart', () => {
		const estimator = new CompressionRatioEstimator()
		estimator.observe('a', CompressionType.Snappy, 100_000, encodedAt(0.3))
		expect(estimator.ratio('a', CompressionType.Zstd)).toBe(1)
		expect(estimator.ratio('b', CompressionType.Snappy)).toBe(1)
	})

	it('resets to 1.0, or to a worse observed ratio, like the Java client after a broker rejection', () => {
		const estimator = new CompressionRatioEstimator()
		estimator.observe('t', CompressionType.Lz4, 100_000, encodedAt(0.3))
		estimator.reset('t', CompressionType.Lz4, 0.3)
		expect(estimator.ratio('t', CompressionType.Lz4)).toBe(1)
		estimator.reset('t', CompressionType.Lz4, 1.02)
		expect(estimator.ratio('t', CompressionType.Lz4)).toBe(1.02)
	})

	it('adds a 5% margin for compressed batches and sizes uncompressed ones exactly', () => {
		const estimator = new CompressionRatioEstimator()
		estimator.observe('t', CompressionType.Gzip, 100_000, encodedAt(0.5))
		expect(estimator.estimateBatchBytes('t', CompressionType.Gzip, 1_000_000)).toBeCloseTo(HEADER + 525_000)
		expect(estimator.recordBytesWithin('t', CompressionType.Gzip, HEADER + 525_000)).toBeCloseTo(1_000_000)
		expect(estimator.estimateBatchBytes('t', CompressionType.None, 1_000_000)).toBe(HEADER + 1_000_000)
		expect(estimator.recordBytesWithin('t', CompressionType.None, HEADER + 1_000_000)).toBe(1_000_000)
	})
})
