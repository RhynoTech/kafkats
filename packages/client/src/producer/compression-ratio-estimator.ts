/**
 * Compression ratio estimation for sizing record batches before they are encoded.
 *
 * A port of the Java client's CompressionRatioEstimator and of how MemoryRecordsBuilder uses it
 * (Kafka 3.9): one ratio per topic and codec, starting at 1.0, that moves towards observed ratios
 * slowly when batches compress better and quickly when they compress worse, so estimates stay
 * on the safe side.
 */

import { CompressionType } from '@/protocol/records/compression.js'

export const RECORD_BATCH_HEADER_BYTES = 61

// Java's COMPRESSION_RATIO_IMPROVING_STEP and COMPRESSION_RATIO_DETERIORATE_STEP.
const IMPROVING_STEP = 0.005
const DETERIORATE_STEP = 0.05
// Java's COMPRESSION_RATE_ESTIMATION_FACTOR, applied on top of the estimated ratio.
const ESTIMATION_FACTOR = 1.05
const INITIAL_RATIO = 1

/**
 * Encoded record bytes per uncompressed record byte, per topic and codec.
 *
 * Ratios compare record bytes only (the batch header is added separately), and the uncompressed
 * side is whatever size measure the caller passes consistently to both `observe` and
 * `estimateBatchBytes`, so a measure that overestimates record size is absorbed into the ratio.
 */
export class CompressionRatioEstimator {
	private readonly ratios = new Map<string, number>()

	ratio(topic: string, compression: CompressionType): number {
		return this.ratios.get(key(topic, compression)) ?? INITIAL_RATIO
	}

	/** Move the estimate towards the ratio of a batch that was just encoded; returns the new estimate. */
	observe(topic: string, compression: CompressionType, recordBytes: number, encodedBatchBytes: number): number {
		const current = this.ratio(topic, compression)
		const observed = Math.max(encodedBatchBytes - RECORD_BATCH_HEADER_BYTES, 0) / Math.max(recordBytes, 1)
		let next = current
		if (observed > current) {
			next = Math.max(current + DETERIORATE_STEP, observed)
		} else if (observed < current) {
			next = Math.max(current - IMPROVING_STEP, observed)
		}
		this.ratios.set(key(topic, compression), next)
		return next
	}

	/** Java's reset after a batch the broker rejected as too large: back to 1.0, or worse if observed worse. */
	reset(topic: string, compression: CompressionType, observedRatio: number): void {
		this.ratios.set(key(topic, compression), Math.max(INITIAL_RATIO, observedRatio))
	}

	/** Expected encoded batch size for records adding up to `recordBytes`. */
	estimateBatchBytes(topic: string, compression: CompressionType, recordBytes: number): number {
		return RECORD_BATCH_HEADER_BYTES + recordBytes * this.factor(topic, compression)
	}

	/** Most record bytes a batch can hold and still be expected to encode within `limit`. */
	recordBytesWithin(topic: string, compression: CompressionType, limit: number): number {
		return recordBytesWithin(limit, compression, this.ratio(topic, compression))
	}

	private factor(topic: string, compression: CompressionType): number {
		return sizeFactor(compression, this.ratio(topic, compression))
	}
}

/** Most record bytes a batch can hold and still be expected to encode within `limit` at `ratio`. */
export function recordBytesWithin(limit: number, compression: CompressionType, ratio: number): number {
	return (limit - RECORD_BATCH_HEADER_BYTES) / sizeFactor(compression, ratio)
}

function sizeFactor(compression: CompressionType, ratio: number): number {
	// Uncompressed batches are sized exactly, as in MemoryRecordsBuilder.estimatedBytesWritten().
	return compression === CompressionType.None ? 1 : ratio * ESTIMATION_FACTOR
}

function key(topic: string, compression: CompressionType): string {
	return `${compression}:${topic}`
}
