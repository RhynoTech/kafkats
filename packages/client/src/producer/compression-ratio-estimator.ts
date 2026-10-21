/**
 * Compression ratio estimation for sizing record batches before they are encoded.
 *
 * Modelled on the Java client's CompressionRatioEstimator and how MemoryRecordsBuilder uses it
 * (Kafka 3.9): one ratio per topic and codec, starting at 1.0, with a 5% margin. Java lets the
 * ratio improve by only 0.005 per batch; here it follows each observed batch at once. The producer
 * checks a batch's exact size before sending it, so a ratio that turns out low costs one more
 * encode, while one that stays high costs a request for every extra piece, and the pieces of a
 * partition are sent one after another.
 */

import { CompressionType } from '@/protocol/records/compression.js'

export const RECORD_BATCH_HEADER_BYTES = 61

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

	/** Estimate from now on the ratio of a batch that was just encoded; returns it. */
	observe(topic: string, compression: CompressionType, recordBytes: number, encodedBatchBytes: number): number {
		const observed = Math.max(encodedBatchBytes - RECORD_BATCH_HEADER_BYTES, 0) / Math.max(recordBytes, 1)
		this.ratios.set(key(topic, compression), observed)
		return observed
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
