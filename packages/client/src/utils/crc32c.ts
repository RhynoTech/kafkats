/**
 * CRC32C (Castagnoli) checksum implementation for Kafka protocol
 *
 * Used in RecordBatch v2 format for message integrity verification.
 * Uses the Castagnoli polynomial (0x1EDC6F41) which is optimized for
 * hardware acceleration on modern CPUs.
 *
 * This module uses native hardware-accelerated CRC32C via @node-rs/crc32
 * when available, with a fallback to a pure JS "slice-by-16" implementation.
 */

import { createRequire } from 'node:module'

type NativeCrc32c = (data: Uint8Array) => number

let nativeCrc32c: NativeCrc32c | null = null
try {
	const require = createRequire(import.meta.url)
	const mod = require('@node-rs/crc32') as { crc32c?: unknown }
	if (typeof mod.crc32c === 'function') {
		nativeCrc32c = mod.crc32c as NativeCrc32c
	}
} catch {
	// Optional native implementation not installed/available.
}

/** False when checksums run in JavaScript because @node-rs/crc32 is not installed. */
export const hasNativeCrc32c = nativeCrc32c !== null

// CRC32C polynomial (Castagnoli): 0x1EDC6F41
// Reversed/reflected polynomial for table generation: 0x82F63B78
const CRC32C_POLYNOMIAL = 0x82f63b78

/**
 * Slice-by-16 lookup tables in one array, table k at [256 * k, 256 * k + 256): 16 bytes per step.
 * One typed-array base keeps the hot loop free of per-table loads.
 */
const CRC32C_TABLE: Int32Array = generateCrc32cTable()

function generateCrc32cTable(): Int32Array {
	const table = new Int32Array(16 * 256)

	for (let i = 0; i < 256; i++) {
		let crc = i
		for (let j = 0; j < 8; j++) {
			crc = crc & 1 ? (crc >>> 1) ^ CRC32C_POLYNOMIAL : crc >>> 1
		}
		table[i] = crc
	}

	// Table k advances table k - 1 by one more zero byte
	for (let k = 1; k < 16; k++) {
		for (let i = 0; i < 256; i++) {
			const previous = table[(k - 1) * 256 + i]!
			table[k * 256 + i] = (previous >>> 8) ^ table[previous & 0xff]!
		}
	}

	return table
}

// Below this a DataView costs more to create than the 16-byte steps save
const SLICE_BY_16_MIN_BYTES = 64

/**
 * Calculate CRC32C checksum using pure JS slice-by-16.
 *
 * Reads 32-bit little-endian words through a DataView, which handles any byte offset and host
 * byte order; an Int32Array view needs 4-byte alignment and little-endian hardware.
 */
export function crc32cJS(data: Uint8Array, start: number, end: number): number {
	const T = CRC32C_TABLE
	let crc = -1
	let i = start

	if (end - start >= SLICE_BY_16_MIN_BYTES) {
		const view = new DataView(data.buffer, data.byteOffset, data.byteLength)
		for (const last = end - 16; i <= last; i += 16) {
			const a = view.getInt32(i, true) ^ crc
			const b = view.getInt32(i + 4, true)
			const c = view.getInt32(i + 8, true)
			const d = view.getInt32(i + 12, true)
			crc =
				T[3840 + (a & 0xff)]! ^
				T[3584 + ((a >>> 8) & 0xff)]! ^
				T[3328 + ((a >>> 16) & 0xff)]! ^
				T[3072 + (a >>> 24)]! ^
				T[2816 + (b & 0xff)]! ^
				T[2560 + ((b >>> 8) & 0xff)]! ^
				T[2304 + ((b >>> 16) & 0xff)]! ^
				T[2048 + (b >>> 24)]! ^
				T[1792 + (c & 0xff)]! ^
				T[1536 + ((c >>> 8) & 0xff)]! ^
				T[1280 + ((c >>> 16) & 0xff)]! ^
				T[1024 + (c >>> 24)]! ^
				T[768 + (d & 0xff)]! ^
				T[512 + ((d >>> 8) & 0xff)]! ^
				T[256 + ((d >>> 16) & 0xff)]! ^
				T[d >>> 24]!
		}
	}

	while (i < end) {
		crc = T[(crc ^ data[i++]!) & 0xff]! ^ (crc >>> 8)
	}

	return (crc ^ -1) >>> 0
}

/**
 * Calculate CRC32C checksum of a buffer.
 *
 * Uses native hardware-accelerated CRC32C when available, falling back to the
 * pure JS slice-by-16 implementation otherwise.
 *
 * @param data - The buffer to calculate checksum for
 * @param start - Optional start offset (default: 0)
 * @param length - Optional length (default: data.length - start)
 * @returns The CRC32C checksum as an unsigned 32-bit integer
 */
export function crc32c(data: Buffer, start: number = 0, length?: number): number {
	const end = length !== undefined ? start + length : data.length

	if (nativeCrc32c !== null) {
		const view = start === 0 && end === data.length ? data : data.subarray(start, end)
		return nativeCrc32c(view) >>> 0
	}

	return crc32cJS(data, start, end)
}

/**
 * Verify a CRC32C checksum matches expected value
 *
 * @param data - The buffer to verify
 * @param expected - The expected CRC32C value
 * @param start - Optional start offset
 * @param length - Optional length
 * @returns true if checksum matches
 */
export function verifyCrc32c(data: Buffer, expected: number, start: number = 0, length?: number): boolean {
	return crc32c(data, start, length) === expected
}

/**
 * Calculate CRC32C checksum and return as a signed 32-bit integer
 * (matching Java's int representation used in Kafka protocol)
 *
 * @param data - The buffer to calculate checksum for
 * @param start - Optional start offset
 * @param length - Optional length
 * @returns The CRC32C checksum as a signed 32-bit integer
 */
export function crc32cSigned(data: Buffer, start: number = 0, length?: number): number {
	const unsigned = crc32c(data, start, length)
	// Convert to signed if necessary (values > 0x7FFFFFFF become negative)
	return unsigned > 0x7fffffff ? unsigned - 0x100000000 : unsigned
}
