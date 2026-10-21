import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { createRecordBatch, encodeRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'

function makeFetchManager() {
	const cluster = {
		getLeaderForPartition: () => undefined,
		getLogger: () => null,
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g1')
	const fm = new FetchManager(
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		cluster as any,
		offsetManager,
		'earliest',
		{
			maxBytesPerPartition: 1024,
			minBytes: 1,
			maxWaitMs: 50,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
		}
	)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return fm as any
}

describe('FetchManager.decodeRecords corruption handling', () => {
	it('throws on a corrupt leading batch instead of silently dropping the partition records', () => {
		const fm = makeFetchManager()
		// Too short to even read a record-batch header → the first batch fails to decode.
		// The broker always returns at least one complete batch, so this is genuine
		// corruption, not a truncated trailing batch.
		const corrupt = Buffer.from([1, 2, 3, 4, 5])
		expect(() => fm.decodeRecords(corrupt, [], 100n)).toThrow(/corrupt data/)
	})

	it('stops at a truncated trailing batch but returns the records decoded before it', () => {
		const fm = makeFetchManager()
		const valid = encodeRecordBatchSync(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n))
		// One complete batch followed by a truncated second batch (header underflows).
		const buf = Buffer.concat([valid, valid.subarray(0, 6)])

		const result = fm.decodeRecords(buf, [], 100n) as
			| { records: DecodedRecord[]; nextOffset: bigint | null }
			| Promise<{ records: DecodedRecord[]; nextOffset: bigint | null }>
		expect(result).not.toBeInstanceOf(Promise)
		const { records } = result as { records: DecodedRecord[]; nextOffset: bigint | null }
		expect(records.map(r => r.value?.toString('utf-8'))).toEqual(['hello'])
	})

	it('throws on a complete-but-corrupt non-first batch instead of treating it as truncation', async () => {
		const fm = makeFetchManager()
		const first = encodeRecordBatchSync(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n))
		const second = encodeRecordBatchSync(createRecordBatch([{ value: 'world', timestamp: 0 }], 1n, 0n))
		// Flip a byte inside the second batch's CRC-covered region: the batch is fully
		// present (framing intact), so this is corruption, not maxBytes truncation.
		second.writeUInt8(second.readUInt8(second.length - 1) ^ 0xff, second.length - 1)
		const buf = Buffer.concat([first, second])

		// A slow first batch can use up the decode slice, which moves the rest onto the async path.
		await expect(async () => fm.decodeRecords(buf, [], 100n)).rejects.toThrow(/corrupt data/)
	})

	it('throws on a complete-but-corrupt non-first batch on the async (compressed) path', async () => {
		const fm = makeFetchManager()
		const first = await encodeRecordBatch(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n), {
			compression: CompressionType.Gzip,
		})
		const second = encodeRecordBatchSync(createRecordBatch([{ value: 'world', timestamp: 0 }], 1n, 0n))
		second.writeUInt8(second.readUInt8(second.length - 1) ^ 0xff, second.length - 1)
		const buf = Buffer.concat([first, second])

		await expect(async () => fm.decodeRecords(buf, [], 100n)).rejects.toThrow(/corrupt data/)
	})

	it('still tolerates a truncated trailing batch on the async (compressed) path', async () => {
		const fm = makeFetchManager()
		const first = await encodeRecordBatch(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n), {
			compression: CompressionType.Gzip,
		})
		const second = encodeRecordBatchSync(createRecordBatch([{ value: 'world', timestamp: 0 }], 1n, 0n))
		const buf = Buffer.concat([first, second.subarray(0, second.length - 4)])

		const { records } = await fm.decodeRecords(buf, [], 100n)
		expect(records.map((r: DecodedRecord) => r.value?.toString('utf-8'))).toEqual(['hello'])
	})
})

describe('FetchManager.decodeRecords single pass', () => {
	const values = (records: DecodedRecord[]) => records.map(r => r.value?.toString('utf-8'))

	it('decodes uncompressed batches around a compressed one in order, in one pass', async () => {
		const fm = makeFetchManager()
		const first = encodeRecordBatchSync(createRecordBatch([{ value: 'a', timestamp: 0 }], 0n, 0n))
		const second = await encodeRecordBatch(createRecordBatch([{ value: 'b', timestamp: 0 }], 1n, 0n), {
			compression: CompressionType.Gzip,
		})
		const third = encodeRecordBatchSync(createRecordBatch([{ value: 'c', timestamp: 0 }], 2n, 0n))

		const result = fm.decodeRecords(Buffer.concat([first, second, third]), [], 100n)
		expect(result).toBeInstanceOf(Promise)
		const { records, nextOffset } = await result
		expect(values(records)).toEqual(['a', 'b', 'c'])
		expect(nextOffset).toBe(3n)
	})

	it('yields to the event loop once decoding outlasts its time slice', async () => {
		const fm = makeFetchManager()
		const batches = Array.from({ length: 6 }, (_, i) =>
			encodeRecordBatchSync(createRecordBatch([{ value: `v${i}`, timestamp: 0 }], BigInt(i), 0n))
		)
		let now = 0
		const clock = vi.spyOn(performance, 'now').mockImplementation(() => (now += 1))
		const immediate = vi.spyOn(globalThis, 'setImmediate')
		try {
			const result = fm.decodeRecords(Buffer.concat(batches), [], 100n)
			expect(result).toBeInstanceOf(Promise)
			const { records } = await result
			expect(values(records)).toEqual(['v0', 'v1', 'v2', 'v3', 'v4', 'v5'])
			expect(immediate).toHaveBeenCalled()
		} finally {
			clock.mockRestore()
			immediate.mockRestore()
		}
	})
})
