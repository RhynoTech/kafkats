import { afterEach, describe, expect, it, vi } from 'vitest'

import { CompletedFetch, readFetchedRecords } from '@/consumer/completed-fetch.js'
import type { FetchedRecords, ReadFetchedRecordsOptions } from '@/consumer/completed-fetch.js'
import { compressionCodecs } from '@/protocol/records/codec-registry.js'
import { CompressionType, getCompressionTypeName, gzipCodec } from '@/protocol/records/compression.js'
import { createRecordBatch, decodeRecordBatch, encodeRecordBatch } from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import { crc32c } from '@/utils/crc32c.js'
import { drainAll } from './helpers/completed-fetch.js'

interface BatchOptions {
	compression?: CompressionType
	producerId?: bigint
	transactional?: boolean
	control?: boolean
	logAppendTime?: boolean
}

/** A batch with a record at each offset; gaps in the offsets make it a compacted (sparse) batch. */
function batch(offsets: bigint[], options: BatchOptions = {}): Promise<Buffer> {
	const records = createRecordBatch(
		offsets.map(offset => ({
			key: `k${offset}`,
			value: `value-${offset}`,
			headers: { h: `${offset}` },
			timestamp: 1_000 + Number(offset),
		})),
		offsets[0]!,
		1_000n,
		options.producerId ?? -1n
	)
	records.records.forEach((record, index) => {
		record.offsetDelta = Number(offsets[index]! - offsets[0]!)
	})
	records.lastOffsetDelta = Number(offsets[offsets.length - 1]! - offsets[0]!)
	return encodeRecordBatch(records, {
		compression: options.compression,
		isTransactional: options.transactional,
		isControlBatch: options.control,
		timestampType: options.logAppendTime ? 'LogAppendTime' : 'CreateTime',
	})
}

function read(
	data: Buffer,
	options: Partial<ReadFetchedRecordsOptions> = {}
): FetchedRecords | Promise<FetchedRecords> {
	return readFetchedRecords(data, {
		abortedTransactions: [],
		lastStableOffset: 1_000_000n,
		readCommitted: false,
		verifyCrc: true,
		...options,
		fetchOffset: options.fetchOffset ?? data.readBigInt64BE(0),
	})
}

async function drainOffsets(data: Buffer, options: Partial<ReadFetchedRecordsOptions> = {}) {
	const fetched = await read(data, options)
	const records = await drainAll(new CompletedFetch('t', 0, 0, fetched))
	return { fetched, offsets: records.map(record => record.offset) }
}

/** Corrupt a compressed batch's payload but keep its CRC valid, so only decompression can fail. */
function corruptPayload(encoded: Buffer): Buffer {
	const copy = Buffer.from(encoded)
	copy.fill(0xff, 61 + 10, copy.length)
	copy.writeUInt32BE(crc32c(copy, 21, copy.length - 21), 17)
	return copy
}

const codecs = [CompressionType.Gzip, CompressionType.Snappy, CompressionType.Lz4, CompressionType.Zstd]
	.filter(type => compressionCodecs.has(type))
	.map(type => [getCompressionTypeName(type), type] as const)

afterEach(() => {
	vi.restoreAllMocks()
})

describe('readFetchedRecords', () => {
	it('reads batch headers only, so nothing is decompressed until a record is drained', async () => {
		const decompress = vi.spyOn(gzipCodec, 'decompress')
		const data = Buffer.concat([
			await batch([0n, 1n, 2n], { compression: CompressionType.Gzip }),
			await batch([3n, 4n], { compression: CompressionType.Gzip }),
		])

		const fetched = await read(data)
		expect(fetched).toMatchObject({ nextOffset: 5n, recordCount: 5 })
		expect(decompress).not.toHaveBeenCalled()

		const fetch = new CompletedFetch('t', 0, 0, fetched)
		expect((await fetch.peek())?.offset).toBe(0n)
		expect(decompress).toHaveBeenCalledTimes(1)
	})

	it('filters records below the fetch offset in a dense batch that straddles it', async () => {
		const { fetched, offsets } = await drainOffsets(await batch([0n, 1n, 2n, 3n, 4n]), { fetchOffset: 3n })
		expect(fetched.recordCount).toBe(2)
		expect(fetched.nextOffset).toBe(5n)
		expect(offsets).toEqual([3n, 4n])
	})

	it('decodes a compacted batch that straddles the fetch offset to count its records', async () => {
		for (const compression of [CompressionType.None, CompressionType.Gzip]) {
			const data = await batch([0n, 4n, 9n], { compression })
			const fetched = read(data, { fetchOffset: 5n })
			expect(fetched).toBeInstanceOf(Promise)
			expect(await fetched).toMatchObject({ recordCount: 1, nextOffset: 10n })
			const { offsets } = await drainOffsets(data, { fetchOffset: 5n })
			expect(offsets).toEqual([9n])
		}
	})

	it('counts a compacted batch from its header when the fetch offset does not split it', async () => {
		const fetched = read(await batch([0n, 4n, 9n]), { fetchOffset: 0n })
		expect(fetched).not.toBeInstanceOf(Promise)
		expect(fetched).toMatchObject({ recordCount: 3, nextOffset: 10n })
	})

	it('skips control batches and still advances past them', async () => {
		const data = Buffer.concat([
			await batch([0n, 1n]),
			await batch([2n], { control: true }),
			await batch([3n], { compression: CompressionType.Gzip }),
		])
		const { fetched, offsets } = await drainOffsets(data)
		expect(fetched).toMatchObject({ recordCount: 3, nextOffset: 4n })
		expect(offsets).toEqual([0n, 1n, 3n])

		const markerOnly = await read(await batch([7n], { control: true }))
		expect(markerOnly).toMatchObject({ recordCount: 0, nextOffset: 8n, batches: [] })
	})

	it('drops aborted transactions for read_committed, each ending at its producer marker', async () => {
		const data = Buffer.concat([
			await batch([0n, 1n], { producerId: 7n, transactional: true, compression: CompressionType.Gzip }),
			await batch([2n], { producerId: 7n, transactional: true, control: true }),
			await batch([3n, 4n], { producerId: 8n, transactional: true }),
			await batch([5n], { producerId: 8n, transactional: true, control: true }),
			await batch([6n], { producerId: 7n, transactional: true }),
			await batch([7n], { producerId: 7n }),
		])
		const abortedTransactions = [{ producerId: 7n, firstOffset: 0n }]

		const committed = await drainOffsets(data, { readCommitted: true, abortedTransactions })
		expect(committed.fetched.recordCount).toBe(4)
		expect(committed.offsets).toEqual([3n, 4n, 6n, 7n])

		const uncommitted = await drainOffsets(data, { readCommitted: false, abortedTransactions })
		expect(uncommitted.fetched.recordCount).toBe(6)
		expect(uncommitted.offsets).toEqual([0n, 1n, 3n, 4n, 6n, 7n])
	})

	it('ends an aborted transaction at the last stable offset when its marker was not fetched', async () => {
		const data = Buffer.concat([
			await batch([0n], { producerId: 7n, transactional: true }),
			await batch([1n, 2n], { producerId: 7n, transactional: true }),
			await batch([3n], { producerId: 9n, transactional: true }),
		])
		const { fetched, offsets } = await drainOffsets(data, {
			readCommitted: true,
			abortedTransactions: [{ producerId: 7n, firstOffset: 1n }],
			lastStableOffset: 4n,
		})
		expect(fetched.recordCount).toBe(2)
		expect(offsets).toEqual([0n, 3n])
	})

	it('filters per record when an aborted range starts inside a batch', async () => {
		const { fetched, offsets } = await drainOffsets(
			await batch([0n, 1n, 2n], { producerId: 7n, transactional: true }),
			{ readCommitted: true, abortedTransactions: [{ producerId: 7n, firstOffset: 1n }], lastStableOffset: 3n }
		)
		expect(fetched.recordCount).toBe(1)
		expect(offsets).toEqual([0n])
	})

	it('stops at a truncated trailing batch and copies only the complete batches', async () => {
		const first = await batch([0n, 1n])
		const second = await batch([2n, 3n])
		for (const cut of [6, second.length - 4]) {
			const { fetched, offsets } = await drainOffsets(Buffer.concat([first, second.subarray(0, cut)]))
			expect(fetched).toMatchObject({ recordCount: 2, nextOffset: 2n })
			expect(fetched.data.length).toBe(first.length)
			expect(offsets).toEqual([0n, 1n])
		}
	})

	it('throws on a corrupt first batch instead of treating it as truncation', async () => {
		expect(() => read(Buffer.from([1, 2, 3, 4, 5]), { fetchOffset: 0n })).toThrow(/truncated/)
		const badMagic = Buffer.from(await batch([0n]))
		badMagic.writeInt8(1, 16)
		expect(() => read(badMagic)).toThrow(/magic/)
	})

	it('throws on a complete batch with a bad CRC after the first, and skips the check when disabled', async () => {
		const second = Buffer.from(await batch([1n]))
		second.writeUInt8(second.readUInt8(second.length - 1) ^ 0xff, second.length - 1)
		const data = Buffer.concat([await batch([0n]), second])
		expect(() => read(data)).toThrow(/CRC mismatch/)
		expect(await read(data, { verifyCrc: false })).toMatchObject({ recordCount: 2 })
	})
})

describe('CompletedFetch', () => {
	it('keeps a cursor inside a partly drained batch and decompresses each batch once', async () => {
		const decompress = vi.spyOn(gzipCodec, 'decompress')
		const fetched = await read(
			Buffer.concat([
				await batch([0n, 1n, 2n, 3n], { compression: CompressionType.Gzip }),
				await batch([4n, 5n], { compression: CompressionType.Gzip }),
			])
		)
		const fetch = new CompletedFetch('t', 0, 0, fetched)
		expect(fetch.heldBytes).toBe(fetched.data.length)

		const taken: bigint[] = []
		for (let index = 0; index < 2; index++) {
			taken.push((await fetch.peek())!.offset)
			fetch.consume()
		}
		// The rest of the open batch stays decompressed for the next drain, and is counted as held.
		expect(fetch.heldBytes).toBeGreaterThan(fetched.data.length)
		expect(fetch.position).toBe(0n)
		fetch.commit()
		expect(fetch.position).toBe(2n)

		taken.push(...(await drainAll(fetch)).map(record => record.offset))
		expect(taken).toEqual([0n, 1n, 2n, 3n, 4n, 5n])
		expect(decompress).toHaveBeenCalledTimes(2)
		expect(fetch.isEmpty).toBe(true)
		expect(fetch.heldBytes).toBe(fetched.data.length)
	})

	it.each(codecs)('delivers the records an eager decode does with %s compression', async (_name, compression) => {
		const encoded = [
			await batch([0n, 1n, 2n], { compression }),
			await batch([3n, 5n], { compression }),
			await batch([6n, 7n], { compression, logAppendTime: true }),
		]
		const eager: DecodedRecord[] = []
		for (const data of encoded) eager.push(...(await decodeRecordBatch(data)).records)

		const lazy = await drainAll(new CompletedFetch('t', 0, 0, await read(Buffer.concat(encoded))))
		expect(lazy).toEqual(eager)
		expect(lazy.map(record => record.offset)).toEqual([0n, 1n, 2n, 3n, 5n, 6n, 7n])
		expect(lazy[6]!.timestamp).toBe(1_007n)
	})

	it('throws from a drain, not from the read, when a compressed batch fails to decompress', async () => {
		const data = Buffer.concat([
			await batch([0n]),
			corruptPayload(await batch([1n, 2n], { compression: CompressionType.Gzip })),
		])
		const fetch = new CompletedFetch('t', 0, 0, await read(data))
		expect((await fetch.peek())!.offset).toBe(0n)
		fetch.consume()
		fetch.commit()
		await expect(async () => fetch.peek()).rejects.toThrow()
		expect(fetch.position).toBe(1n)
	})
})
