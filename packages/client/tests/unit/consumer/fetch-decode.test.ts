import { gunzipSync } from 'node:zlib'

import { afterEach, describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { CompressionType, gzipCodec } from '@/protocol/records/compression.js'
import { createRecordBatch, encodeRecordBatch } from '@/protocol/records/index.js'
import { crc32c } from '@/utils/crc32c.js'
import { LoopSlice } from '@/utils/loop-slice.js'
import { encodeBatch } from './helpers/completed-fetch.js'

const broker = { nodeId: 1 }

async function makeFetchManager() {
	const cluster = {
		getLeaderForPartition: vi.fn().mockRejectedValue(new Error('no broker in unit test')),
		getLogger: () => null,
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g1')
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fm = new FetchManager(cluster as any, offsetManager, 'earliest', {
		maxBytesPerPartition: 1024,
		minBytes: 1,
		maxWaitMs: 20,
		partitionConcurrency: 1,
		isolationLevel: 'read_uncommitted',
	})
	// Create the buffer before any partition is assigned, so the background loop stays idle.
	await fm.poll()
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fmAny = fm as any
	return { fm, fmAny }
}

/** Respond to one fetch with the given records data per partition of topic `t`. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function deliver(fmAny: any, data: Record<number, Buffer>): Promise<void> {
	const partitions = Object.keys(data).map(Number)
	const states = partitions.map(partition => fmAny.partitionStates.get(`t:${partition}`))
	const fetch = vi.fn().mockResolvedValue({
		topics: [
			{
				topic: 't',
				partitions: partitions.map(partition => ({
					partitionIndex: partition,
					errorCode: ErrorCode.None,
					highWatermark: 100n,
					lastStableOffset: 100n,
					logStartOffset: 0n,
					abortedTransactions: [],
					preferredReadReplica: -1,
					recordsData: data[partition],
				})),
			},
		],
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
	})
	await fmAny.fetchFromBrokerToBuffer({ ...broker, fetch }, states, 1048576)
}

function gzipBatch(offsets: bigint[]): Promise<Buffer> {
	return encodeRecordBatch(
		createRecordBatch(
			offsets.map(offset => ({ value: `v${offset}`, timestamp: 0 })),
			offsets[0]!,
			0n
		),
		{ compression: CompressionType.Gzip }
	)
}

/** A gzip batch of records estimated at 100 bytes each, at consecutive offsets from `first`. */
function gzipBatchOf(first: bigint, count: number): Promise<Buffer> {
	const records = Array.from({ length: count }, () => ({ value: Buffer.alloc(36), timestamp: 0 }))
	return encodeRecordBatch(createRecordBatch(records, first, 0n), { compression: CompressionType.Gzip })
}

const offsetRange = (first: bigint, count: number) => Array.from({ length: count }, (_, index) => first + BigInt(index))

/** Corrupt a compressed batch's payload but keep its CRC valid, so only decompression can fail. */
function corruptPayload(encoded: Buffer): Buffer {
	const copy = Buffer.from(encoded)
	copy.fill(0xff, 61 + 10, copy.length)
	copy.writeUInt32BE(crc32c(copy, 21, copy.length - 21), 17)
	return copy
}

const offsetsOf = (batches: Array<{ records: Array<{ offset: bigint }> }>) =>
	batches.flatMap(batch => batch.records.map(record => record.offset))

afterEach(() => {
	vi.restoreAllMocks()
})

describe('FetchManager lazy decoding', () => {
	it('surfaces corrupt data from the poll after the one reaching it, buffering the other partitions', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([
			{ topic: 't', partition: 0, offset: 0n },
			{ topic: 't', partition: 1, offset: 0n },
		])

		// Too short to hold a batch header: the broker always returns the first batch whole.
		await deliver(fmAny, { 0: Buffer.from([1, 2, 3, 4, 5]), 1: encodeBatch([0n, 1n]) })
		expect(fmAny.partitionStates.get('t:0').offset).toBe(0n)
		expect(fmAny.partitionStates.get('t:1').offset).toBe(2n)

		expect(offsetsOf(await fm.poll())).toEqual([0n, 1n])
		await expect(fm.poll()).rejects.toThrow(/corrupt data.*t-0 at offset 0/)
		fm.stop()
	})

	it('delivers the records before a batch that fails to decode, then surfaces it and rewinds to it', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		await deliver(fmAny, { 0: Buffer.concat([encodeBatch([0n, 1n]), corruptPayload(await gzipBatch([2n, 3n]))]) })
		const state = fmAny.partitionStates.get('t:0')
		expect(state.offset).toBe(4n)
		const epoch = state.fetchEpoch

		expect(offsetsOf(await fm.poll())).toEqual([0n, 1n])
		expect(state.offset).toBe(2n)
		expect(state.fetchEpoch).toBe(epoch + 1)
		await expect(fm.poll()).rejects.toThrow(/corrupt data.*t-0 at offset 2/)
		fm.stop()
	})

	it('decodes what the next poll takes in the background, so the poll path does not decompress', async () => {
		const decompress = vi.spyOn(gzipCodec, 'decompress')
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		const batches: Buffer[] = []
		for (let first = 0n; first < 30n; first += 5n) batches.push(await gzipBatchOf(first, 5))
		await deliver(fmAny, { 0: Buffer.concat(batches) })

		// A 1024-byte poll takes ten records and looks at the eleventh, so it needs three batches.
		await vi.waitFor(() => expect(decompress).toHaveBeenCalledTimes(3))
		expect(offsetsOf(await fm.poll())).toEqual(offsetRange(0n, 10))
		expect(decompress).toHaveBeenCalledTimes(3)

		// Once the poll returns, the next poll's records are decoded while its caller works.
		await vi.waitFor(() => expect(decompress).toHaveBeenCalledTimes(5))
		await new Promise(resolve => setTimeout(resolve, 20))
		expect(decompress).toHaveBeenCalledTimes(5)
		const [fetch] = fmAny.fetchBuffer.queue
		expect(fmAny.fetchBuffer.sizeBytes).toBeGreaterThanOrEqual(fetch.data.length + 11 * 100)

		expect(offsetsOf(await fm.poll())).toEqual(offsetRange(10n, 10))
		expect(decompress).toHaveBeenCalledTimes(5)
		fm.stop()
	})

	it('never decodes data dropped by pause, seek or revoke', async () => {
		const decompress = vi.spyOn(gzipCodec, 'decompress')
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([0, 1, 2].map(partition => ({ topic: 't', partition, offset: 0n })))
		await deliver(fmAny, { 0: await gzipBatch([0n, 1n]), 1: await gzipBatch([0n, 1n]), 2: await gzipBatch([0n]) })
		expect(fmAny.fetchBuffer.sizeBytes).toBeGreaterThan(0)

		fm.pausePartitions([{ topic: 't', partition: 0 }])
		fm.seekPartition('t', 1, 0n)
		fm.removePartitions([{ topic: 't', partition: 2 }])

		expect(fmAny.fetchBuffer.sizeBytes).toBe(0)
		expect(await fm.poll()).toEqual([])
		expect(decompress).not.toHaveBeenCalled()
		fm.stop()
	})

	it('drops a partition paused while a drain awaits decompression, rewinding to the records it took', async () => {
		let release: () => void = () => {}
		const gate = new Promise<void>(resolve => {
			release = resolve
		})
		vi.spyOn(gzipCodec, 'decompress').mockImplementation(async data => {
			await gate
			return gunzipSync(data)
		})
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([
			{ topic: 't', partition: 0, offset: 0n },
			{ topic: 't', partition: 1, offset: 0n },
		])
		await deliver(fmAny, { 0: Buffer.concat([encodeBatch([0n]), await gzipBatch([1n, 2n])]), 1: encodeBatch([5n]) })
		const state = fmAny.partitionStates.get('t:0')
		expect(state.offset).toBe(3n)

		const polling = fm.poll()
		// The drain has taken offset 0 and waits on the compressed batch behind it.
		await new Promise(resolve => setTimeout(resolve, 5))
		fm.pausePartitions([{ topic: 't', partition: 0 }])
		expect(state.offset).toBe(0n)
		release()

		const batches = await polling
		expect(batches.map(batch => batch.partition)).toEqual([1])
		expect(offsetsOf(batches)).toEqual([5n])
		fm.stop()
	})

	it.each([
		['sought', (fm: FetchManager) => fm.seekPartition('t', 0, 0n)],
		['revoked', (fm: FetchManager) => fm.removePartitions([{ topic: 't', partition: 0 }])],
	])('drops a partition %s while a drain awaits its decompression', async (_action, act) => {
		let release: () => void = () => {}
		const gate = new Promise<void>(resolve => {
			release = resolve
		})
		vi.spyOn(gzipCodec, 'decompress').mockImplementation(async data => {
			await gate
			return gunzipSync(data)
		})
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		await deliver(fmAny, { 0: Buffer.concat([encodeBatch([0n]), await gzipBatch([1n, 2n])]) })

		const polling = fm.poll()
		await new Promise(resolve => setTimeout(resolve, 5))
		act(fm)
		release()

		expect(await polling).toEqual([])
		expect(fmAny.fetchBuffer.sizeBytes).toBe(0)
		fm.stop()
	})

	it('holds a copy of the complete batches, not a view of the response', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		const complete = encodeBatch([0n, 1n])
		const response = Buffer.concat([Buffer.alloc(32), complete, encodeBatch([2n]).subarray(0, 20)])
		await deliver(fmAny, { 0: response.subarray(32) })

		const [fetch] = fmAny.fetchBuffer.queue
		expect(fetch.data.buffer).not.toBe(response.buffer)
		expect(fetch.data).toEqual(complete)
		expect(fmAny.fetchBuffer.sizeBytes).toBe(complete.length)
		fm.stop()
	})
})

describe('FetchManager response slicing', () => {
	it('yields between partitions once reading a response outlasts its slice, keeping response order', async () => {
		const { fm, fmAny } = await makeFetchManager()
		const partitions = [0, 1, 2, 3]
		fm.addPartitions(partitions.map(partition => ({ topic: 't', partition, offset: 0n })))
		vi.spyOn(LoopSlice.prototype, 'expired', 'get').mockReturnValue(true)
		const yields = vi.spyOn(LoopSlice.prototype, 'yield')
		await deliver(fmAny, Object.fromEntries(partitions.map(partition => [partition, encodeBatch([0n, 1n])])))

		// One yield before the response is read, then one before each partition. Decoding ahead, which
		// runs during those yields, has a slice of its own.
		const responseYields = yields.mock.contexts.filter(slice => slice !== fmAny.fetchBuffer.decodeSlice)
		expect(responseYields).toHaveLength(1 + partitions.length)
		expect(fmAny.fetchBuffer.queue.map((fetch: { partition: number }) => fetch.partition)).toEqual(partitions)
		expect((await fm.poll()).map(batch => [batch.partition, batch.records.length])).toEqual(
			partitions.map(partition => [partition, 2])
		)
		fm.stop()
	})

	it('drops a partition paused while the response yielded before it', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([0, 1].map(partition => ({ topic: 't', partition, offset: 0n })))
		vi.spyOn(LoopSlice.prototype, 'expired', 'get').mockReturnValue(true)
		const yieldOnce = LoopSlice.prototype.yield
		let yields = 0
		vi.spyOn(LoopSlice.prototype, 'yield').mockImplementation(function (this: LoopSlice) {
			if (++yields === 3) fm.pausePartitions([{ topic: 't', partition: 1 }])
			return yieldOnce.call(this)
		})
		await deliver(fmAny, { 0: encodeBatch([0n, 1n]), 1: encodeBatch([0n, 1n]) })

		expect(fmAny.fetchBuffer.queue.map((fetch: { partition: number }) => fetch.partition)).toEqual([0])
		expect(fmAny.partitionStates.get('t:0').offset).toBe(2n)
		expect(fmAny.partitionStates.get('t:1').offset).toBe(0n)
		fm.stop()
	})
})

describe('FetchManager decode slicing', () => {
	it('yields to the event loop while back-to-back polls decode', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		await deliver(fmAny, { 0: encodeBatch(offsetRange(0n, 40)) })

		let now = performance.now()
		vi.spyOn(performance, 'now').mockImplementation(() => (now += 1))
		let turns = 0
		let polling = true
		const tick = () => {
			if (!polling) return
			turns++
			setImmediate(tick)
		}
		setImmediate(tick)
		// Handlers that settle without I/O let the next poll start in the same loop turn.
		const offsets: bigint[] = []
		while (fmAny.fetchBuffer.hasDataFor()) offsets.push(...offsetsOf(await fm.poll()))
		polling = false

		expect(offsets).toEqual(offsetRange(0n, 40))
		expect(turns).toBeGreaterThan(0)
		fm.stop()
	})

	it('yields to the event loop while decoding ahead of the next poll', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		vi.spyOn(LoopSlice.prototype, 'expired', 'get').mockReturnValue(true)
		const yields = vi.spyOn(LoopSlice.prototype, 'yield')
		await deliver(fmAny, { 0: encodeBatch(offsetRange(0n, 40)) })

		// Ten records fill the next poll's 1024-byte budget, and the eleventh is decoded to find that out.
		await vi.waitFor(() => expect(fmAny.fetchBuffer.queue[0].tail).toBe(11))
		const decodeYields = yields.mock.contexts.filter(slice => slice === fmAny.fetchBuffer.decodeSlice)
		expect(decodeYields.length).toBeGreaterThanOrEqual(11)
		expect(offsetsOf(await fm.poll())).toEqual(offsetRange(0n, 10))
		fm.stop()
	})

	it('neither decodes nor delivers a partition paused while a poll yielded before it', async () => {
		const { fm, fmAny } = await makeFetchManager()
		fm.addPartitions([0, 1].map(partition => ({ topic: 't', partition, offset: 0n })))
		await deliver(fmAny, { 0: await gzipBatch([0n, 1n]), 1: await gzipBatch([0n, 1n]) })
		const decompress = vi.spyOn(gzipCodec, 'decompress')
		vi.spyOn(LoopSlice.prototype, 'expired', 'get').mockReturnValue(true)
		const yieldOnce = LoopSlice.prototype.yield
		let yields = 0
		vi.spyOn(LoopSlice.prototype, 'yield').mockImplementation(function (this: LoopSlice) {
			if (++yields === 1) fm.pausePartitions([{ topic: 't', partition: 0 }])
			return yieldOnce.call(this)
		})

		expect((await fm.poll()).map(batch => [batch.partition, offsetsOf([batch])])).toEqual([[1, [0n, 1n]]])
		expect(decompress).toHaveBeenCalledOnce()
		expect(fmAny.partitionStates.get('t:0').offset).toBe(0n)
		fm.stop()
	})
})
