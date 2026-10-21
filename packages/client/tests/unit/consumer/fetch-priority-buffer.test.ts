import { describe, expect, it, vi } from 'vitest'
import { recordBytes } from '@/consumer/completed-fetch.js'
import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { strict, weighted, type PriorityStrategy } from '@/consumer/priority.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { createRecordBatch, encodeRecordBatch } from '@/protocol/records/index.js'
import { completedFetch } from './helpers/completed-fetch.js'

function makeFetchManager(
	maxBytesPerPartition = 300,
	priority: PriorityStrategy = strict({ order: ['live', 'bulk'] })
): FetchManager {
	const cluster = { getLeaderForPartition: vi.fn(), getLogger: () => null }
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g')
	return new FetchManager(cluster as never, offsetManager, 'earliest', {
		maxBytesPerPartition,
		minBytes: 1,
		maxWaitMs: 1,
		partitionConcurrency: 1,
		isolationLevel: 'read_uncommitted',
		maxBufferedBytes: 1000,
		priority,
	})
}

describe('FetchManager priority buffer', () => {
	it('drains priority order, rotates partial partitions, and retains excluded records', async () => {
		const manager = makeFetchManager()
		manager.addPartitions([
			{ topic: 'live', partition: 0, offset: 0n },
			{ topic: 'live', partition: 1, offset: 0n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer
		const fetches = [
			completedFetch('bulk', 0, [10n, 11n]),
			completedFetch('live', 0, [0n, 1n, 2n, 3n]),
			completedFetch('live', 1, [5n, 6n]),
		]
		for (const fetch of fetches) buffer.add(fetch)

		const first = await manager.poll()
		expect(first).toHaveLength(1)
		expect(first[0]).toMatchObject({ topic: 'live', partition: 0 })
		expect(first[0]!.records.map(record => record.offset)).toEqual([0n, 1n, 2n])
		// live:0 is part-way through its batch, so all of its fetched bytes are still held.
		expect(buffer.remainingCapacity()).toBe(1000 - fetches.reduce((sum, fetch) => sum + fetch.heldBytes, 0))

		const second = await manager.poll(new Set(['live:0']))
		expect(second[0]).toMatchObject({ topic: 'live', partition: 1 })
		expect(second.flatMap(batch => batch.records.map(record => record.offset))).toEqual([5n, 6n, 10n, 11n])

		const third = await manager.poll()
		expect(third.flatMap(batch => batch.records.map(record => record.offset))).toEqual([3n])
		manager.stop()
	})

	it('keeps lower weighted shares progressing when an oversized record exceeds its allocation', async () => {
		const manager = makeFetchManager(400, weighted({ shares: { live: 3, bulk: 1 } }))
		manager.addPartitions([
			{ topic: 'live', partition: 0, offset: 0n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer
		// Each 564-byte live record overshoots live's 300-byte share of the 400-byte poll budget.
		buffer.add(completedFetch('live', 0, [0n, 1n], { valueBytes: 500 }))
		buffer.add(completedFetch('bulk', 0, [10n, 11n]))

		const served = (batches: Array<{ topic: string; records: Array<{ offset: bigint }> }>) =>
			batches.map(batch => `${batch.topic}:${batch.records.map(record => record.offset).join(',')}`)
		expect(served(await manager.poll())).toEqual(['live:0', 'bulk:10'])
		expect(served(await manager.poll())).toEqual(['live:1', 'bulk:11'])
		manager.stop()
	})

	it('delivers partitions the plan omitted FIFO within the remaining budget', async () => {
		// A zero cap still delivers one record per poll; holding a topic back is done through fetch.
		const manager = makeFetchManager(400, {
			schedule: () => ({ drain: [{ topic: 'live', partition: 0, maxBytes: 0 }], fetch: [] }),
		})
		manager.addPartitions([
			{ topic: 'live', partition: 0, offset: 0n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])
		await manager.poll()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const buffer = (manager as any).fetchBuffer
		buffer.add(completedFetch('bulk', 0, [10n, 11n]))
		buffer.add(completedFetch('live', 0, [0n, 1n]))
		buffer.add(completedFetch('bulk', 0, [12n, 13n, 14n]))

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const drained = await (manager as any).drainBuffer()
		const offsets = (batches: Array<{ records: Array<{ offset: bigint }> }>) =>
			batches.flatMap(batch => batch.records.map(record => record.offset))
		// All queue entries for the omitted partition are walked, not just the first.
		expect(offsets(drained)).toEqual([0n, 10n, 11n, 12n, 13n])
		expect(offsets(await manager.poll())).toEqual([1n, 14n])
		manager.stop()
	})
})

describe('FetchManager priority with compressed data', () => {
	const budget = 64 * 1024

	function manager(priority: PriorityStrategy): FetchManager {
		const cluster = { getLeaderForPartition: vi.fn(), getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g')
		const fm = new FetchManager(cluster as never, offsetManager, 'earliest', {
			maxBytesPerPartition: budget,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			maxBufferedBytes: 64 * 1024 * 1024,
			priority,
		})
		fm.addPartitions([
			{ topic: 'live', partition: 0, offset: 0n },
			{ topic: 'bulk', partition: 0, offset: 0n },
		])
		return fm
	}

	function batchOf(first: number, count: number, valueBytes: number, compression: CompressionType) {
		const value = Buffer.alloc(valueBytes, 0x61)
		const records = Array.from({ length: count }, () => ({ value, timestamp: 0 }))
		return encodeRecordBatch(createRecordBatch(records, BigInt(first), 0n), { compression })
	}

	/** About 210 KiB of records that gzip to a few KiB. */
	async function liveData(): Promise<Buffer> {
		const batches: Buffer[] = []
		for (let first = 0; first < 200; first += 50) batches.push(await batchOf(first, 50, 1000, CompressionType.Gzip))
		return Buffer.concat(batches)
	}

	async function deliver(fm: FetchManager, topic: string, recordsData: Buffer): Promise<void> {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const internal = fm as any
		const partition = {
			partitionIndex: 0,
			errorCode: ErrorCode.None,
			highWatermark: 1_000_000n,
			lastStableOffset: 1_000_000n,
			logStartOffset: 0n,
			abortedTransactions: [],
			preferredReadReplica: -1,
			recordsData,
		}
		const fetch = vi.fn().mockResolvedValue({ topics: [{ topic, partitions: [partition] }], throttleTimeMs: 0 })
		await internal.fetchFromBrokerToBuffer(
			{ nodeId: 1, fetch },
			[internal.partitionStates.get(`${topic}:0`)],
			1 << 30
		)
	}

	it('splits a weighted poll by decoded size, not by the compressed size buffered', async () => {
		const fm = manager(weighted({ shares: { live: 3, bulk: 1 } }))
		await fm.poll()
		await deliver(fm, 'live', await liveData())
		await deliver(fm, 'bulk', await batchOf(0, 100, 600, CompressionType.None))

		const taken = { live: 0, bulk: 0 }
		for (const batch of await fm.poll()) {
			for (const record of batch.records) taken[batch.topic as keyof typeof taken] += recordBytes(record)
		}
		expect(taken.live / (taken.live + taken.bulk)).toBeCloseTo(0.75, 1)
		fm.stop()
	})

	it('withholds a lower strict tier as soon as a compressed higher tier holds a poll of records', async () => {
		const strategy = strict({ order: ['live', 'bulk'] })
		const fm = manager(strategy)
		await fm.poll()
		await deliver(fm, 'live', await liveData())

		// Before any record is decoded ahead for a poll, which would count them at their decoded size.
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const internal = fm as any
		const decision = strategy.schedule(internal.buildSchedulerState(internal.fetchBuffer))
		expect(decision.fetch.map(tp => tp.topic)).toEqual(['live'])
		fm.stop()
	})
})
