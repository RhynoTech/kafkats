import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'
import { encodeBatch } from './helpers/completed-fetch.js'

async function makeFetchManager(checkCrcs: boolean) {
	const cluster = {
		getLeaderForPartition: vi.fn().mockRejectedValue(new Error('no broker in unit test')),
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
			maxWaitMs: 20,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			checkCrcs,
		}
	)
	await fm.poll()
	fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fmAny = fm as any
	const fetch = vi.fn().mockResolvedValue({
		topics: [
			{
				topic: 't',
				partitions: [
					{
						partitionIndex: 0,
						errorCode: ErrorCode.None,
						highWatermark: 1n,
						lastStableOffset: 1n,
						logStartOffset: 0n,
						abortedTransactions: [],
						preferredReadReplica: -1,
						recordsData: batchWithBadCrc(),
					},
				],
			},
		],
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
	})
	await fmAny.fetchFromBrokerToBuffer({ nodeId: 1, fetch }, [fmAny.partitionStates.get('t:0')], 1048576)
	return fm
}

function fetchResponse(recordsData: Buffer) {
	return {
		topics: [
			{
				topic: 't',
				partitions: [
					{
						partitionIndex: 0,
						errorCode: ErrorCode.None,
						highWatermark: 100n,
						lastStableOffset: 100n,
						logStartOffset: 0n,
						abortedTransactions: [],
						preferredReadReplica: -1,
						recordsData,
					},
				],
			},
		],
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
	}
}

function managerWithLeader(fetch: ReturnType<typeof vi.fn>) {
	const broker = { nodeId: 1, fetch }
	const cluster = { getLeaderForPartition: vi.fn().mockResolvedValue(broker), getLogger: () => null }
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
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	return { fm, fmAny: fm as any, broker }
}

/** Valid batches at 0-4 and 5-6, then a batch at 20-22 whose CRC no longer matches. */
function dataWithBadThirdBatch(): Buffer {
	const bad = Buffer.from(encodeBatch([20n, 21n, 22n]))
	bad.writeUInt8(bad.readUInt8(bad.length - 1) ^ 0xff, bad.length - 1)
	return Buffer.concat([encodeBatch([0n, 1n, 2n, 3n, 4n]), encodeBatch([5n, 6n]), bad])
}

const offsetsOf = (batches: Array<{ records: Array<{ offset: bigint }> }>) =>
	batches.flatMap(batch => batch.records.map(record => record.offset))

// A valid batch with its CRC field corrupted (the record data itself is intact, so decode
// succeeds when CRC checking is off and only the checksum comparison fails when it is on).
function batchWithBadCrc(): Buffer {
	const buf = Buffer.from(encodeRecordBatchSync(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n)))
	// CRC is the uint32 at offset 17: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1).
	buf[17] = buf[17]! ^ 0xff
	return buf
}

describe('FetchManager checkCrcs', () => {
	it('surfaces a corrupt batch CRC from poll() when checkCrcs is true', async () => {
		const fm = await makeFetchManager(true)
		await expect(fm.poll()).rejects.toThrow(/CRC mismatch/)
		fm.stop()
	})

	it('delivers the batches before a later batch with a bad CRC, then names that batch', async () => {
		const { fm, fmAny, broker } = managerWithLeader(
			vi.fn().mockResolvedValue(fetchResponse(dataWithBadThirdBatch()))
		)
		// Start the fetch loop before assigning, so the response is buffered rather than drained.
		await fm.poll()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		const state = fmAny.partitionStates.get('t:0')
		await vi.waitFor(() => expect(state.offset).toBe(7n))

		// The partition is not re-fetched in a loop while the bad batch waits behind the valid ones.
		await new Promise(resolve => setTimeout(resolve, 50))
		expect(broker.fetch).toHaveBeenCalledTimes(1)

		expect(offsetsOf(await fm.poll())).toEqual([0n, 1n, 2n, 3n, 4n, 5n, 6n])
		await expect(fm.poll()).rejects.toThrow(/corrupt data.*t-0 at offset 20: CRC mismatch/)
		expect(state.offset).toBe(7n)
		fm.stop()
	})

	it('raises a bad batch from a later response only after the records buffered before it', async () => {
		const bad = Buffer.from(encodeBatch([5n, 6n]))
		bad.writeUInt8(bad.readUInt8(bad.length - 1) ^ 0xff, bad.length - 1)
		const fetch = vi
			.fn()
			.mockResolvedValueOnce(fetchResponse(encodeBatch([0n, 1n, 2n, 3n, 4n])))
			.mockResolvedValue(fetchResponse(bad))
		const { fm, fmAny } = managerWithLeader(fetch)
		await fm.poll()
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
		await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(2))
		await vi.waitFor(() => expect(fmAny.fetchBuffer.queue).toHaveLength(2))

		expect(offsetsOf(await fm.poll())).toEqual([0n, 1n, 2n, 3n, 4n])
		await expect(fm.poll()).rejects.toThrow(/corrupt data.*t-0 at offset 5: CRC mismatch/)
		fm.stop()
	})

	it('skips CRC verification and decodes the batch when checkCrcs is false', async () => {
		const fm = await makeFetchManager(false)
		const batches = await fm.poll()
		expect(batches.flatMap(batch => batch.records.map(record => record.value?.toString('utf-8')))).toEqual([
			'hello',
		])
		fm.stop()
	})
})
