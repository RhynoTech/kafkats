import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import type { IsolationLevel, TopicPartition } from '@/consumer/types.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { FetchRequest } from '@/protocol/messages/requests/fetch.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'

function makeFetchManager(isolationLevel: IsolationLevel = 'read_uncommitted') {
	const broker = { nodeId: 1, fetch: vi.fn() }
	const cluster = {
		getLeaderForPartition: vi.fn().mockResolvedValue(broker),
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
			isolationLevel,
		}
	)
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fmAny = fm as any
	fmAny.fetchBuffer = { add: vi.fn(), isFull: () => false }
	return { broker, fm, fmAny }
}

const oneRecord = encodeRecordBatchSync(createRecordBatch([{ value: 'v' }], 0n, 0n))

interface PartitionData extends TopicPartition {
	recordsData?: Buffer
	abortedTransactions?: Array<{ producerId: bigint; firstOffset: bigint }>
}

function fetchResponse(withData: PartitionData[]) {
	return {
		topics: withData.map(({ topic, partition, recordsData = oneRecord, abortedTransactions = [] }) => ({
			topic,
			partitions: [
				{
					partitionIndex: partition,
					errorCode: ErrorCode.None,
					highWatermark: 100n,
					lastStableOffset: 100n,
					logStartOffset: 0n,
					abortedTransactions,
					preferredReadReplica: -1,
					recordsData,
				},
			],
		})),
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
	}
}

function requestOrder(request: FetchRequest): Array<[string, number[]]> {
	return request.topics.map(entry => [entry.topic, entry.partitions.map(p => p.partitionIndex)])
}

describe('FetchManager fetch order', () => {
	it('moves a partition that returned records behind the others and keeps that order on the wire', async () => {
		const { broker, fm, fmAny } = makeFetchManager()
		fm.addPartitions([
			{ topic: 'a', partition: 0, offset: 0n },
			{ topic: 'a', partition: 1, offset: 0n },
			{ topic: 'b', partition: 0, offset: 0n },
		])

		broker.fetch.mockResolvedValueOnce(fetchResponse([{ topic: 'a', partition: 0 }]))
		await fmAny.fetchFromBrokerToBuffer(broker, [...fmAny.partitionStates.values()], 1048576)

		expect(requestOrder(broker.fetch.mock.calls[0]![0])).toEqual([
			['a', [0, 1]],
			['b', [0]],
		])
		expect([...fmAny.partitionStates.keys()]).toEqual(['a:1', 'b:0', 'a:0'])

		broker.fetch.mockResolvedValueOnce(fetchResponse([]))
		await fmAny.fetchFromBrokerToBuffer(broker, [...fmAny.partitionStates.values()], 1048576)

		// Grouping by topic would put a:0 back in front of b:0.
		expect(requestOrder(broker.fetch.mock.calls[1]![0])).toEqual([
			['a', [1]],
			['b', [0]],
			['a', [0]],
		])
	})

	// The broker's fetch session rotates on response bytes, not on what the client keeps, so a long
	// aborted run must not hold the front of every request.
	it.each([
		{
			content: 'aborted transactional records',
			isolationLevel: 'read_committed' as const,
			recordsData: encodeRecordBatchSync(
				createRecordBatch(
					Array.from({ length: 10 }, () => ({ value: 'v' })),
					0n,
					0n,
					7n,
					0,
					0
				),
				{ isTransactional: true }
			),
			abortedTransactions: [{ producerId: 7n, firstOffset: 0n }],
		},
		{
			content: 'a control batch',
			isolationLevel: 'read_uncommitted' as const,
			recordsData: encodeRecordBatchSync(createRecordBatch([{ value: 'marker' }], 0n, 0n), {
				isControlBatch: true,
			}),
			abortedTransactions: [],
		},
	])(
		'moves a partition behind the others when its fetch returned only $content',
		async ({ isolationLevel, recordsData, abortedTransactions }) => {
			const { broker, fm, fmAny } = makeFetchManager(isolationLevel)
			fm.addPartitions([
				{ topic: 'a', partition: 0, offset: 0n },
				{ topic: 'a', partition: 1, offset: 0n },
			])

			broker.fetch.mockResolvedValueOnce(
				fetchResponse([{ topic: 'a', partition: 0, recordsData, abortedTransactions }])
			)
			await fmAny.fetchFromBrokerToBuffer(broker, [...fmAny.partitionStates.values()], 1048576)

			expect(fmAny.fetchBuffer.add).not.toHaveBeenCalled()
			expect([...fmAny.partitionStates.keys()]).toEqual(['a:1', 'a:0'])
		}
	)
})
