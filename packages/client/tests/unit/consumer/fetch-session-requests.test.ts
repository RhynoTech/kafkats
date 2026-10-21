import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

function partitionResponse(partitionIndex: number, withRecords: boolean) {
	return {
		partitionIndex,
		errorCode: ErrorCode.None,
		highWatermark: 50n,
		lastStableOffset: 50n,
		logStartOffset: 0n,
		abortedTransactions: [],
		preferredReadReplica: -1,
		recordsData: withRecords ? Buffer.from('records') : null,
	}
}

function fetchResponse(sessionId: number, partitions: Array<{ partition: number; records?: boolean }>, errorCode = 0) {
	return {
		throttleTimeMs: 0,
		errorCode,
		sessionId,
		topics:
			partitions.length === 0
				? []
				: [
						{
							topic: 't',
							topicId: '',
							partitions: partitions.map(p => partitionResponse(p.partition, p.records ?? false)),
						},
					],
	}
}

function setup() {
	const broker = {
		nodeId: 1,
		fetch: vi.fn(),
		closeFetchSession: vi.fn().mockResolvedValue(undefined),
	}
	const cluster = {
		getLeaderForPartition: vi.fn().mockResolvedValue(broker),
		refreshMetadata: vi.fn().mockResolvedValue(undefined),
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
	const fmAny = fm as any
	fmAny.fetchBuffer = {
		add: vi.fn(),
		removePartitions: vi.fn(),
		removePartitionsReturningEarliest: vi.fn(() => new Map()),
		isFull: () => false,
		clear: vi.fn(),
	}
	fmAny.decodeRecords = vi.fn().mockReturnValue({
		records: [
			{ offset: 10n, key: null, value: Buffer.from('a'), headers: {}, timestamp: 0n },
			{ offset: 11n, key: null, value: Buffer.from('b'), headers: {}, timestamp: 0n },
		],
		nextOffset: 12n,
	})
	fm.addPartitions([
		{ topic: 't', partition: 0, offset: 10n },
		{ topic: 't', partition: 1, offset: 20n },
	])
	const fetchOnce = async () => {
		const ready = [...fmAny.partitionStates.values()].filter((state: { paused: boolean }) => !state.paused)
		await fmAny.fetchFromBrokerToBuffer(broker, ready, 1 << 20)
	}
	const lastRequest = () => broker.fetch.mock.calls.at(-1)![0]
	return { broker, fm, fmAny, fetchOnce, lastRequest }
}

/** Partitions a request names, as `partition@fetchOffset`. */
function named(request: { topics: Array<{ partitions: Array<{ partitionIndex: number; fetchOffset: bigint }> }> }) {
	return request.topics.flatMap(topic => topic.partitions.map(p => `${p.partitionIndex}@${p.fetchOffset}`))
}

describe('FetchManager fetch sessions', () => {
	it('sends a full fetch first, then names only the partitions whose position moved', async () => {
		const { broker, fmAny, fetchOnce, lastRequest } = setup()
		broker.fetch.mockResolvedValueOnce(fetchResponse(7, [{ partition: 0, records: true }, { partition: 1 }]))

		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionId: 0, sessionEpoch: 0, forgottenTopicsData: [] })
		expect(named(lastRequest())).toEqual(['0@10', '1@20'])
		expect(fmAny.partitionStates.get('t:0').offset).toBe(12n)

		// An incremental response leaves out partitions with nothing new.
		broker.fetch.mockResolvedValueOnce(fetchResponse(7, []))
		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionId: 7, sessionEpoch: 1, forgottenTopicsData: [] })
		expect(named(lastRequest())).toEqual(['0@12'])
		expect(fmAny.partitionStates.get('t:1').offset).toBe(20n)

		broker.fetch.mockResolvedValueOnce(fetchResponse(7, []))
		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionId: 7, sessionEpoch: 2, topics: [], forgottenTopicsData: [] })
	})

	it('forgets a paused partition and adds it back on resume', async () => {
		const { broker, fm, fetchOnce, lastRequest } = setup()
		broker.fetch.mockResolvedValue(fetchResponse(7, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()
		broker.fetch.mockResolvedValue(fetchResponse(7, []))

		fm.pausePartitions([{ topic: 't', partition: 1 }])
		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionEpoch: 1, topics: [] })
		expect(lastRequest().forgottenTopicsData).toEqual([
			{ topic: 't', topicId: '00000000-0000-0000-0000-000000000000', partitions: [1] },
		])

		fm.resumePartitions([{ topic: 't', partition: 1 }])
		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionEpoch: 2, forgottenTopicsData: [] })
		expect(named(lastRequest())).toEqual(['1@20'])
	})

	it('sends a sought position as a changed partition', async () => {
		const { broker, fm, fetchOnce, lastRequest } = setup()
		broker.fetch.mockResolvedValue(fetchResponse(7, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()
		broker.fetch.mockResolvedValue(fetchResponse(7, []))

		fm.seekPartition('t', 1, 5n)
		await fetchOnce()

		expect(named(lastRequest())).toEqual(['1@5'])
	})

	it('falls back to a full fetch after a failed request', async () => {
		const { broker, fetchOnce, lastRequest } = setup()
		broker.fetch.mockResolvedValueOnce(fetchResponse(7, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()

		broker.fetch.mockRejectedValueOnce(new Error('connection closed'))
		await expect(fetchOnce()).rejects.toThrow('connection closed')

		broker.fetch.mockResolvedValueOnce(fetchResponse(8, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionId: 7, sessionEpoch: 0 })
		expect(named(lastRequest())).toEqual(['0@10', '1@20'])
	})

	it('ignores the partitions of a response the session rejects', async () => {
		const { broker, fmAny, fetchOnce, lastRequest } = setup()
		broker.fetch.mockResolvedValueOnce(fetchResponse(7, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()

		// Partition 5 is not in the session, so the response cannot belong to it.
		broker.fetch.mockResolvedValueOnce({
			...fetchResponse(7, [{ partition: 0, records: true }]),
			topics: [{ topic: 't', topicId: '', partitions: [partitionResponse(0, true), partitionResponse(5, true)] }],
		})
		await fetchOnce()

		expect(fmAny.fetchBuffer.add).not.toHaveBeenCalled()
		expect(fmAny.partitionStates.get('t:0').offset).toBe(10n)

		broker.fetch.mockResolvedValueOnce(fetchResponse(9, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()
		expect(lastRequest()).toMatchObject({ sessionId: 7, sessionEpoch: 0 })
	})

	it('closes open sessions on stop, even when the broker cannot be reached', async () => {
		const { broker, fm, fetchOnce } = setup()
		broker.fetch.mockResolvedValueOnce(fetchResponse(7, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()
		broker.closeFetchSession.mockRejectedValueOnce(new Error('connection closed'))

		await expect(fm.stop()).resolves.toBeUndefined()

		expect(broker.closeFetchSession).toHaveBeenCalledWith(7)
		await fm.stop()
		expect(broker.closeFetchSession).toHaveBeenCalledTimes(1)
	})

	it('does not hold up stop for a broker that does not answer the close', async () => {
		const { broker, fm, fetchOnce } = setup()
		broker.fetch.mockResolvedValueOnce(fetchResponse(7, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()
		broker.closeFetchSession.mockReturnValueOnce(new Promise(() => {}))

		vi.useFakeTimers()
		try {
			let stopped = false
			void fm.stop().then(() => {
				stopped = true
			})
			await vi.advanceTimersByTimeAsync(1000)
			expect(stopped).toBe(true)
		} finally {
			vi.useRealTimers()
		}
		expect(broker.closeFetchSession).toHaveBeenCalledWith(7)
	})

	it('does not close anything when the broker never created a session', async () => {
		const { broker, fm, fetchOnce, lastRequest } = setup()
		broker.fetch.mockResolvedValue(fetchResponse(0, [{ partition: 0 }, { partition: 1 }]))
		await fetchOnce()
		await fetchOnce()

		expect(lastRequest()).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
		await fm.stop()
		expect(broker.closeFetchSession).not.toHaveBeenCalled()
	})
})
