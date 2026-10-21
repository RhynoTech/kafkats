import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

const emptyResponse = { throttleTimeMs: 0, errorCode: ErrorCode.None, sessionId: 0, topics: [] }

function createFetchManager(cluster: { getLeaderForPartition: unknown; refreshMetadata: unknown }) {
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const mockCluster = { ...cluster, getLogger: () => null } as any
	return new FetchManager(mockCluster, new OffsetManager(mockCluster, 'g1'), 'earliest', {
		maxBytesPerPartition: 1024,
		minBytes: 1,
		maxWaitMs: 20,
		partitionConcurrency: 1,
		isolationLevel: 'read_uncommitted',
	})
}

describe('FetchManager after a failed fetch', () => {
	it('refreshes metadata and looks the leader up again instead of retrying a dead connection', async () => {
		const deadBroker = {
			nodeId: 1,
			fetch: vi.fn().mockRejectedValue(new Error('Cannot send on disconnected connection')),
		}
		const reconnectedBroker = {
			nodeId: 1,
			fetch: vi.fn().mockResolvedValue(emptyResponse),
		}
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValueOnce(deadBroker).mockResolvedValue(reconnectedBroker),
			refreshMetadata: vi.fn().mockResolvedValue(undefined),
		}
		const fm = createFetchManager(cluster)
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

		const polling = fm.poll()
		await vi.waitFor(() => expect(reconnectedBroker.fetch).toHaveBeenCalled(), { timeout: 1000 })
		await fm.stop()
		await polling

		expect(deadBroker.fetch).toHaveBeenCalledTimes(1)
		expect(cluster.getLeaderForPartition).toHaveBeenCalledTimes(2)
		expect(cluster.refreshMetadata).toHaveBeenCalledWith(['t'])
	})

	it('keeps refreshing metadata while the leader cannot be reached, until it names the new leader', async () => {
		const deadBroker = { nodeId: 1, fetch: vi.fn().mockRejectedValue(new Error('Connection closed')) }
		const newLeader = { nodeId: 2, fetch: vi.fn().mockResolvedValue(emptyResponse) }
		// After a crash the controller moves leadership only seconds later, so the refresh that follows
		// the failed fetch still names the dead broker.
		let refreshes = 0
		let leader: 'dead' | 'unreachable' | 'moved' = 'dead'
		const cluster = {
			getLeaderForPartition: vi.fn(async () => {
				if (leader === 'dead') {
					leader = 'unreachable'
					return deadBroker
				}
				if (leader === 'unreachable') throw new Error('connect ECONNREFUSED')
				return newLeader
			}),
			refreshMetadata: vi.fn(async () => {
				if (++refreshes === 3) leader = 'moved'
			}),
		}
		const fm = createFetchManager(cluster)
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

		const polling = fm.poll()
		await vi.waitFor(() => expect(newLeader.fetch).toHaveBeenCalled(), { timeout: 2000 })
		await fm.stop()
		await polling

		expect(deadBroker.fetch).toHaveBeenCalledTimes(1)
		expect(cluster.refreshMetadata).toHaveBeenCalledTimes(3)
		expect(cluster.refreshMetadata).toHaveBeenLastCalledWith(['t'])
	})

	it('backs off a failing leader lookup while another broker keeps fetching', async () => {
		const healthy = {
			nodeId: 1,
			fetch: vi.fn(() => new Promise(resolve => setTimeout(() => resolve(emptyResponse), 100))),
			closeFetchSession: vi.fn().mockResolvedValue(undefined),
		}
		let deadLookups = 0
		const cluster = {
			getLeaderForPartition: vi.fn(async (_topic: string, partition: number) => {
				if (partition === 0) return healthy
				deadLookups++
				throw new Error('connect ECONNREFUSED')
			}),
			refreshMetadata: vi.fn().mockResolvedValue(undefined),
		}
		const fm = createFetchManager(cluster)
		fm.addPartitions([
			{ topic: 't', partition: 0, offset: 0n },
			{ topic: 't', partition: 1, offset: 0n },
		])

		const polling = fm.poll()
		await new Promise(resolve => setTimeout(resolve, 1000))
		await fm.stop()
		await polling

		// 50 ms doubling per failure: lookups at about 0, 50, 150, 350 and 750 ms.
		expect(deadLookups).toBeGreaterThanOrEqual(3)
		expect(deadLookups).toBeLessThanOrEqual(6)
		expect(cluster.refreshMetadata.mock.calls.length).toBeLessThanOrEqual(deadLookups)
		expect(healthy.fetch.mock.calls.length).toBeGreaterThanOrEqual(5)
	})

	it('does not refresh metadata for a fetch that fails after stop', async () => {
		let failFetch: (error: Error) => void = () => {}
		const broker = {
			nodeId: 1,
			fetch: vi.fn(
				() =>
					new Promise((_resolve, reject) => {
						failFetch = reject
					})
			),
			closeFetchSession: vi.fn().mockResolvedValue(undefined),
		}
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValue(broker),
			refreshMetadata: vi.fn().mockResolvedValue(undefined),
		}
		const fm = createFetchManager(cluster)
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

		const polling = fm.poll()
		await vi.waitFor(() => expect(broker.fetch).toHaveBeenCalled(), { timeout: 1000 })
		await fm.stop()
		await polling
		// Closing the client's connections rejects the long poll still in flight.
		failFetch(new Error('Connection closing'))
		await new Promise(resolve => setTimeout(resolve, 50))

		// A refresh now would reconnect a broker the client is closing, and nothing would close it again.
		expect(cluster.refreshMetadata).not.toHaveBeenCalled()
	})
})
