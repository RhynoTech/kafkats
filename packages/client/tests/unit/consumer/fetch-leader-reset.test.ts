import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

describe('FetchManager after a failed fetch', () => {
	it('refreshes metadata and looks the leader up again instead of retrying a dead connection', async () => {
		const deadBroker = {
			nodeId: 1,
			fetch: vi.fn().mockRejectedValue(new Error('Cannot send on disconnected connection')),
		}
		const reconnectedBroker = {
			nodeId: 1,
			fetch: vi
				.fn()
				.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None, sessionId: 0, topics: [] }),
		}
		const cluster = {
			getLeaderForPartition: vi.fn().mockResolvedValueOnce(deadBroker).mockResolvedValue(reconnectedBroker),
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
				maxWaitMs: 20,
				partitionConcurrency: 1,
				isolationLevel: 'read_uncommitted',
			}
		)
		fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])

		const polling = fm.poll()
		await vi.waitFor(() => expect(reconnectedBroker.fetch).toHaveBeenCalled(), { timeout: 1000 })
		await fm.stop()
		await polling

		expect(deadBroker.fetch).toHaveBeenCalledTimes(1)
		expect(cluster.getLeaderForPartition).toHaveBeenCalledTimes(2)
		expect(cluster.refreshMetadata).toHaveBeenCalledWith(['t'])
	})
})
