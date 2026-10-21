import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { completedFetch } from './helpers/completed-fetch.js'

describe('FetchManager default fast path', () => {
	it('drains buffered fetches without building scheduler state when priority is disabled', async () => {
		const cluster = { getLeaderForPartition: vi.fn(), getLogger: () => null }
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const offsetManager = new OffsetManager(cluster as any, 'g')
		const manager = new FetchManager(cluster as never, offsetManager, 'earliest', {
			maxBytesPerPartition: 1024,
			minBytes: 1,
			maxWaitMs: 1,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
		})
		await manager.poll()

		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const internal = manager as any
		const buildSchedulerState = vi.spyOn(internal, 'buildSchedulerState')
		internal.fetchBuffer.add(completedFetch('topic-a', 0, [0n]))

		const drained = await internal.drainBuffer()
		expect(drained[0].records.map((record: { offset: bigint }) => record.offset)).toEqual([0n])
		expect(buildSchedulerState).not.toHaveBeenCalled()
		manager.stop()
	})
})
