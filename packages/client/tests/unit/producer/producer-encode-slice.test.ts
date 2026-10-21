import { afterEach, describe, expect, it, vi } from 'vitest'

import { Producer } from '@/producer/producer.js'
import type { PartitionBatch, QueuedMessage } from '@/producer/types.js'
import { createMockCluster } from './_helpers.js'

const TOPIC = 'test-topic'

function partitionBatch(partition: number): PartitionBatch {
	// About 15 KiB, under the size up to which snappy compresses synchronously
	const messages: QueuedMessage[] = Array.from({ length: 5 }, (_, i) => ({
		topic: TOPIC,
		partition,
		key: null,
		value: Buffer.from(`{"id":${i},"pad":"${'x'.repeat(3000)}"}`),
		headers: {},
		timestamp: 0n,
		resolve: () => {},
		reject: () => {},
	}))
	return { topic: TOPIC, partition, messages, sizeBytes: 15_000, createdAt: 0 }
}

describe('Producer batch encoding', () => {
	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('yields to the event loop once a drain outlasts its encoding slice', async () => {
		const producer = new Producer(createMockCluster(), { compression: 'snappy', lingerMs: 0 })
		const internals = producer as unknown as {
			prepareProduceBatches(batches: PartitionBatch[]): Promise<unknown[]>
		}
		const batches = Array.from({ length: 20 }, (_, partition) => partitionBatch(partition))

		let now = 0
		vi.spyOn(performance, 'now').mockImplementation(() => (now += 1))
		let turns = 0
		let preparing = true
		const tick = () => {
			if (!preparing) return
			turns++
			setImmediate(tick)
		}
		setImmediate(tick)
		const prepared = await internals.prepareProduceBatches(batches)
		preparing = false

		expect(prepared).toHaveLength(20)
		expect(turns).toBeGreaterThan(0)
	})
})
