import { describe, expect, it } from 'vitest'

import { buffer } from '@/codec.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

// Each backlog is larger than the whole fetch buffer (10 x maxBytesPerPartition), so every refetch
// is small and the broker hands it to the first partition in the request that has data.
const maxBytesPerPartition = 16 * 1024
const value = Buffer.alloc(500, 0x61)

describe.concurrent('Consumer (integration) - fetch fairness', () => {
	it('serves every backlogged partition before draining any one of them', async () => {
		const client = createClient('it-fetch-fairness')
		await client.connect()

		const partitions = 40
		const recordsPerPartition = 600
		const topicName = uniqueName('it-fetch-fairness')
		const testTopic = topic<Buffer>(topicName, { value: buffer() })
		await client.createTopics([{ name: topicName, numPartitions: partitions, replicationFactor: 1 }])

		const producer = client.producer({ lingerMs: 0 })
		for (let sent = 0; sent < recordsPerPartition; sent += 10) {
			await producer.send(
				testTopic,
				Array.from({ length: partitions * 10 }, (_, i) => ({ partition: i % partitions, value }))
			)
		}
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition,
		})
		const served = new Set<number>()
		let servedWhenFirstDrained = -1
		const safetyTimer = setTimeout(() => consumer.stop(), 20_000)
		await consumer.runBatch(
			testTopic,
			async (batch, ctx) => {
				served.add(ctx.partition)
				if (servedWhenFirstDrained < 0 && batch.at(-1)!.offset === BigInt(recordsPerPartition - 1)) {
					servedWhenFirstDrained = served.size
					consumer.stop()
				}
				await sleep(5)
			},
			{ autoCommit: false, partitionConcurrency: 3 }
		)
		clearTimeout(safetyTimer)

		expect(servedWhenFirstDrained).toBe(partitions)

		await producer.disconnect()
		await client.disconnect()
	})

	it('keeps serving another topic behind a topic whose first partition is idle', async () => {
		const client = createClient('it-fetch-fairness-topics')
		await client.connect()

		const records = 1000
		const firstName = uniqueName('it-fetch-fairness-a')
		const secondName = uniqueName('it-fetch-fairness-b')
		const first = topic<Buffer>(firstName, { value: buffer() })
		const second = topic<Buffer>(secondName, { value: buffer() })
		await client.createTopics([
			{ name: firstName, numPartitions: 2, replicationFactor: 1 },
			{ name: secondName, numPartitions: 1, replicationFactor: 1 },
		])

		// Partition 0 of the first topic stays empty and therefore never moves in the fetch order, so a
		// request that groups partitions by topic keeps the first topic ahead of the second.
		const producer = client.producer({ lingerMs: 0 })
		for (let sent = 0; sent < records; sent += 10) {
			await producer.send(
				first,
				Array.from({ length: 10 }, () => ({ partition: 1, value }))
			)
			await producer.send(
				second,
				Array.from({ length: 10 }, () => ({ partition: 0, value }))
			)
		}
		await producer.flush()

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition,
		})
		const delivered = new Map<string, number>()
		let deliveredByOtherWhenFirstDrained = -1
		const safetyTimer = setTimeout(() => consumer.stop(), 20_000)
		await consumer.runBatch(
			[first, second],
			async (batch, ctx) => {
				delivered.set(ctx.topic, (delivered.get(ctx.topic) ?? 0) + batch.length)
				if (deliveredByOtherWhenFirstDrained < 0 && batch.at(-1)!.offset === BigInt(records - 1)) {
					deliveredByOtherWhenFirstDrained =
						delivered.get(ctx.topic === firstName ? secondName : firstName) ?? 0
					consumer.stop()
				}
				await sleep(20)
			},
			{
				autoCommit: false,
				partitionConcurrency: 3,
				assignment: [
					{ topic: firstName, partition: 0 },
					{ topic: firstName, partition: 1 },
					{ topic: secondName, partition: 0 },
				],
			}
		)
		clearTimeout(safetyTimer)

		expect(deliveredByOtherWhenFirstDrained).toBeGreaterThan(records * 0.8)

		await producer.disconnect()
		await client.disconnect()
	})
})
