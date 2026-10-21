import { randomBytes } from 'node:crypto'
import { describe, expect, it } from 'vitest'

import type { KafkaClient } from '@/client/index.js'
import { TransactionAbortedError } from '@/client/errors.js'
import type { ProducerTraceEvent } from '@/producer/types.js'

import { createClient } from '../helpers/kafka.js'
import { uniqueName } from '../helpers/testkit.js'

const LIMIT = 1_048_576

/** Incompressible records adding up to `totalBytes`, numbered in a header so order can be checked. */
function randomRecords(totalBytes: number) {
	return Array.from({ length: Math.ceil(totalBytes / 3000) }, (_, i) => ({
		key: 'one-key',
		value: randomBytes(3000),
		headers: { seq: String(i) },
	}))
}

/** Read the topic with read_committed until `expected` records or the `sentinel` value arrive. */
async function readCommitted(
	client: KafkaClient,
	topicName: string,
	until: { expected?: number; sentinel?: string }
): Promise<Array<{ seq: number; value: Buffer }>> {
	const consumer = client.consumer({
		groupId: uniqueName('it-split-verify'),
		autoOffsetReset: 'earliest',
		isolationLevel: 'read_committed',
	})
	const received: Array<{ seq: number; value: Buffer }> = []
	await consumer.runEach(
		topicName,
		async message => {
			const value = message.value as Buffer
			received.push({ seq: Number(message.headers.seq?.toString() ?? -1), value })
			if (received.length === until.expected || value.toString() === until.sentinel) {
				consumer.stop()
			}
		},
		{ autoCommit: false }
	)
	return received
}

describe.concurrent('Producer (integration) - record batch splitting', () => {
	it('commits a 3 MB one-partition transactional send as several batches under 1 MiB, in order', async () => {
		const client = createClient('it-split-default')
		await client.connect()
		const topicName = uniqueName('it-split-default')
		await client.createTopics([{ name: topicName, numPartitions: 1, replicationFactor: 1 }])

		const produced: ProducerTraceEvent[] = []
		const producer = client.producer({
			transactionalId: uniqueName('tx-split'),
			compression: 'snappy',
			trace: e => void (e.stage === 'produce_request' && produced.push(e)),
		})
		const records = randomRecords(3_000_000)

		await producer.transaction(async tx => void (await tx.send(topicName, records)))

		expect(produced.length).toBeGreaterThanOrEqual(3)
		expect(produced.every(e => e.bytes! <= LIMIT)).toBe(true)
		const received = await readCommitted(client, topicName, { expected: records.length })
		expect(received.map(r => r.seq)).toEqual(records.map((_, i) => i))
		expect(received.every((r, i) => r.value.equals(records[i]!.value))).toBe(true)

		await producer.disconnect()
		await client.disconnect()
	}, 60_000)

	it('aborts a send that a topic max.message.bytes below 1 MiB rejects, and the next transaction commits', async () => {
		const client = createClient('it-split-low-limit')
		await client.connect()
		const topicName = uniqueName('it-split-low-limit')
		await client.createTopics([
			{ name: topicName, numPartitions: 1, replicationFactor: 1, configs: { 'max.message.bytes': '100000' } },
		])
		const producer = client.producer({ transactionalId: uniqueName('tx-split-low'), compression: 'snappy' })

		await expect(
			producer.transaction(async tx => void (await tx.send(topicName, randomRecords(3_000_000))))
		).rejects.toThrow(TransactionAbortedError)
		await producer.transaction(async tx => void (await tx.send(topicName, { value: Buffer.from('sentinel') })))

		const received = await readCommitted(client, topicName, { sentinel: 'sentinel' })
		expect(received.map(r => r.value.toString())).toEqual(['sentinel'])

		await producer.disconnect()
		await client.disconnect()
	}, 60_000)
})
