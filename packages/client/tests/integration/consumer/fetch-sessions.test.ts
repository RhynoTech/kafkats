import { afterEach, describe, expect, it, vi } from 'vitest'

import { Broker } from '@/client/broker.js'
import { buffer } from '@/codec.js'
import type { Consumer } from '@/consumer/consumer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { FetchRequest } from '@/protocol/messages/requests/fetch.js'
import type { FetchResponse } from '@/protocol/messages/responses/fetch.js'
import { topic } from '@/topic.js'

import { createClient } from '../helpers/kafka.js'
import { sleep, uniqueName } from '../helpers/testkit.js'

interface Exchange {
	broker: Broker
	request: FetchRequest
	response?: FetchResponse
}

const fetchThroughBroker = Broker.prototype.fetch
const value = Buffer.alloc(200, 0x61)

/**
 * Record every Fetch this file sends. `before` runs ahead of a request and may act on the broker
 * first, which lets a test break the session at an exact point. The spy is per file, so the tests
 * here run one at a time.
 */
function recordFetches(before?: (exchange: Exchange, index: number) => Promise<void>): Exchange[] {
	const exchanges: Exchange[] = []
	vi.spyOn(Broker.prototype, 'fetch').mockImplementation(async function (this: Broker, request: FetchRequest) {
		const exchange: Exchange = { broker: this, request }
		exchanges.push(exchange)
		await before?.(exchange, exchanges.length - 1)
		exchange.response = await fetchThroughBroker.call(this, request)
		return exchange.response
	})
	return exchanges
}

function partitionsIn(topics: Array<{ partitions: unknown[] }>): number {
	return topics.reduce((count, entry) => count + entry.partitions.length, 0)
}

async function untilRunning(consumer: Consumer): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		consumer.once('running', () => resolve())
		consumer.once('error', reject)
	})
}

async function produceBacklog(
	client: ReturnType<typeof createClient>,
	topicName: string,
	partitions: number,
	perPartition: number
): Promise<void> {
	const producer = client.producer({ lingerMs: 0 })
	const t = topic<Buffer>(topicName, { value: buffer() })
	for (let sent = 0; sent < perPartition; sent += 50) {
		await producer.send(
			t,
			Array.from({ length: partitions * 50 }, (_, i) => ({ partition: i % partitions, value }))
		)
	}
	await producer.flush()
	await producer.disconnect()
}

/** Consume `perPartition` records from each partition, failing on any gap, duplicate or reordering. */
async function consumeExactlyOnce(
	consumer: Consumer,
	topicName: string,
	partitions: number,
	perPartition: number,
	onRecord?: (partition: number, offset: bigint) => void
): Promise<void> {
	const next = Array<bigint>(partitions).fill(0n)
	const problems: string[] = []
	let remaining = partitions * perPartition
	await consumer.runEach(
		topic<Buffer>(topicName, { value: buffer() }),
		async message => {
			if (message.offset !== next[message.partition]) {
				problems.push(`p${message.partition}: expected ${next[message.partition]}, got ${message.offset}`)
			}
			next[message.partition] = message.offset + 1n
			onRecord?.(message.partition, message.offset)
			if (--remaining === 0) consumer.stop()
		},
		{ autoCommit: false }
	)
	expect(problems).toEqual([])
	expect(next).toEqual(Array<bigint>(partitions).fill(BigInt(perPartition)))
}

afterEach(() => {
	vi.restoreAllMocks()
})

describe('Consumer (integration) - incremental fetch sessions', () => {
	it('names only changed partitions once the broker created a session, and closes it on stop', async () => {
		const exchanges = recordFetches()
		const client = createClient('it-fetch-sessions-incremental')
		await client.connect()

		const topicName = uniqueName('it-fetch-sessions')
		const t = topic<Buffer>(topicName, { value: buffer() })
		await client.createTopics([{ name: topicName, numPartitions: 8, replicationFactor: 1 }])

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxWaitMs: 200,
		})
		const offsets: bigint[] = []
		let stoppedAt = 0
		const runPromise = consumer.runEach(
			t,
			async message => {
				offsets.push(message.offset)
				if (offsets.length === 30) {
					stoppedAt = exchanges.length
					consumer.stop()
				}
			},
			{ autoCommit: false }
		)
		await untilRunning(consumer)

		const producer = client.producer({ lingerMs: 0 })
		for (let round = 0; round < 3; round++) {
			await producer.send(
				t,
				Array.from({ length: 10 }, () => ({ partition: 3, value }))
			)
			await sleep(300)
		}
		await runPromise

		expect(offsets).toEqual(Array.from({ length: 30 }, (_, i) => BigInt(i)))

		// The last request may still have been in flight when stop() closed the session.
		const [first, ...rest] = exchanges.slice(0, stoppedAt - 1)
		expect(first!.request).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
		expect(partitionsIn(first!.request.topics)).toBe(8)
		const sessionId = first!.response!.sessionId
		expect(sessionId).toBeGreaterThan(0)

		expect(rest.length).toBeGreaterThan(1)
		rest.forEach((exchange, index) => {
			expect(exchange.request).toMatchObject({ sessionId, sessionEpoch: index + 1 })
			expect(exchange.response!.errorCode).toBe(ErrorCode.None)
			// Only partition 3 ever moves, so it is the only partition an incremental request names.
			expect(exchange.request.topics.flatMap(entry => entry.partitions.map(p => p.partitionIndex))).toEqual(
				partitionsIn(exchange.request.topics) === 0 ? [] : [3]
			)
			expect(partitionsIn(exchange.response!.topics)).toBeLessThanOrEqual(1)
		})
		expect(rest.some(exchange => partitionsIn(exchange.request.topics) === 1)).toBe(true)

		// The broker no longer knows the session: stop() closed it.
		const probe = await fetchThroughBroker.call(first!.broker, {
			maxWaitMs: 0,
			minBytes: 0,
			sessionId,
			sessionEpoch: 1,
			topics: [],
		})
		expect(probe.errorCode).toBe(ErrorCode.FetchSessionIdNotFound)

		await producer.disconnect()
		await client.disconnect()
	})

	it('starts a new session when the broker lost the old one, without skipping or repeating offsets', async () => {
		let closedSession = 0
		const exchanges = recordFetches(async (exchange, index) => {
			const sessionId = exchange.request.sessionId ?? 0
			if (index === 3 && sessionId > 0) {
				closedSession = sessionId
				await exchange.broker.closeFetchSession(sessionId)
			}
		})
		const client = createClient('it-fetch-sessions-lost')
		await client.connect()

		const topicName = uniqueName('it-fetch-sessions-lost')
		await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		await produceBacklog(client, topicName, 4, 1000)

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: 16 * 1024,
		})
		await consumeExactlyOnce(consumer, topicName, 4, 1000)

		expect(closedSession).toBeGreaterThan(0)
		expect(exchanges[3]!.response!.errorCode).toBe(ErrorCode.FetchSessionIdNotFound)
		expect(exchanges[4]!.request).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
		expect(partitionsIn(exchanges[4]!.request.topics)).toBe(4)
		expect(exchanges[4]!.response!.sessionId).not.toBe(closedSession)
		expect(exchanges[5]!.request).toMatchObject({ sessionId: exchanges[4]!.response!.sessionId, sessionEpoch: 1 })

		await client.disconnect()
	})

	it('recovers when another request moved the session epoch on, without skipping or repeating offsets', async () => {
		const exchanges = recordFetches(async (exchange, index) => {
			const { sessionId, sessionEpoch } = exchange.request
			if (index === 3 && sessionId! > 0) {
				// A request the broker treats as this session's next one, so the consumer's is stale.
				await fetchThroughBroker.call(exchange.broker, {
					maxWaitMs: 0,
					minBytes: 0,
					sessionId,
					sessionEpoch,
					topics: [],
				})
			}
		})
		const client = createClient('it-fetch-sessions-epoch')
		await client.connect()

		const topicName = uniqueName('it-fetch-sessions-epoch')
		await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		await produceBacklog(client, topicName, 4, 1000)

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: 16 * 1024,
		})
		await consumeExactlyOnce(consumer, topicName, 4, 1000)

		const stale = exchanges[3]!
		expect(stale.response!.errorCode).toBe(ErrorCode.InvalidFetchSessionEpoch)
		// The retry closes the stale session (its ID with epoch 0) and opens another.
		expect(exchanges[4]!.request).toMatchObject({ sessionId: stale.request.sessionId, sessionEpoch: 0 })
		expect(exchanges[4]!.response!.sessionId).toBeGreaterThan(0)
		expect(exchanges[4]!.response!.sessionId).not.toBe(stale.request.sessionId)

		await client.disconnect()
	})

	it('falls back to a full fetch after a failed request, without skipping or repeating offsets', async () => {
		const exchanges = recordFetches(async (_exchange, index) => {
			if (index === 3) throw new Error('simulated connection loss')
		})
		const client = createClient('it-fetch-sessions-error')
		await client.connect()

		const topicName = uniqueName('it-fetch-sessions-error')
		await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		await produceBacklog(client, topicName, 4, 1000)

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: 16 * 1024,
		})
		await consumeExactlyOnce(consumer, topicName, 4, 1000)

		const failedSession = exchanges[3]!.request.sessionId
		expect(failedSession).toBeGreaterThan(0)
		expect(exchanges[4]!.request).toMatchObject({ sessionId: failedSession, sessionEpoch: 0 })
		expect(partitionsIn(exchanges[4]!.request.topics)).toBe(4)

		await client.disconnect()
	})

	it('keeps every offset across pause/resume and seek', async () => {
		const exchanges = recordFetches()
		const client = createClient('it-fetch-sessions-pause-seek')
		await client.connect()

		const topicName = uniqueName('it-fetch-sessions-pause-seek')
		const t = topic<Buffer>(topicName, { value: buffer() })
		await client.createTopics([{ name: topicName, numPartitions: 4, replicationFactor: 1 }])
		await produceBacklog(client, topicName, 4, 1000)

		const consumer = client.consumer({
			groupId: uniqueName('it-group'),
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: 16 * 1024,
		})
		const delivered = Array.from({ length: 4 }, () => [] as bigint[])
		let paused = false
		let sought = false
		await consumer.runEach(
			t,
			async message => {
				delivered[message.partition]!.push(message.offset)
				if (!paused && message.partition === 0 && message.offset === 200n) {
					paused = true
					consumer.pause([
						{ topic: topicName, partition: 0 },
						{ topic: topicName, partition: 1 },
					])
					setTimeout(() => {
						consumer.resume([
							{ topic: topicName, partition: 0 },
							{ topic: topicName, partition: 1 },
						])
					}, 300)
				}
				if (!sought && message.partition === 2 && message.offset === 700n) {
					sought = true
					consumer.seek(topicName, 2, 500n)
				}
				// Partition 2 is done once its replay from 500 reached the end again.
				const replayed = delivered[2]!.length > 1000
				if (replayed && delivered.every(offsets => offsets.at(-1) === 999n)) consumer.stop()
			},
			{ autoCommit: false }
		)

		const range = (from: number, to: number) => Array.from({ length: to - from }, (_, i) => BigInt(from + i))
		expect(delivered[0]).toEqual(range(0, 1000))
		expect(delivered[1]).toEqual(range(0, 1000))
		expect(delivered[3]).toEqual(range(0, 1000))
		// runEach finishes the batch it was in when seek() ran, then continues from the seek target.
		const resumedAt = delivered[2]!.indexOf(500n, 501)
		expect(delivered[2]!.slice(0, resumedAt)).toEqual(range(0, resumedAt))
		expect(delivered[2]!.slice(resumedAt)).toEqual(range(500, 1000))

		// Paused partitions left the session and came back, and the seek was sent as a changed offset.
		const forgotten = exchanges.flatMap(exchange =>
			(exchange.request.forgottenTopicsData ?? []).flatMap(entry => entry.partitions)
		)
		expect(forgotten).toEqual(expect.arrayContaining([0, 1]))
		expect(
			exchanges.some(exchange =>
				exchange.request.topics.some(entry =>
					entry.partitions.some(p => p.partitionIndex === 2 && p.fetchOffset === 500n)
				)
			)
		).toBe(true)
		expect(exchanges.slice(0, -1).every(exchange => exchange.response?.errorCode === ErrorCode.None)).toBe(true)

		await client.disconnect()
	})

	it('hands partitions over in a rebalance without skipping or repeating offsets', async () => {
		recordFetches()
		const client = createClient('it-fetch-sessions-rebalance')
		await client.connect()

		const partitions = 6
		const perPartition = 1500
		const topicName = uniqueName('it-fetch-sessions-rebalance')
		const t = topic<Buffer>(topicName, { value: buffer() })
		await client.createTopics([{ name: topicName, numPartitions: partitions, replicationFactor: 1 }])
		await produceBacklog(client, topicName, partitions, perPartition)

		const groupId = uniqueName('it-group')
		const config = { groupId, autoOffsetReset: 'earliest' as const, maxBytesPerPartition: 16 * 1024 }
		const otherClient = createClient('it-fetch-sessions-rebalance-2')
		await otherClient.connect()
		const first = client.consumer(config)
		const second = otherClient.consumer(config)
		const seen = Array.from({ length: partitions }, () => new Map<bigint, number>())
		let total = 0
		const handler = async (message: { partition: number; offset: bigint }) => {
			const counts = seen[message.partition]!
			counts.set(message.offset, (counts.get(message.offset) ?? 0) + 1)
			total++
			await sleep(0)
		}

		const firstRun = first.runEach(t, handler)
		await untilRunning(first)
		await vi.waitFor(() => expect(total).toBeGreaterThan(1000), { timeout: 20_000 })
		const secondRun = second.runEach(t, handler)
		await vi.waitFor(() => expect(total).toBeGreaterThan(4000), { timeout: 20_000 })
		first.stop()
		await firstRun
		await vi.waitFor(() => expect(seen.every(counts => counts.size === perPartition)).toBe(true), {
			timeout: 30_000,
		})
		second.stop()
		await secondRun

		const repeated = seen.flatMap((counts, partition) =>
			[...counts].filter(([, count]) => count > 1).map(([offset]) => `p${partition}@${offset}`)
		)
		expect(repeated).toEqual([])

		await otherClient.disconnect()
		await client.disconnect()
	}, 90_000)
})
