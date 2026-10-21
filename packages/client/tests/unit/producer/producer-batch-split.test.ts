import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { ProducerFencedError, TransactionAbortedError } from '@/client/errors.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import { compressionCodecs } from '@/protocol/records/codec-registry.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { createMockBroker, createMockCluster, buildProduceResponse, type MockBroker } from './_helpers.js'

const TOPIC = 'test-topic'
const LIMIT = 1_048_576
const THREE_MIB = 3 * 1024 * 1024

// RecordBatch v2 header offsets
const PRODUCER_EPOCH_OFFSET = 51
const BASE_SEQUENCE_OFFSET = 53
const RECORD_COUNT_OFFSET = 57

type SentBatch = {
	partition: number
	bytes: number
	producerEpoch: number
	baseSequence: number
	recordCount: number
	accepted: boolean
}

type FakeLeaderOptions = {
	/** Batches above this size are rejected with MESSAGE_TOO_LARGE, like a topic's max.message.bytes. */
	brokerLimit?: number
	delayMs?: number
	/** Error to answer the n-th partition batch with (0-based, counting every batch sent). */
	failBatch?: (index: number, batch: SentBatch) => ErrorCode | undefined
}

/** Broker stand-in: assigns offsets per partition and answers each partition batch like a leader would. */
function fakeLeader(broker: MockBroker, opts: FakeLeaderOptions = {}): SentBatch[] {
	const brokerLimit = opts.brokerLimit ?? Number.POSITIVE_INFINITY
	const sent: SentBatch[] = []
	const nextOffset = new Map<number, bigint>()
	broker.produce.mockImplementation(async (request: ProduceRequest) => {
		if (opts.delayMs) await new Promise(resolve => setTimeout(resolve, opts.delayMs))
		const topics = request.topics.map(t => ({
			name: t.name,
			partitions: t.partitions.map(p => {
				const records = p.records as Buffer
				const batch = {
					partition: p.partitionIndex,
					bytes: records.length,
					producerEpoch: records.readInt16BE(PRODUCER_EPOCH_OFFSET),
					baseSequence: records.readInt32BE(BASE_SEQUENCE_OFFSET),
					recordCount: records.readInt32BE(RECORD_COUNT_OFFSET),
					accepted: records.length <= brokerLimit,
				}
				const injected = opts.failBatch?.(sent.length, batch)
				sent.push(batch)
				if (injected !== undefined) {
					batch.accepted = false
					return { partitionIndex: p.partitionIndex, errorCode: injected }
				}
				if (!batch.accepted) {
					return { partitionIndex: p.partitionIndex, errorCode: ErrorCode.MessageTooLarge }
				}
				const baseOffset = nextOffset.get(p.partitionIndex) ?? 0n
				nextOffset.set(p.partitionIndex, baseOffset + BigInt(batch.recordCount))
				return { partitionIndex: p.partitionIndex, errorCode: ErrorCode.None, baseOffset }
			}),
		}))
		return buildProduceResponse(topics)
	})
	return sent
}

/** `count` records of `valueBytes` each; with compression 'none' the encoded batch is about their total size. */
function messages(count: number, valueBytes: number, partition = 0) {
	return Array.from({ length: count }, (_, i) => ({ value: Buffer.alloc(valueBytes, `v${i % 10}`), partition }))
}

/** Records of 5000 bytes adding up to at least `totalBytes`. */
function sized(totalBytes: number, partition = 0) {
	return messages(Math.ceil(totalBytes / 5000), 5000, partition)
}

function expectContiguous(batches: SentBatch[]) {
	let next = batches[0]!.baseSequence
	for (const b of batches) {
		expect(b.baseSequence).toBe(next)
		next += b.recordCount
	}
}

/** Batches still counted as in the send pipeline, once the last send's bookkeeping has run. */
async function pendingCount(producer: Producer): Promise<number> {
	await new Promise(resolve => setTimeout(resolve, 10))
	return (producer as unknown as { accumulator: { pendingCount: number } }).accumulator.pendingCount
}

/** Make zstd an identity codec whose compress() waits `delayMs(input)` ms; returns the restore function. */
function slowIdentityZstd(delayMs: (data: Buffer) => number): () => void {
	const original = compressionCodecs.get(CompressionType.Zstd)
	compressionCodecs.register(CompressionType.Zstd, {
		compress: async data => {
			await new Promise(resolve => setTimeout(resolve, delayMs(data)))
			return data
		},
		decompress: async data => data,
	})
	return () => {
		if (original) compressionCodecs.register(CompressionType.Zstd, original)
	}
}

describe('Producer record batch splitting', () => {
	let mockCluster: ReturnType<typeof createMockCluster>
	let mockBroker: MockBroker

	function useCluster(partitionCount: number) {
		mockCluster = createMockCluster({ partitionCount })
		mockCluster.getLeaderForPartition.mockResolvedValue(mockBroker)
		mockCluster.getAnyBroker.mockResolvedValue(mockBroker)
		mockCluster.getCoordinator.mockResolvedValue(mockBroker)
	}

	beforeEach(() => {
		mockBroker = createMockBroker(1)
		useCluster(1)
		let epoch = 0
		mockBroker.initProducerId.mockImplementation(async () => ({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			producerId: 7n,
			producerEpoch: epoch++,
		}))
		mockBroker.addPartitionsToTxn.mockImplementation(async request => ({
			throttleTimeMs: 0,
			errorCode: ErrorCode.None,
			results: request.topics.map(t => ({
				name: t.name,
				resultsByPartition: t.partitions.map(partitionIndex => ({ partitionIndex, errorCode: ErrorCode.None })),
			})),
		}))
		mockBroker.endTxn.mockResolvedValue({ throttleTimeMs: 0, errorCode: ErrorCode.None })
	})

	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('sends a 3 MiB one-partition send as several ordered batches with contiguous sequences', async () => {
		const sent = fakeLeader(mockBroker)
		const encodes: number[] = []
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			trace: e => void (e.stage === 'encode_batch' && encodes.push(e.bytes!)),
		})
		const batch = sized(THREE_MIB)

		const results = await producer.send(TOPIC, batch)

		expect(sent.length).toBe(4)
		expect(sent.every(b => b.bytes <= LIMIT && b.accepted)).toBe(true)
		expect(sent.reduce((s, b) => s + b.recordCount, 0)).toBe(batch.length)
		expect(sent[0]!.baseSequence).toBe(0)
		expectContiguous(sent)
		// Encoding first costs one encode of the whole share before it is cut.
		expect(encodes).toHaveLength(sent.length + 1)
		expect(encodes[0]).toBeGreaterThan(THREE_MIB)
		expect(results.map(r => r.offset)).toEqual(Array.from({ length: batch.length }, (_, i) => BigInt(i)))
		expect(await pendingCount(producer)).toBe(0)
	})

	it('cuts by record bytes, so one large record does not force repeated splits', async () => {
		const sent = fakeLeader(mockBroker)
		const encodes: number[] = []
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			trace: e => void (e.stage === 'encode_batch' && encodes.push(e.bytes!)),
		})

		await producer.send(TOPIC, [
			...messages(300, 1000),
			{ value: Buffer.alloc(900_000, 'b'), partition: 0 },
			...messages(300, 1000),
		])

		expect(sent).toHaveLength(3)
		expect(sent[0]!.recordCount).toBe(300)
		expect(sent.every(b => b.bytes <= LIMIT)).toBe(true)
		expect(sent.reduce((s, b) => s + b.recordCount, 0)).toBe(601)
		expect(encodes).toHaveLength(4)
		expectContiguous(sent)
	})

	it('keeps order when a split batch also straddles the sequence wrap', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })
		await producer.send(TOPIC, { value: Buffer.from('init'), partition: 0 })
		;(producer as unknown as { sequences: Map<string, number> }).sequences.set(`${TOPIC}:0`, 0x7fffffff - 99)
		sent.length = 0

		const results = await producer.send(TOPIC, messages(500, 5000))

		expect(sent.map(b => [b.baseSequence, b.recordCount])).toEqual([
			[0x7fffffff - 99, 100],
			[0, 188],
			[188, 188],
			[376, 24],
		])
		expect(sent.every(b => b.bytes <= LIMIT)).toBe(true)
		const offsets = results.map(r => r.offset)
		expect(offsets).toEqual([...offsets].sort((a, b) => (a < b ? -1 : 1)))
		expect(await pendingCount(producer)).toBe(0)
	})

	it('fails a multi-record batch the broker rejects as too large without retrying or splitting it', async () => {
		// No broker-limit learning in this design: a topic limit below 1 MiB fails like before.
		const sent = fakeLeader(mockBroker, { brokerLimit: 1500 })
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true, retries: 5, retryBackoffMs: 1 })

		await expect(producer.send(TOPIC, messages(2, 1000))).rejects.toThrow(/MessageTooLarge/)

		expect(sent).toEqual([expect.objectContaining({ recordCount: 2, baseSequence: 0, accepted: false })])
		await producer.send(TOPIC, { value: Buffer.from('small'), partition: 0 })
		expect(sent[1]).toEqual(expect.objectContaining({ recordCount: 1, baseSequence: 0, accepted: true }))
	})

	it('sends a single record over the limit unchanged and aborts when the broker rejects it', async () => {
		const sent = fakeLeader(mockBroker, { brokerLimit: LIMIT })
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split-oversized' })
		const head = sized(600_000)
		const tail = sized(600_000)

		await expect(
			producer.transaction(
				async tx =>
					void (await tx.send(TOPIC, [
						...head,
						{ value: Buffer.alloc(1_200_000, 'x'), partition: 0 },
						...tail,
					]))
			)
		).rejects.toThrow(TransactionAbortedError)

		// The head piece is sent, the lone record is rejected by the broker, and the tail piece
		// queued behind it is never sent.
		expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([
			[head.length, true],
			[1, false],
		])
		expect(sent[1]!.bytes).toBeGreaterThan(LIMIT)
		expect(mockBroker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
		const endTxnAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
		expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < endTxnAt)).toBe(true)
		expect(await pendingCount(producer)).toBe(0)
	})

	it('does not send pieces queued behind a rejected piece while the transaction callback is still running', async () => {
		const sent = fakeLeader(mockBroker, { brokerLimit: LIMIT })
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split-poisoned' })
		const head = sized(600_000)
		const tail = [...sized(900_000), ...sized(900_000)]

		await expect(
			producer.transaction(async tx => {
				void tx
					.send(TOPIC, [...head, { value: Buffer.alloc(1_200_000, 'x'), partition: 0 }, ...tail])
					.catch(() => {})
				await new Promise(resolve => setTimeout(resolve, 50))
			})
		).rejects.toThrow(TransactionAbortedError)

		expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([
			[head.length, true],
			[1, false],
		])
		expect(mockBroker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
	})

	it('does not send pieces queued behind a piece that fails to encode', async () => {
		const original = compressionCodecs.get(CompressionType.Zstd)
		let compressions = 0
		compressionCodecs.register(CompressionType.Zstd, {
			compress: async data => {
				// The whole share, then the first piece, then a failure on the second piece.
				if (++compressions === 3) throw new Error('codec failure')
				return data
			},
			decompress: async data => data,
		})
		try {
			const sent = fakeLeader(mockBroker)
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				transactionalId: 'tx-split-encode-failure',
				compression: 'zstd',
			})

			await expect(
				producer.transaction(async tx => void (await tx.send(TOPIC, sized(THREE_MIB))))
			).rejects.toThrow(TransactionAbortedError)
			await new Promise(resolve => setTimeout(resolve, 20))

			expect(sent).toHaveLength(1)
			expect(compressions).toBe(3)
			expect(await pendingCount(producer)).toBe(0)
		} finally {
			if (original) compressionCodecs.register(CompressionType.Zstd, original)
		}
	})

	it('keeps a later send to the same partition behind every piece of an earlier split send', async () => {
		fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })
		await producer.send(TOPIC, { value: Buffer.from('warm'), partition: 0 })

		const [first, second] = await Promise.all([
			producer.send(TOPIC, sized(THREE_MIB)),
			producer.send(TOPIC, messages(3, 10)),
		])

		const lastOfFirst = first[first.length - 1]!.offset
		expect(second.every(r => r.offset > lastOfFirst)).toBe(true)
	})

	it('commits every piece of a split send in one transaction', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split' })
		const batch = sized(THREE_MIB)

		await producer.transaction(async tx => void (await tx.send(TOPIC, batch)))

		expect(sent.length).toBe(4)
		expect(sent.reduce((s, b) => s + b.recordCount, 0)).toBe(batch.length)
		expectContiguous(sent)
		expect(mockBroker.addPartitionsToTxn).toHaveBeenCalledTimes(1)
		expect(mockBroker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: true }))
		const endTxnAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
		expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < endTxnAt)).toBe(true)
	})

	it('sends no queued piece after the transaction aborts while pieces are queued', async () => {
		const sent = fakeLeader(mockBroker, { delayMs: 5 })
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split-user-abort' })

		await expect(
			producer.transaction(async tx => {
				void tx.send(TOPIC, sized(THREE_MIB)).catch(() => {})
				while (mockBroker.produce.mock.calls.length === 0) {
					await new Promise(resolve => setTimeout(resolve, 1))
				}
				throw new Error('user abort')
			})
		).rejects.toThrow(TransactionAbortedError)
		await new Promise(resolve => setTimeout(resolve, 20))

		expect(sent).toHaveLength(1)
		const endTxnAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
		expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < endTxnAt)).toBe(true)

		sent.length = 0
		await producer.transaction(
			async tx => void (await tx.send(TOPIC, { value: Buffer.from('next'), partition: 0 }))
		)
		expect(sent).toEqual([expect.objectContaining({ recordCount: 1, accepted: true })])
		expect(await pendingCount(producer)).toBe(0)
	})

	it('produces no queued piece after EndTxn(abort) when a piece is rejected', async () => {
		// A topic limit below 1 MiB: the first piece is rejected while the others are still queued.
		const sent = fakeLeader(mockBroker, { brokerLimit: 500_000, delayMs: 5 })
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split-abort' })

		await expect(producer.transaction(async tx => void (await tx.send(TOPIC, sized(THREE_MIB))))).rejects.toThrow(
			TransactionAbortedError
		)
		expect(sent).toHaveLength(1)
		const abortAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
		expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < abortAt)).toBe(true)

		sent.length = 0
		await producer.transaction(
			async tx => void (await tx.send(TOPIC, { value: Buffer.from('next'), partition: 0 }))
		)
		expect(sent).toEqual([expect.objectContaining({ recordCount: 1, accepted: true })])
		expect(await pendingCount(producer)).toBe(0)
	})

	it('sends no queued piece in either epoch when the producer is fenced', async () => {
		const sent = fakeLeader(mockBroker, {
			failBatch: index => (index === 0 ? ErrorCode.ProducerFenced : undefined),
		})
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })

		await expect(producer.send(TOPIC, sized(THREE_MIB))).rejects.toThrow(ProducerFencedError)
		await new Promise(resolve => setTimeout(resolve, 20))
		expect(sent).toHaveLength(1)

		await producer.send(TOPIC, { value: Buffer.from('after'), partition: 0 })
		expect(sent[1]).toEqual(expect.objectContaining({ producerEpoch: 1, baseSequence: 0, recordCount: 1 }))
		expect(await pendingCount(producer)).toBe(0)
	})

	it('drops a split batch whose producer epoch was fenced while it was being compressed', async () => {
		const restore = slowIdentityZstd(data => (data.length > 100_000 ? 50 : 0))
		try {
			useCluster(2)
			const sent = fakeLeader(mockBroker, {
				failBatch: (_, batch) => (batch.partition === 0 ? ErrorCode.ProducerFenced : undefined),
			})
			const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true, compression: 'zstd' })
			await producer.send(TOPIC, { value: Buffer.from('warm'), partition: 1 })
			sent.length = 0

			const big = producer.send(TOPIC, sized(THREE_MIB, 1))
			await new Promise(resolve => setTimeout(resolve, 10))
			await expect(producer.send(TOPIC, { value: Buffer.from('x'), partition: 0 })).rejects.toThrow(
				ProducerFencedError
			)
			await expect(big).rejects.toThrow(ProducerFencedError)
			await new Promise(resolve => setTimeout(resolve, 120))

			expect(sent.filter(b => b.partition === 1)).toEqual([])
			expect(await pendingCount(producer)).toBe(0)
		} finally {
			restore()
		}
	})

	it('waits for a partition still being encoded before EndTxn(abort)', async () => {
		const encodeDelayMs = 40
		const restore = slowIdentityZstd(() => encodeDelayMs)
		try {
			useCluster(2)
			fakeLeader(mockBroker)
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				transactionalId: 'tx-split-encoding',
				compression: 'zstd',
			})
			const both = [...messages(500, 5000, 0), ...messages(500, 5000, 1)]

			// Partition 0 is encoded twice by ~2x the delay and partition 1 by ~4x: abort in between,
			// so partition 0's queued pieces are cleared while partition 1 is still being encoded.
			await expect(
				producer.transaction(async tx => {
					void tx.send(TOPIC, both).catch(() => {})
					await new Promise(resolve => setTimeout(resolve, encodeDelayMs * 2.5))
					throw new Error('user abort')
				})
			).rejects.toThrow(TransactionAbortedError)
			await new Promise(resolve => setTimeout(resolve, encodeDelayMs * 6))

			const endTxnAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
			expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < endTxnAt)).toBe(true)
			expect(await pendingCount(producer)).toBe(0)
		} finally {
			restore()
		}
	})

	it("waits for another broker's in-flight produce before EndTxn(abort) when one partition fails", async () => {
		useCluster(2)
		const slow = createMockBroker(2)
		mockCluster.getLeaderForPartition.mockImplementation(async (_topic, partition) =>
			partition === 0 ? mockBroker : slow
		)
		const events: string[] = []
		mockBroker.produce.mockResolvedValue(
			buildProduceResponse([
				{ name: TOPIC, partitions: [{ partitionIndex: 0, errorCode: ErrorCode.InvalidRecord }] },
			])
		)
		slow.produce.mockImplementation(async () => {
			await new Promise(resolve => setTimeout(resolve, 50))
			events.push('produce acked')
			return buildProduceResponse([
				{ name: TOPIC, partitions: [{ partitionIndex: 1, errorCode: ErrorCode.None, baseOffset: 0n }] },
			])
		})
		mockBroker.endTxn.mockImplementation(async () => {
			events.push('endTxn')
			return { throttleTimeMs: 0, errorCode: ErrorCode.None }
		})
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-two-brokers' })

		await expect(
			producer.transaction(
				async tx => void (await tx.send(TOPIC, [...messages(1, 10, 0), ...messages(1, 10, 1)]))
			)
		).rejects.toThrow(TransactionAbortedError)

		expect(events).toEqual(['produce acked', 'endTxn'])
	})
})
