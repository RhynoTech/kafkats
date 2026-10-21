import { randomBytes } from 'node:crypto'
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest'

import { Producer } from '@/producer/producer.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { ProducerFencedError, RecordTooLargeError, TransactionAbortedError } from '@/client/errors.js'
import type { ProduceRequest } from '@/protocol/messages/requests/produce.js'
import type { ProducerTraceEvent } from '@/producer/types.js'
import type { CompressionRatioEstimator } from '@/producer/compression-ratio-estimator.js'
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
	/**
	 * Batches above this size are rejected with MESSAGE_TOO_LARGE, like a topic's max.message.bytes.
	 * Read on every batch, so a test can change it like an operator altering the topic.
	 */
	brokerLimit?: number
	delayMs?: number
	/** Error to answer the n-th partition batch with (0-based, counting every batch sent). */
	failBatch?: (index: number, batch: SentBatch) => ErrorCode | undefined
}

/** Broker stand-in: assigns offsets per partition and answers each partition batch like a leader would. */
function fakeLeader(broker: MockBroker, opts: FakeLeaderOptions = {}): SentBatch[] {
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
					accepted: records.length <= (opts.brokerLimit ?? Number.POSITIVE_INFINITY),
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

/** Records of 3000 bytes that snappy compresses to roughly 40% (1000 random bytes, then a repeated run). */
function compressible(totalBytes: number, partition = 0) {
	return Array.from({ length: Math.ceil(totalBytes / 3000) }, (_, i) => ({
		key: `key-${i}`,
		value: Buffer.concat([randomBytes(1000), Buffer.alloc(2000, `run-${i % 7}`)]),
		headers: { seq: String(i) },
		partition,
	}))
}

const SHARED_RUN = randomBytes(2000)

/** Records of 3000 bytes ending in the same 2000 random bytes: alone one barely compresses, many do. */
function sharingARun(totalBytes: number, partition = 0) {
	return Array.from({ length: Math.ceil(totalBytes / 3000) }, () => ({
		value: Buffer.concat([randomBytes(1000), SHARED_RUN]),
		partition,
	}))
}

function incompressible(totalBytes: number, partition = 0) {
	return Array.from({ length: Math.ceil(totalBytes / 3000) }, () => ({ value: randomBytes(3000), partition }))
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

function traced(stage: ProducerTraceEvent['stage'], into: ProducerTraceEvent[]) {
	return (e: ProducerTraceEvent) => void (e.stage === stage && into.push(e))
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
		const encodes: ProducerTraceEvent[] = []
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			trace: traced('encode_batch', encodes),
		})
		const batch = sized(THREE_MIB)

		const results = await producer.send(TOPIC, batch)

		expect(sent.length).toBe(4)
		expect(sent.every(b => b.bytes <= LIMIT && b.accepted)).toBe(true)
		expect(sent.reduce((s, b) => s + b.recordCount, 0)).toBe(batch.length)
		expect(sent[0]!.baseSequence).toBe(0)
		expectContiguous(sent)
		// Uncompressed batches are sized exactly before encoding, so nothing is encoded twice.
		expect(encodes.map(e => e.bytes)).toEqual(sent.map(b => b.bytes))
		expect(encodes.every(e => e.estimatedBytes! >= e.bytes!)).toBe(true)
		expect(results.map(r => r.offset)).toEqual(Array.from({ length: batch.length }, (_, i) => BigInt(i)))
		expect(await pendingCount(producer)).toBe(0)
	})

	it('sizes records with keys and headers as an upper bound of their encoded size', async () => {
		const sent = fakeLeader(mockBroker)
		const encodes: ProducerTraceEvent[] = []
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			trace: traced('encode_batch', encodes),
		})
		const batch = Array.from({ length: 30_000 }, (_, i) => ({
			key: i % 3 === 0 ? null : `k${i}`,
			value: i % 5 === 0 ? null : Buffer.alloc(i % 200),
			headers: i % 2 === 0 ? { 'trace-id': `t${i}`, é: Buffer.alloc(3) } : undefined,
			partition: 0,
			timestamp: new Date(1_700_000_000_000 + i * 997),
		}))

		await producer.send(TOPIC, batch)

		expect(sent.length).toBeGreaterThan(1)
		expect(encodes).toHaveLength(sent.length)
		expect(encodes.every(e => e.estimatedBytes! >= e.bytes! && e.bytes! <= LIMIT)).toBe(true)
		expectContiguous(sent)
	})

	it('cuts by record bytes, so one large record does not force repeated splits', async () => {
		const sent = fakeLeader(mockBroker)
		const encodes: ProducerTraceEvent[] = []
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			trace: traced('encode_batch', encodes),
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
		expect(encodes).toHaveLength(3)
		expectContiguous(sent)
	})

	it('keeps order when a split batch also straddles the sequence wrap', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })
		await producer.send(TOPIC, { value: Buffer.from('init'), partition: 0 })
		;(producer as unknown as { sequences: Map<string, number> }).sequences.set(`${TOPIC}:0`, 0x7fffffff - 99)
		sent.length = 0

		const results = await producer.send(TOPIC, messages(500, 5000))

		expect(sent[0]).toEqual(expect.objectContaining({ baseSequence: 0x7fffffff - 99, recordCount: 100 }))
		expect(sent[1]!.baseSequence).toBe(0)
		expectContiguous(sent.slice(1))
		expect(sent.reduce((s, b) => s + b.recordCount, 0)).toBe(500)
		expect(sent.every(b => b.bytes <= LIMIT)).toBe(true)
		const offsets = results.map(r => r.offset)
		expect(offsets).toEqual([...offsets].sort((a, b) => (a < b ? -1 : 1)))
		expect(await pendingCount(producer)).toBe(0)
	})

	describe('compression ratio estimate', () => {
		it('cuts a compressible send conservatively before the estimate has learned the ratio', async () => {
			const sent = fakeLeader(mockBroker)
			const splits: ProducerTraceEvent[] = []
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				idempotent: true,
				compression: 'snappy',
				trace: traced('batch_split', splits),
			})

			await producer.send(TOPIC, compressible(1_500_000))

			// A cold estimate assumes no compression, so 1.5 MB is cut although it encodes to ~0.6 MB.
			expect(splits.map(s => s.reason)).toEqual(['estimate'])
			expect(sent).toHaveLength(2)
			expect(sent.reduce((s, b) => s + b.bytes, 0)).toBeLessThan(LIMIT)
		})

		it('learns the ratio from the first batch large enough to need cutting', async () => {
			const sent = fakeLeader(mockBroker)
			const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true, compression: 'snappy' })
			await producer.send(TOPIC, compressible(1_500_000))
			sent.length = 0

			await producer.send(TOPIC, compressible(1_500_000))

			expect(sent).toHaveLength(1)
		})

		it('does not learn from small batches, which compress worse than the large ones it sizes', async () => {
			const sent = fakeLeader(mockBroker)
			const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true, compression: 'snappy' })
			await producer.send(TOPIC, sharingARun(200_000))
			for (let i = 0; i < 20; i++) {
				await producer.send(TOPIC, sharingARun(3000)[0]!)
			}
			sent.length = 0

			await producer.send(TOPIC, sharingARun(1_500_000))

			expect(sent).toHaveLength(1)
		})

		it('sends what fits by the learned ratio as one batch, and cuts larger sends with one encode per piece', async () => {
			const sent = fakeLeader(mockBroker)
			const encodes: ProducerTraceEvent[] = []
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				idempotent: true,
				compression: 'snappy',
				trace: traced('encode_batch', encodes),
			})
			await producer.send(TOPIC, compressible(200_000))
			sent.length = 0
			encodes.length = 0

			await producer.send(TOPIC, compressible(1_500_000))
			expect(sent).toHaveLength(1)
			expect(encodes).toHaveLength(1)

			await producer.send(TOPIC, compressible(6_000_000))
			expect(sent.length).toBeGreaterThanOrEqual(3)
			expect(sent.every(b => b.bytes <= LIMIT && b.accepted)).toBe(true)
			expect(encodes).toHaveLength(sent.length)
			expectContiguous(sent)
		})

		it('re-cuts a batch that encodes larger than estimated and raises the estimate', async () => {
			const sent = fakeLeader(mockBroker)
			const encodes: ProducerTraceEvent[] = []
			const splits: ProducerTraceEvent[] = []
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				idempotent: true,
				compression: 'snappy',
				trace: e =>
					void (e.stage === 'encode_batch' ? encodes.push(e) : e.stage === 'batch_split' && splits.push(e)),
			})
			await producer.send(TOPIC, compressible(200_000))
			sent.length = 0
			encodes.length = 0

			// Random bytes do not compress: the learned ~0.4 ratio makes 1.5 MB look like ~0.6 MB.
			await producer.send(TOPIC, incompressible(1_500_000))
			expect(splits.map(s => s.reason)).toEqual(['encoded_size'])
			expect(encodes).toHaveLength(sent.length + 1)
			expect(sent.every(b => b.bytes <= LIMIT && b.accepted)).toBe(true)
			expectContiguous(sent)

			// The estimate jumped to the observed ratio, so the next such send is cut before encoding.
			splits.length = 0
			encodes.length = 0
			sent.length = 0
			await producer.send(TOPIC, incompressible(1_500_000))
			expect(splits.map(s => s.reason)).toEqual(['estimate'])
			expect(encodes).toHaveLength(sent.length)
		})

		it('shares what one transaction lane learned about a topic limit with the other lanes', async () => {
			const sent = fakeLeader(mockBroker, { brokerLimit: 500_000 })
			const producer = new Producer(mockCluster, {
				lingerMs: 0,
				transactionalId: 'tx-split-lanes',
				transactionConcurrency: 2,
			})
			await producer.transaction(async tx => void (await tx.send(TOPIC, sized(1_200_000))))
			const firstRejected = Math.min(...sent.filter(b => !b.accepted).map(b => b.bytes))
			sent.length = 0

			// Two concurrent transactions use both lanes: a lane that had not learned the limit would
			// start again from 1 MiB batches.
			await Promise.all([
				producer.transaction(async tx => void (await tx.send(TOPIC, sized(1_200_000)))),
				producer.transaction(async tx => void (await tx.send(TOPIC, sized(1_200_000)))),
			])
			expect(sent.length).toBeGreaterThan(0)
			expect(sent.every(b => b.bytes < firstRejected)).toBe(true)
		})
	})

	it('cuts into even pieces instead of full pieces and a small leftover', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })

		await producer.send(TOPIC, sized(2.2 * LIMIT))

		expect(sent).toHaveLength(3)
		const sizes = sent.map(b => b.bytes)
		expect(Math.max(...sizes) / Math.min(...sizes)).toBeLessThan(1.05)
	})

	it('does not cut queued pieces again when the estimate rises before they are sent', async () => {
		const sent = fakeLeader(mockBroker)
		const splits: ProducerTraceEvent[] = []
		let estimator: CompressionRatioEstimator | undefined
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			compression: 'snappy',
			trace: e => {
				if (e.stage !== 'batch_split') return
				splits.push(e)
				// A worse batch elsewhere on the topic, observed while these pieces wait.
				estimator!.observe(TOPIC, CompressionType.Snappy, 1000, 61 + 2000)
			},
		})
		estimator = (producer as unknown as { compressionRatios: CompressionRatioEstimator }).compressionRatios

		await producer.send(TOPIC, compressible(3_000_000))

		expect(splits).toHaveLength(1)
		expect(sent).toHaveLength(splits[0]!.pieces!)
		expect(sent.every(b => b.accepted)).toBe(true)
	})

	it('narrows down a topic limit without being rejected again just under the last rejection', async () => {
		const sent = fakeLeader(mockBroker, { brokerLimit: 300_000 })
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })

		// Sends of 1.5 to 3 MB, so pieces probe many sizes between the topic limit and 1 MiB.
		const rejections: number[] = []
		for (let i = 0; i < 40; i++) {
			sent.length = 0
			await producer.send(TOPIC, messages(Math.ceil((1_500_000 + ((i * 377_777) % 1_500_000)) / 2000), 2000))
			rejections.push(sent.filter(b => !b.accepted).length)
		}

		expect(rejections.reduce((a, b) => a + b, 0)).toBeLessThanOrEqual(3)
		expect(rejections.slice(10).every(n => n === 0)).toBe(true)
		expect(Math.max(...sent.map(b => b.bytes))).toBeGreaterThan(0.9 * 300_000)
	})

	it('re-cuts a batch over a narrowed topic limit by its own ratio, not the estimate the rejection reset', async () => {
		const sent = fakeLeader(mockBroker, { brokerLimit: 200_000 })
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true, compression: 'snappy' })

		await producer.send(TOPIC, compressible(3_000_000))

		// ~1.2 MB compressed needs at least 7 batches under 200 KB; cutting by the reset estimate
		// of 1.0 would make the batches a ~40%-compressible payload produces about half as large.
		expect(sent.filter(b => !b.accepted)).toHaveLength(1)
		expect(sent.filter(b => b.accepted).length).toBeLessThanOrEqual(9)
		expectContiguous(sent.filter(b => b.accepted))
	})

	it('splits a 2-record batch the broker rejects as too large without using the retry budget', async () => {
		const sent = fakeLeader(mockBroker, { brokerLimit: 1500 })
		const splits: ProducerTraceEvent[] = []
		// A retry would sleep retryBackoffMs; the split must not wait at all.
		const producer = new Producer(mockCluster, {
			lingerMs: 0,
			idempotent: true,
			retries: 1,
			retryBackoffMs: 60_000,
			trace: traced('batch_split', splits),
		})

		const startedAt = Date.now()
		const results = await producer.send(TOPIC, messages(2, 1000))

		expect(Date.now() - startedAt).toBeLessThan(1000)
		expect(sent.map(b => [b.baseSequence, b.recordCount, b.accepted])).toEqual([
			[0, 2, false],
			[0, 1, true],
			[1, 1, true],
		])
		expect(results.map(r => r.offset)).toEqual([0n, 1n])
		expect(splits.map(s => s.reason)).toEqual(['broker_rejected'])

		// The rejected size is remembered as the topic's limit: the next send is cut before sending.
		sent.length = 0
		await producer.send(TOPIC, messages(2, 1000))
		expect(sent.map(b => [b.baseSequence, b.recordCount, b.accepted])).toEqual([
			[2, 1, true],
			[3, 1, true],
		])
		expect(await pendingCount(producer)).toBe(0)
	})

	it('commits a transaction whose pieces the broker rejects as too large, after splitting them', async () => {
		const sent = fakeLeader(mockBroker, { brokerLimit: 300_000 })
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split-low-limit' })
		const batch = sized(THREE_MIB)

		await producer.transaction(async tx => void (await tx.send(TOPIC, batch)))

		const accepted = sent.filter(b => b.accepted)
		expect(accepted.reduce((s, b) => s + b.recordCount, 0)).toBe(batch.length)
		expectContiguous(accepted)
		expect(mockBroker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: true }))
		const endTxnAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
		expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < endTxnAt)).toBe(true)
	})

	it('sends a single record above a learned topic limit, and forgets the limit once a larger batch fits', async () => {
		const leader: FakeLeaderOptions = { brokerLimit: 100_000 }
		const sent = fakeLeader(mockBroker, leader)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })
		await producer.send(TOPIC, messages(2, 60_000))
		expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([
			[2, false],
			[1, true],
			[1, true],
		])

		// Raising max.message.bytes is the usual answer to a record too large for the topic.
		leader.brokerLimit = LIMIT
		sent.length = 0
		await producer.send(TOPIC, { value: Buffer.alloc(150_000, 'x'), partition: 0 })
		expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([[1, true]])

		sent.length = 0
		await producer.send(TOPIC, messages(10, 60_000))
		expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([[10, true]])
	})

	it('learns a topic limit again five minutes after its last rejection', async () => {
		vi.useFakeTimers({ toFake: ['Date'] })
		try {
			const leader: FakeLeaderOptions = { brokerLimit: 100_000 }
			const sent = fakeLeader(mockBroker, leader)
			const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })
			await producer.send(TOPIC, messages(2, 60_000))

			leader.brokerLimit = LIMIT
			sent.length = 0
			await producer.send(TOPIC, messages(10, 60_000))
			expect(sent.length).toBeGreaterThan(1)

			vi.setSystemTime(Date.now() + 5 * 60_000 + 1)
			sent.length = 0
			await producer.send(TOPIC, messages(10, 60_000))
			expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([[10, true]])
		} finally {
			vi.useRealTimers()
		}
	})

	it('fails a single record larger than the limit with RecordTooLargeError without sending it', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true })

		await expect(producer.send(TOPIC, { value: Buffer.alloc(1_200_000, 'x'), partition: 0 })).rejects.toThrow(
			RecordTooLargeError
		)
		expect(sent).toHaveLength(0)

		await producer.send(TOPIC, { value: Buffer.from('small'), partition: 0 })
		expect(sent).toEqual([expect.objectContaining({ baseSequence: 0, recordCount: 1 })])
	})

	it('aborts the transaction on a single record larger than the limit and sends nothing queued behind it', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, transactionalId: 'tx-split-oversized' })
		const head = sized(600_000)
		const tail = [...sized(900_000), ...sized(900_000)]

		const failure = await producer
			.transaction(
				async tx =>
					void (await tx.send(TOPIC, [
						...head,
						{ value: Buffer.alloc(1_200_000, 'x'), partition: 0 },
						...tail,
					]))
			)
			.catch((error: unknown) => error)

		expect(failure).toBeInstanceOf(TransactionAbortedError)
		expect((failure as TransactionAbortedError).cause).toBeInstanceOf(RecordTooLargeError)
		expect(sent.map(b => [b.recordCount, b.accepted])).toEqual([[head.length, true]])
		expect(mockBroker.endTxn).toHaveBeenCalledWith(expect.objectContaining({ committed: false }))
		const endTxnAt = mockBroker.endTxn.mock.invocationCallOrder[0]!
		expect(mockBroker.produce.mock.invocationCallOrder.every(at => at < endTxnAt)).toBe(true)
		expect(await pendingCount(producer)).toBe(0)
	})

	it('does not send pieces queued behind a piece that fails to encode', async () => {
		const original = compressionCodecs.get(CompressionType.Zstd)
		let compressions = 0
		compressionCodecs.register(CompressionType.Zstd, {
			compress: async data => {
				// The first piece, then a failure on the second piece.
				if (++compressions === 2) throw new Error('codec failure')
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
			expect(compressions).toBe(2)
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

	it('drops a batch whose producer epoch was fenced while it was being compressed', async () => {
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

			// Each round encodes one piece per partition for 2x the delay: abort mid-way through the
			// second round, while queued pieces are cleared and both partitions are still encoding.
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

	it('rejects a maxRecordBatchBytes that cannot hold a record', () => {
		expect(() => new Producer(mockCluster, { maxRecordBatchBytes: 61 })).toThrow(/maxRecordBatchBytes/)
		expect(() => new Producer(mockCluster, { maxRecordBatchBytes: 1.5 })).toThrow(/maxRecordBatchBytes/)
	})

	it('sends batches up to a raised maxRecordBatchBytes unsplit', async () => {
		const sent = fakeLeader(mockBroker)
		const producer = new Producer(mockCluster, { lingerMs: 0, idempotent: true, maxRecordBatchBytes: 8 * LIMIT })

		await producer.send(TOPIC, [...sized(THREE_MIB), { value: Buffer.alloc(2_000_000), partition: 0 }])

		expect(sent).toHaveLength(1)
		expect(sent[0]!.bytes).toBeGreaterThan(5_000_000)
	})
})
