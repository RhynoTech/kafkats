/**
 * Fetch manager for consumer
 *
 * Handles:
 * - Fetching records from assigned partitions
 * - Managing partition concurrency
 * - Decoding record batches as they are delivered
 */

import type { Cluster } from '@/client/cluster.js'
import type { Broker } from '@/client/broker.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { KafkaProtocolError } from '@/client/errors.js'
import type { DecodedRecord } from '@/protocol/records/index.js'
import type { TopicPartition, TopicPartitionOffset, AutoOffsetReset, IsolationLevel, PartitionBatch } from './types.js'
import type { OffsetManager } from './offset-manager.js'
import { noopLogger, type Logger } from '@/logger.js'
import { tpKey, formatPartitions } from '@/utils/topic-partition.js'
import { sleep } from '@/utils/sleep.js'
import type { PriorityStrategy, ScheduleDecision, SchedulerPartition, SchedulerState } from './priority.js'
import { CompletedFetch, readFetchedRecords, recordBytes } from './completed-fetch.js'

function corruptDataError(topic: string, partition: number, offset: bigint, cause: unknown): Error {
	const message = cause instanceof Error ? cause.message : String(cause)
	return new Error(
		`Failed to decode record batch (corrupt data) for ${topic}-${partition} at offset ${offset}: ${message}`
	)
}

/** A partition whose buffered data failed to decode during a drain. */
interface DrainFailure {
	fetch: CompletedFetch
	error: unknown
}

/** Per-partition aggregate of buffered-but-undelivered bytes, for priority scheduling. */
interface BufferedPartitionStats {
	topic: string
	partition: number
	bytes: number
}

/** Anything with set-like `has(topic:partition)`; a Set or a Map keyed by partition works. */
type PartitionKeys = Pick<ReadonlySet<string>, 'has'>

/**
 * Buffer for completed fetches with memory bounding and wait/signal. It counts the bytes it holds:
 * record batches as fetched, compressed ones at their compressed size, plus any batch left
 * decompressed part-way through and the records decoded ahead of a drain.
 */
class FetchBuffer {
	private queue: CompletedFetch[] = []
	private reservedBytes = 0
	private waiters: Array<(force?: boolean) => void> = []
	private capacityWaiter: (() => void) | null = null
	private draining = false
	private stageRun = 0

	constructor(
		readonly maxBytes: number,
		private readonly onAdd: () => void = () => {}
	) {}

	/** Summed on demand: decoding ahead grows a fetch's bytes outside any buffer call. */
	get sizeBytes(): number {
		let bytes = 0
		for (const fetch of this.queue) bytes += fetch.heldBytes
		return bytes
	}

	/**
	 * Add a completed fetch to the buffer and signal any waiters
	 */
	add(fetch: CompletedFetch): void {
		this.queue.push(fetch)
		this.signalWaiters()
		this.onAdd()
	}

	/**
	 * Drain one batch per partition and assignment epoch, coalescing that partition's buffered
	 * fetches up to `maxBytesPerPartition` estimated bytes and decoding the records it takes that
	 * stage() has not already decoded. Each batch takes at least one record so an oversized record
	 * still makes progress. Planned partitions go first, in plan order and capped per entry, then the
	 * remaining partitions FIFO. A poll stops starting partitions once it has used
	 * `maxBytesPerPartition` of budget; served partitions rotate behind the others for the next poll.
	 *
	 * A partition discarded (paused, seeked, revoked) while the drain awaits decompression is left
	 * out of the result. A partition whose data fails to decode returns the records before the bad
	 * one and a failure.
	 */
	async drain(
		maxBytesPerPartition: number,
		excludedPartitions?: PartitionKeys,
		plan: ScheduleDecision['drain'] = []
	): Promise<{ batches: PartitionBatch[]; failures: DrainFailure[] }> {
		const byPartition = this.byPartition(excludedPartitions)
		const drained = new Map<string, PartitionBatch>()
		const failures: DrainFailure[] = []

		const take = async (partitionKey: string, maxBytes: number): Promise<number> => {
			let takenBytes = 0
			for (const fetch of byPartition.get(partitionKey)!) {
				const key = `${partitionKey}\0${fetch.assignmentEpoch}`
				let batch = drained.get(key)
				let full = false
				try {
					for (;;) {
						const peeked = fetch.peek()
						const record = peeked instanceof Promise ? await peeked : peeked
						if (record === null) break
						const size = recordBytes(record)
						if (takenBytes > 0 && takenBytes + size > maxBytes) {
							full = true
							break
						}
						if (!batch) {
							batch = {
								topic: fetch.topic,
								partition: fetch.partition,
								records: [],
								assignmentEpoch: fetch.assignmentEpoch,
							}
							drained.set(key, batch)
						}
						batch.records.push(record)
						fetch.consume()
						takenBytes += size
					}
				} catch (error) {
					failures.push({ fetch, error })
					full = true
				}
				if (full) break
			}
			return takenBytes
		}

		this.draining = true
		this.stageRun++
		let served: Set<string>
		try {
			served = await this.walk(byPartition, maxBytesPerPartition, plan, take)
		} finally {
			this.draining = false
		}

		const kept = (partitionKey: string) => !byPartition.get(partitionKey)!.some(fetch => fetch.dropped)
		const batches: PartitionBatch[] = []
		for (const batch of drained.values()) {
			if (kept(tpKey(batch.topic, batch.partition))) batches.push(batch)
		}
		for (const partitionKey of served) {
			if (kept(partitionKey)) for (const fetch of byPartition.get(partitionKey)!) fetch.commit()
		}

		const untouched: CompletedFetch[] = []
		const rotated: CompletedFetch[] = []
		for (const fetch of this.queue) {
			if (fetch.isEmpty) continue
			const target = served.has(tpKey(fetch.topic, fetch.partition)) ? rotated : untouched
			target.push(fetch)
		}
		this.queue = [...untouched, ...rotated]
		this.signalCapacityWaiter()
		return { batches, failures: failures.filter(failure => !failure.fetch.dropped) }
	}

	/**
	 * Decode ahead the records the next drain will look at, walking partitions as a drain with no
	 * partition excluded would, so decompression and parsing overlap the handlers instead of adding
	 * to the next poll. The records stay buffered, and counted, until a drain takes them. A drain, a
	 * newer stage, or a fetch leaving the buffer stops the walk at its next await. A record that fails
	 * to decode stops the walk for its partition; the drain raises it after the records before it.
	 */
	async stage(maxBytesPerPartition: number, plan: ScheduleDecision['drain'] = []): Promise<void> {
		if (this.draining) return
		const run = ++this.stageRun
		const byPartition = this.byPartition()
		const decodeAhead = async (partitionKey: string, maxBytes: number): Promise<number | null> => {
			let takenBytes = 0
			for (const fetch of byPartition.get(partitionKey)!) {
				for (let index = 0; ;) {
					let record: DecodedRecord | null | Promise<void>
					try {
						record = fetch.ahead(index)
						if (record instanceof Promise) await record
					} catch {
						return run === this.stageRun ? takenBytes : null
					}
					if (record instanceof Promise) {
						if (run !== this.stageRun || fetch.dropped) return null
						continue
					}
					if (record === null) break
					const size = recordBytes(record)
					if (takenBytes > 0 && takenBytes + size > maxBytes) return takenBytes
					takenBytes += size
					index++
				}
			}
			return takenBytes
		}
		await this.walk(byPartition, maxBytesPerPartition, plan, decodeAhead)
	}

	/** Buffered fetches per partition, in queue order. */
	private byPartition(excludedPartitions?: PartitionKeys): Map<string, CompletedFetch[]> {
		const byPartition = new Map<string, CompletedFetch[]>()
		for (const fetch of this.queue) {
			const key = tpKey(fetch.topic, fetch.partition)
			if (excludedPartitions?.has(key)) continue
			const entries = byPartition.get(key)
			if (entries) entries.push(fetch)
			else byPartition.set(key, [fetch])
		}
		return byPartition
	}

	/**
	 * Visit partitions in drain order until a poll's budget is used, returning those visited. `take`
	 * returns the bytes it took from a partition, or null to end the walk.
	 */
	private async walk(
		byPartition: Map<string, CompletedFetch[]>,
		maxBytesPerPartition: number,
		plan: ScheduleDecision['drain'],
		take: (partitionKey: string, maxBytes: number) => Promise<number | null>
	): Promise<Set<string>> {
		const served = new Set<string>()
		// Bytes charged against the poll budget. A partition is charged at most its own allowance, so
		// an oversized first record cannot crowd out the partitions planned after it.
		let budgetUsed = 0
		const visit = async (partitionKey: string, maxBytes: number): Promise<boolean> => {
			served.add(partitionKey)
			const taken = await take(partitionKey, maxBytes)
			if (taken === null) return false
			budgetUsed += Math.min(taken, maxBytes)
			return true
		}

		for (const entry of plan) {
			if (budgetUsed >= maxBytesPerPartition) return served
			const partitionKey = tpKey(entry.topic, entry.partition)
			if (served.has(partitionKey) || !byPartition.has(partitionKey)) continue
			const maxBytes = Math.min(entry.maxBytes, maxBytesPerPartition)
			if (!(await visit(partitionKey, maxBytes > 0 ? maxBytes : 0))) return served
		}
		for (const partitionKey of byPartition.keys()) {
			if (budgetUsed >= maxBytesPerPartition) return served
			if (!served.has(partitionKey) && !(await visit(partitionKey, maxBytesPerPartition))) return served
		}
		return served
	}

	/** Aggregate buffered bytes per partition, in queue (first-seen) order. */
	snapshot(): BufferedPartitionStats[] {
		const stats = new Map<string, BufferedPartitionStats>()
		for (const fetch of this.queue) {
			const key = tpKey(fetch.topic, fetch.partition)
			const existing = stats.get(key)
			if (existing) existing.bytes += fetch.heldBytes
			else stats.set(key, { topic: fetch.topic, partition: fetch.partition, bytes: fetch.heldBytes })
		}
		return Array.from(stats.values())
	}

	/** Partitions holding a corrupt batch that no drain has raised yet. */
	corruptPartitions(): Set<string> {
		const keys = new Set<string>()
		for (const fetch of this.queue) if (fetch.corruption) keys.add(tpKey(fetch.topic, fetch.partition))
		return keys
	}

	hasDataFor(excludedPartitions?: PartitionKeys): boolean {
		return this.queue.some(fetch => !excludedPartitions?.has(tpKey(fetch.topic, fetch.partition)))
	}

	isFull(): boolean {
		return this.remainingCapacity() === 0
	}

	remainingCapacity(): number {
		return Math.max(0, this.maxBytes - this.sizeBytes - this.reservedBytes)
	}

	/** Reserve part of the memory budget for a Fetch response before dispatching it. */
	reserveCapacity(maxBytes: number): number {
		const reserved = Math.min(maxBytes, this.remainingCapacity())
		this.reservedBytes += reserved
		return reserved
	}

	releaseCapacity(bytes: number): void {
		this.reservedBytes = Math.max(0, this.reservedBytes - bytes)
		this.signalCapacityWaiter()
	}

	/** Wait until poll() or partition removal frees buffer capacity. */
	async waitForCapacity(signal?: AbortSignal): Promise<void> {
		if (!this.isFull() || signal?.aborted) {
			return
		}

		await new Promise<void>(resolve => {
			const resume = () => {
				this.capacityWaiter = null
				signal?.removeEventListener('abort', resume)
				resolve()
			}

			this.capacityWaiter = resume
			signal?.addEventListener('abort', resume, { once: true })
		})
	}

	/**
	 * Wait for data to be available in the buffer
	 * @returns true if data is available, false if timeout/aborted
	 */
	async waitForData(timeoutMs: number, signal?: AbortSignal, excludedPartitions?: PartitionKeys): Promise<boolean> {
		if (this.hasDataFor(excludedPartitions)) {
			return true
		}

		return new Promise<boolean>(resolve => {
			let timeoutId: ReturnType<typeof setTimeout> | null = null
			let onAbort: (() => void) | null = null

			const cleanup = () => {
				if (timeoutId) {
					clearTimeout(timeoutId)
					timeoutId = null
				}
				if (signal && onAbort) {
					signal.removeEventListener('abort', onAbort)
				}
				// Remove from waiters
				const idx = this.waiters.indexOf(waiterCallback)
				if (idx !== -1) {
					this.waiters.splice(idx, 1)
				}
			}

			const waiterCallback = (force = false) => {
				if (!force && !this.hasDataFor(excludedPartitions)) return
				cleanup()
				resolve(this.hasDataFor(excludedPartitions))
			}

			this.waiters.push(waiterCallback)

			// Set timeout
			timeoutId = setTimeout(() => {
				cleanup()
				resolve(false)
			}, timeoutMs)

			// Handle abort signal
			if (signal) {
				if (signal.aborted) {
					cleanup()
					resolve(false)
					return
				}
				onAbort = () => {
					cleanup()
					resolve(false)
				}
				signal.addEventListener('abort', onAbort)
			}
		})
	}

	/**
	 * Signal all waiters that data is available
	 */
	wakeWaiters(): void {
		this.signalWaiters(true)
	}

	private signalWaiters(force = false): void {
		const waiters = [...this.waiters]
		for (const waiter of waiters) {
			waiter(force)
		}
	}

	private signalCapacityWaiter(): void {
		if (!this.isFull()) this.capacityWaiter?.()
	}

	/**
	 * Clear all buffered data
	 */
	clear(): void {
		this.retain(() => false)
	}

	/**
	 * Remove buffered data for partitions not in the given set
	 */
	removePartitionsNotIn(validKeys: Set<string>): void {
		this.retain(fetch => validKeys.has(tpKey(fetch.topic, fetch.partition)))
	}

	/**
	 * Remove buffered data for specific partitions
	 */
	removePartitions(partitions: Array<{ topic: string; partition: number }>): void {
		if (partitions.length === 0) return
		const keysToRemove = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
		this.retain(fetch => !keysToRemove.has(tpKey(fetch.topic, fetch.partition)))
	}

	/**
	 * Remove buffered data for specific partitions, returning per partition that had buffered data
	 * the offset to re-fetch from to get its undelivered records back. Used by pause(): the buffered
	 * records were never delivered but the fetch offset already advanced past them, so the caller
	 * rewinds to re-fetch (and re-deliver) them on resume instead of dropping them.
	 */
	removePartitionsReturningEarliest(partitions: Array<{ topic: string; partition: number }>): Map<string, bigint> {
		const earliest = new Map<string, bigint>()
		if (partitions.length === 0) return earliest
		const keysToRemove = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
		this.retain(fetch => {
			const key = tpKey(fetch.topic, fetch.partition)
			if (!keysToRemove.has(key)) return true
			const current = earliest.get(key)
			if (current === undefined || fetch.position < current) earliest.set(key, fetch.position)
			return false
		})
		return earliest
	}

	private retain(keep: (fetch: CompletedFetch) => boolean): void {
		this.queue = this.queue.filter(fetch => {
			if (keep(fetch)) return true
			fetch.dropped = true
			return false
		})
		this.signalCapacityWaiter()
	}
}

/**
 * Per-partition fetch state
 */
interface PartitionState {
	topic: string
	partition: number
	offset: bigint
	paused: boolean
	abortController: AbortController // Per-partition abort signal for incremental control
	cachedLeader: Broker | null // Cached leader broker - invalidated on NotLeaderOrFollower
	// Bumped by seek(). A fetch captures the epoch at issue time; a response whose
	// partition epoch changed is dropped, so a seek that lands mid-flight is not
	// clobbered by the stale response advancing the offset.
	fetchEpoch: number
}

/**
 * Fetch manager configuration
 */
export interface FetchManagerConfig {
	/** Max bytes per partition in one Fetch request, and per delivered partition batch (estimated). */
	maxBytesPerPartition: number
	minBytes: number
	maxWaitMs: number
	partitionConcurrency: number
	isolationLevel: IsolationLevel
	/** Verify the CRC32C of each fetched batch (default true). See ConsumerConfig.checkCrcs. */
	checkCrcs?: boolean
	/**
	 * Maximum bytes of fetched data to hold for poll() mode, counted as held: record batches as
	 * fetched (compressed ones at their compressed size), a batch left decompressed part-way through
	 * (at most one per partition), and the records decoded ahead for the next poll at their estimated
	 * size. Kafka may also return one oversized first record batch per in-flight broker request.
	 * Default: maxBytesPerPartition * 10.
	 */
	maxBufferedBytes?: number
	/** Called whenever a fetched batch range advances the fetch position. */
	onFetchPosition?: (position: TopicPartitionOffset, recordCount: number) => void
	priority?: PriorityStrategy
}

/**
 * Default fetch manager configuration
 */
export const DEFAULT_FETCH_CONFIG: FetchManagerConfig = {
	maxBytesPerPartition: 1048576, // 1MB
	minBytes: 1,
	maxWaitMs: 5000,
	partitionConcurrency: 1,
	isolationLevel: 'read_committed',
	checkCrcs: true,
}

/**
 * Fetch manager
 *
 * Uses broker-level fetch batching: groups all ready partitions by their leader broker
 * and issues one fetch request per broker containing all partitions.
 */
export class FetchManager {
	private readonly cluster: Cluster
	private readonly config: FetchManagerConfig
	private readonly partitionStates: Map<string, PartitionState> = new Map()
	// Per-partition assignment epoch, bumped whenever a partition is removed. Survives
	// remove/re-add cycles (eager rebalance), so a batch drained before the rebalance can
	// be told apart from one fetched under the new assignment of the same partition.
	private readonly assignmentEpochs: Map<string, number> = new Map()
	private readonly logger: Logger
	private readonly autoOffsetReset?: AutoOffsetReset
	private readonly offsetManager?: OffsetManager
	private abortController: AbortController | null = null
	private running = false

	// Buffered poll mode fields
	private fetchBuffer: FetchBuffer | null = null
	private brokerInFlight: Map<number, boolean> = new Map()
	// A fatal, non-retriable fetch error (e.g. CORRUPT_MESSAGE) surfaced to the next poll().
	// The background fetch loop swallows rejected fetches (logs only), so fatal per-partition
	// errors are stashed here and re-thrown from poll() rather than silently dropping data.
	private pendingError: Error | null = null
	// Partitions currently held by handlers, as last passed to poll(). The consumer passes its live
	// `active` map, so the background fetch loop sees current busy state when it schedules fetches.
	private lastExcluded?: PartitionKeys
	private stageScheduled = false

	constructor(
		cluster: Cluster,
		offsetManager: OffsetManager,
		autoOffsetReset: AutoOffsetReset,
		config: Partial<FetchManagerConfig>,
		logger?: Logger
	) {
		this.cluster = cluster
		this.offsetManager = offsetManager
		this.autoOffsetReset = autoOffsetReset
		this.config = { ...DEFAULT_FETCH_CONFIG, ...config }
		this.logger = logger?.child({ component: 'fetch-manager' }) ?? noopLogger
	}

	/**
	 * Set partitions to fetch from with their starting offsets
	 * This replaces all partitions (used for eager rebalance)
	 */
	setPartitions(partitions: Array<TopicPartition & { offset: bigint }>): void {
		this.logger.debug('setting partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})
		// Abort all existing partition loops
		for (const [key, state] of this.partitionStates) {
			state.abortController.abort()
			this.bumpAssignmentEpoch(key)
		}
		this.partitionStates.clear()

		// Clear buffer for removed partitions (keep only new partitions)
		if (this.fetchBuffer) {
			const newKeys = new Set(partitions.map(p => tpKey(p.topic, p.partition)))
			this.fetchBuffer.removePartitionsNotIn(newKeys)
		}

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state: PartitionState = {
				topic: tp.topic,
				partition: tp.partition,
				offset: tp.offset,
				paused: false,
				abortController: new AbortController(),
				cachedLeader: null,
				fetchEpoch: 0,
			}
			this.partitionStates.set(key, state)
		}
	}

	/**
	 * Add partitions incrementally (used for cooperative rebalance)
	 */
	addPartitions(partitions: Array<TopicPartition & { offset: bigint }>): void {
		if (partitions.length === 0) return

		this.logger.debug('adding partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			// Skip if already exists
			if (this.partitionStates.has(key)) {
				this.logger.debug('partition already exists, skipping', { topic: tp.topic, partition: tp.partition })
				continue
			}

			const state: PartitionState = {
				topic: tp.topic,
				partition: tp.partition,
				offset: tp.offset,
				paused: false,
				abortController: new AbortController(),
				cachedLeader: null,
				fetchEpoch: 0,
			}
			this.partitionStates.set(key, state)
		}
	}

	/**
	 * Remove partitions incrementally (used for cooperative rebalance)
	 * Aborts fetch loops for removed partitions and clears buffered records
	 */
	removePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('removing partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				// Abort the partition's fetch loop
				state.abortController.abort()
				this.partitionStates.delete(key)
				// Fence batches already drained by poll(): if the partition is re-added
				// (eager rebalance keeps some partitions), those stale batches must not be
				// delivered — their records are re-fetched from the committed offset.
				this.bumpAssignmentEpoch(key)
			}
		}

		// Clear buffered records for removed partitions
		this.fetchBuffer?.removePartitions(partitions)
	}

	/**
	 * Pause partitions (stop fetching but keep state)
	 * Also clears any buffered records for the paused partitions
	 */
	pausePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('pausing partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		// Clear buffered records for paused partitions so they are not delivered. They were never
		// delivered but the fetch offset already advanced past them, so rewind to the earliest
		// buffered offset â otherwise those records are lost (never delivered, never re-fetched).
		const earliest = this.fetchBuffer?.removePartitionsReturningEarliest(partitions) ?? new Map<string, bigint>()

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				state.paused = true
				const earliestBuffered = earliest.get(key)
				if (earliestBuffered !== undefined && earliestBuffered < state.offset) {
					state.offset = earliestBuffered
					// Fence any in-flight fetch issued at the old (higher) position. The pause flag
					// alone is cleared on resume, so without bumping the epoch a stale fetch that
					// resolves after resume would buffer at the old offset and advance past the
					// rewound records, losing them. The very bug this rewind prevents.
					state.fetchEpoch++
				}
			}
		}
	}

	/**
	 * Resume paused partitions
	 */
	resumePartitions(partitions: TopicPartition[]): void {
		if (partitions.length === 0) return

		this.logger.debug('resuming partitions', {
			count: partitions.length,
			partitions: formatPartitions(partitions),
		})

		for (const tp of partitions) {
			const key = tpKey(tp.topic, tp.partition)
			const state = this.partitionStates.get(key)
			if (state) {
				state.paused = false
			}
		}
	}

	/**
	 * Seek to a specific offset for a partition
	 *
	 * Updates the fetch position for the partition to the given offset.
	 * The next fetch request will request records starting from this offset.
	 *
	 * @param topic - Topic name
	 * @param partition - Partition index
	 * @param offset - The offset to fetch from next
	 */
	seekPartition(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)
		const state = this.partitionStates.get(key)
		if (state) {
			this.logger.debug('seeking partition', {
				topic,
				partition,
				previousOffset: String(state.offset),
				newOffset: String(offset),
			})
			state.offset = offset
			// Fence any in-flight fetch issued at the old position, and drop buffered
			// records from before the seek, so neither clobbers the new position.
			state.fetchEpoch++
			this.fetchBuffer?.removePartitions([{ topic, partition }])
		} else {
			this.logger.warn('seek attempted on unassigned partition', { topic, partition })
		}
	}

	/**
	 * Update offset for a partition (after consuming)
	 */
	updateOffset(topic: string, partition: number, offset: bigint): void {
		const key = tpKey(topic, partition)
		const state = this.partitionStates.get(key)
		if (state) {
			state.offset = offset + 1n // Next offset to fetch
		}
	}

	/**
	 * Check if a partition is currently assigned to this consumer
	 * Used to filter stale records after rebalance
	 */
	isPartitionAssigned(topic: string, partition: number): boolean {
		return this.partitionStates.has(tpKey(topic, partition))
	}

	private bumpAssignmentEpoch(key: string): void {
		this.assignmentEpochs.set(key, (this.assignmentEpochs.get(key) ?? 0) + 1)
	}

	/**
	 * Current assignment epoch for a partition (0 if it was never removed)
	 */
	getAssignmentEpoch(topic: string, partition: number): number {
		return this.assignmentEpochs.get(tpKey(topic, partition)) ?? 0
	}

	/**
	 * Check whether a drained batch is still deliverable: its partition must be assigned
	 * AND its assignment epoch must match the current one. A mismatch means the partition
	 * was removed (and possibly re-added by an eager rebalance) after the batch was
	 * buffered — delivering it would duplicate records re-fetched from the committed
	 * offset under the new assignment.
	 */
	isBatchAssigned(batch: PartitionBatch): boolean {
		if (!this.partitionStates.has(tpKey(batch.topic, batch.partition))) {
			return false
		}
		return (
			batch.assignmentEpoch === undefined ||
			batch.assignmentEpoch === this.getAssignmentEpoch(batch.topic, batch.partition)
		)
	}

	/**
	 * Handle fetch error for a partition
	 */
	private async handleFetchError(state: PartitionState, errorCode: ErrorCode): Promise<void> {
		switch (errorCode) {
			case ErrorCode.OffsetOutOfRange:
				if (this.offsetManager && this.autoOffsetReset) {
					if (this.autoOffsetReset === 'none') {
						this.pendingError = new Error(
							`Offset out of range for ${state.topic}-${state.partition} at offset ${state.offset}`
						)
						break
					}
					// Records buffered from the now-invalid position precede (or are unrelated
					// to) the reset offset; delivering them would violate the reset. Discard them
					// BEFORE the listOffsets round-trip below â otherwise a concurrent poll() could
					// drain the stale records while we await, or a transient lookup failure could
					// skip the clear entirely.
					this.fetchBuffer?.removePartitions([{ topic: state.topic, partition: state.partition }])

					// Capture the epoch across the (awaited) offset lookup: if a seek lands while we
					// resolve the reset offset, the seek takes precedence and we must not overwrite
					// its position with the reset.
					{
						const epochBeforeReset = state.fetchEpoch
						const resetOffset =
							this.autoOffsetReset === 'earliest'
								? await this.offsetManager.getEarliestOffset(state.topic, state.partition)
								: await this.offsetManager.getLatestOffset(state.topic, state.partition)
						if (state.fetchEpoch === epochBeforeReset) {
							state.offset = resetOffset
						}
					}
				}
				break

			case ErrorCode.NotLeaderOrFollower:
			case ErrorCode.UnknownTopicOrPartition:
				state.cachedLeader = null // Invalidate cached leader
				await this.cluster.refreshMetadata([state.topic])
				break

			case ErrorCode.CorruptMessage:
				// Re-fetching the same offset yields the same corrupt record, so this is not
				// retriable. Surface it to the consumer (via the next poll()) instead of logging
				// and dropping the partition's data; the position is left unchanged so the user can
				// seek past the bad offset to recover.
				this.pendingError = new KafkaProtocolError(
					ErrorCode.CorruptMessage,
					`${state.topic}-${state.partition} at offset ${state.offset}`
				)
				break

			default:
				this.logger.error('fetch error', {
					topic: state.topic,
					partition: state.partition,
					errorCode,
				})
		}
	}

	/**
	 * Stop the fetch loop
	 */
	stop(): void {
		this.logger.debug('stopping fetch manager')
		this.running = false
		// Abort global controller (for sleep in start loop)
		this.abortController?.abort()
		// Abort all per-partition controllers
		for (const state of this.partitionStates.values()) {
			state.abortController.abort()
		}
		// Cleanup buffered poll mode
		if (this.fetchBuffer) {
			this.fetchBuffer.clear()
			this.fetchBuffer = null
		}
		this.brokerInFlight.clear()
	}

	/**
	 * Background fetch loop for buffered poll mode
	 * Continuously fetches from brokers and buffers results
	 * Uses one-outstanding-fetch-per-broker pattern like Java client
	 */
	private async runBackgroundFetchLoop(): Promise<void> {
		const signal = this.abortController?.signal
		// Reusable array to avoid allocations on every iteration
		const readyPartitions: PartitionState[] = []

		while (this.running && !signal?.aborted) {
			try {
				const fetchBuffer = this.fetchBuffer
				if (!fetchBuffer) break

				// Do not issue another Fetch once buffered data plus in-flight response
				// reservations have reached the memory budget.
				if (fetchBuffer.isFull()) {
					await fetchBuffer.waitForCapacity(signal)
					continue
				}

				// Get ready (non-paused) partitions - reuse array
				readyPartitions.length = 0
				const corrupt = fetchBuffer.corruptPartitions()
				for (const [key, state] of this.partitionStates) {
					if (!state.paused && !corrupt.has(key)) {
						readyPartitions.push(state)
					}
				}

				if (this.config.priority && readyPartitions.length > 0) {
					const decision = this.config.priority.schedule(
						this.buildSchedulerState(fetchBuffer, this.lastExcluded)
					)
					const allowed = new Set(decision.fetch.map(tp => tpKey(tp.topic, tp.partition)))
					let writeIndex = 0
					for (const state of readyPartitions) {
						if (allowed.has(tpKey(state.topic, state.partition))) readyPartitions[writeIndex++] = state
					}
					readyPartitions.length = writeIndex
					if (readyPartitions.length === 0) {
						// Nothing fetchable until the buffer drains or a handler frees a partition.
						await sleep(10, { signal }).catch(() => {})
						continue
					}
				}

				if (readyPartitions.length === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}

				// Group partitions by leader broker
				const partitionsByBroker = await this.groupPartitionsByBroker(readyPartitions)

				// stop() may have run during the await above; don't issue a final fetch
				// against the stale buffer captured before it.
				if (!this.running || signal?.aborted) break

				if (partitionsByBroker.size === 0) {
					await sleep(100, { signal }).catch(() => {})
					continue
				}
				// Issue fetch to each broker that doesn't have an in-flight request. Split
				// remaining capacity across those brokers so concurrent responses cannot each
				// independently claim the entire buffer budget.
				let fetchesIssued = 0
				const availableBrokers = Array.from(partitionsByBroker.entries()).filter(
					([brokerId]) => !this.brokerInFlight.get(brokerId)
				)
				for (let index = 0; index < availableBrokers.length; index++) {
					const [brokerId, { broker, partitions }] = availableBrokers[index]!
					const remainingBrokers = availableBrokers.length - index
					const fairShare = Math.floor(fetchBuffer.remainingCapacity() / remainingBrokers)
					const naturalMaxBytes = this.config.maxBytesPerPartition * partitions.length
					const reservedBytes = fetchBuffer.reserveCapacity(Math.min(naturalMaxBytes, fairShare))
					if (reservedBytes === 0) break

					this.brokerInFlight.set(brokerId, true)
					fetchesIssued++

					// Fire and forget - don't await, let it run in background
					this.fetchFromBrokerToBuffer(broker, partitions, reservedBytes)
						.catch(error => {
							this.logger.error('background fetch failed', {
								brokerId,
								error: (error as Error).message,
							})
						})
						.finally(() => {
							fetchBuffer.releaseCapacity(reservedBytes)
							this.brokerInFlight.set(brokerId, false)
						})
				}

				// Only sleep when necessary to prevent busy-loop. Capacity backpressure
				// is handled at the top of the loop before any request is issued.
				if (fetchesIssued === 0) {
					// All brokers have in-flight requests, wait briefly
					await sleep(1, { signal }).catch(() => {})
				}
				// If fetches were issued and buffer is not full, continue immediately
			} catch (error) {
				if (signal?.aborted) break
				this.logger.error('background fetch loop error', { error: (error as Error).message })
				await sleep(100, { signal }).catch(() => {})
			}
		}
	}

	/**
	 * Group partitions by their leader broker
	 */
	private async groupPartitionsByBroker(
		partitions: PartitionState[]
	): Promise<Map<number, { broker: Broker; partitions: PartitionState[] }>> {
		const partitionsByBroker = new Map<number, { broker: Broker; partitions: PartitionState[] }>()

		// Collect partitions needing leader lookup
		const needsLookup: PartitionState[] = []
		for (const state of partitions) {
			if (state.cachedLeader) {
				const brokerId = state.cachedLeader.nodeId
				if (!partitionsByBroker.has(brokerId)) {
					partitionsByBroker.set(brokerId, { broker: state.cachedLeader, partitions: [] })
				}
				partitionsByBroker.get(brokerId)!.partitions.push(state)
			} else {
				needsLookup.push(state)
			}
		}

		// Parallel leader lookups for cache misses
		if (needsLookup.length > 0) {
			const lookups = await Promise.all(
				needsLookup.map(async state => {
					try {
						const leader = await this.cluster.getLeaderForPartition(state.topic, state.partition)
						state.cachedLeader = leader
						return { state, leader }
					} catch (error) {
						this.logger.error('failed to get leader', {
							topic: state.topic,
							partition: state.partition,
							error: (error as Error).message,
						})
						return null
					}
				})
			)

			for (const result of lookups) {
				if (!result) continue
				const brokerId = result.leader.nodeId
				if (!partitionsByBroker.has(brokerId)) {
					partitionsByBroker.set(brokerId, { broker: result.leader, partitions: [] })
				}
				partitionsByBroker.get(brokerId)!.partitions.push(result.state)
			}
		}

		return partitionsByBroker
	}

	/**
	 * Fetch from a single broker and add results to buffer
	 */
	private async fetchFromBrokerToBuffer(
		broker: Broker,
		partitions: PartitionState[],
		maxBytes: number
	): Promise<void> {
		if (!this.fetchBuffer) return

		// Map response entries back to the in-flight state we issued the fetch
		// against. Used to fence stale responses via state.abortController and fetchEpoch.
		const inflightStateByKey = new Map<string, PartitionState>()
		// Epoch captured at issue time; a seek during the fetch bumps state.fetchEpoch,
		// so the stale response is dropped instead of clobbering the new position.
		const issuedEpochByKey = new Map<string, number>()
		const topicsMap = new Map<string, Array<{ state: PartitionState }>>()
		for (const state of partitions) {
			if (!topicsMap.has(state.topic)) {
				topicsMap.set(state.topic, [])
			}
			topicsMap.get(state.topic)!.push({ state })
			const key = tpKey(state.topic, state.partition)
			inflightStateByKey.set(key, state)
			issuedEpochByKey.set(key, state.fetchEpoch)
		}

		const topics = Array.from(topicsMap.entries()).map(([topic, parts]) => ({
			topic,
			topicId: '00000000-0000-0000-0000-000000000000',
			partitions: parts.map(p => ({
				partitionIndex: p.state.partition,
				currentLeaderEpoch: -1,
				fetchOffset: p.state.offset,
				lastFetchedEpoch: -1,
				logStartOffset: -1n,
				partitionMaxBytes: this.config.maxBytesPerPartition,
			})),
		}))

		const response = await broker.fetch({
			replicaId: -1,
			maxWaitMs: this.config.maxWaitMs,
			minBytes: this.config.minBytes,
			maxBytes,
			isolationLevel: this.config.isolationLevel === 'read_committed' ? 1 : 0,
			topics,
		})

		for (const topicResponse of response.topics) {
			for (const partitionResponse of topicResponse.partitions) {
				const key = tpKey(topicResponse.topic, partitionResponse.partitionIndex)
				const state = inflightStateByKey.get(key)
				// Fence: state was revoked, the partition was paused, or a seek changed the fetch
				// position while the fetch was in flight. Drop the records without advancing the
				// offset so they are re-fetched (on resume / from the post-seek position) instead
				// of being clobbered.
				if (
					!state ||
					state.abortController.signal.aborted ||
					state.paused ||
					state.fetchEpoch !== issuedEpochByKey.get(key)
				)
					continue

				if (partitionResponse.errorCode !== ErrorCode.None) {
					await this.handleFetchError(state, partitionResponse.errorCode)
					continue
				}

				if (partitionResponse.recordsData && partitionResponse.recordsData.length > 0) {
					const fetchOffset = state.offset
					const read = readFetchedRecords(partitionResponse.recordsData, {
						fetchOffset,
						abortedTransactions: partitionResponse.abortedTransactions,
						lastStableOffset: partitionResponse.lastStableOffset,
						readCommitted: this.config.isolationLevel === 'read_committed',
						verifyCrc: this.config.checkCrcs !== false,
					})
					const fetched = read instanceof Promise ? await read : read

					// Reading may have awaited a decode; re-fence (revoked, paused, or seeked meanwhile).
					if (
						state.abortController.signal.aborted ||
						state.paused ||
						state.fetchEpoch !== issuedEpochByKey.get(key)
					)
						continue

					// A corrupt batch is buffered behind the records before it, so a drain delivers them and
					// then raises it like a broker CORRUPT_MESSAGE. Re-fetching returns the same bytes, so the
					// partition is not fetched again until then.
					if (fetched.recordCount > 0 || fetched.corruption) {
						const assignmentEpoch = this.getAssignmentEpoch(state.topic, state.partition)
						this.fetchBuffer.add(new CompletedFetch(state.topic, state.partition, assignmentEpoch, fetched))
					}

					// Advance the fetch position past the whole fetched batch range, even when no
					// records survived (all aborted / control-only / empty-compacted) — otherwise the
					// position never moves and the same offset is re-fetched forever. nextOffset is the
					// batch-declared end (>= last record offset + 1), which also steps over a compacted
					// tail. Guarded by `> state.offset` so a stale/overlapping batch can't rewind.
					const advanceTo = fetched.nextOffset
					if (advanceTo !== null && advanceTo > state.offset) {
						state.offset = advanceTo
						this.config.onFetchPosition?.(
							{ topic: state.topic, partition: state.partition, offset: advanceTo },
							fetched.recordCount
						)
					}
				}
			}
		}
	}

	/**
	 * Poll for records from all ready partitions (pull-based API)
	 *
	 * Uses buffered polling with background fetch loop for better performance.
	 * The background loop is started lazily on first call.
	 *
	 * Returns records grouped by topic/partition. Internal offsets are advanced
	 * so subsequent polls return new records.
	 */
	async poll(excludedPartitions?: PartitionKeys): Promise<PartitionBatch[]> {
		// Surface a fatal fetch error (e.g. CORRUPT_MESSAGE) recorded by the background loop.
		// Cleared once thrown; if the condition persists the next fetch re-records it.
		this.throwPendingError()

		// Lazy initialization: start background fetch loop on first call
		if (!this.fetchBuffer) {
			const maxBufferedBytes = this.config.maxBufferedBytes ?? this.config.maxBytesPerPartition * 10
			this.fetchBuffer = new FetchBuffer(maxBufferedBytes, () => this.scheduleStage())
			this.running = true
			this.abortController = new AbortController()
			// Fire and forget - loop runs until stopped via running=false or abort signal
			this.runBackgroundFetchLoop()
		}

		// Return immediately if buffer has data
		if (this.fetchBuffer.hasDataFor(excludedPartitions)) {
			return this.drainBuffer(excludedPartitions)
		}

		// Wait for data to arrive (up to maxWaitMs)
		const hasData = await this.fetchBuffer.waitForData(
			this.config.maxWaitMs,
			this.abortController?.signal,
			excludedPartitions
		)

		// A fatal error may have been recorded by the background loop while we waited (it adds no
		// data, so the waiter isn't signalled). Surface it now rather than returning an empty poll.
		this.throwPendingError()

		return hasData ? this.drainBuffer(excludedPartitions) : []
	}

	/** Wake a poll waiting for an idle partition after handler state changes. */
	wakePoll(): void {
		this.fetchBuffer?.wakeWaiters()
	}

	/** Throw and clear any fatal fetch error recorded by the background loop. No-op otherwise. */
	private throwPendingError(): void {
		if (!this.pendingError) return
		const error = this.pendingError
		this.pendingError = null
		throw error
	}

	/**
	 * Drain buffer and return completed fetches directly
	 */
	private async drainBuffer(excludedPartitions?: PartitionKeys): Promise<PartitionBatch[]> {
		const fetchBuffer = this.fetchBuffer
		if (!fetchBuffer) return []
		const { maxBytesPerPartition, priority } = this.config
		let plan: ScheduleDecision['drain'] | undefined
		if (priority) {
			this.lastExcluded = excludedPartitions
			plan = priority.schedule(this.buildSchedulerState(fetchBuffer, excludedPartitions)).drain
		}
		const { batches, failures } = await fetchBuffer.drain(maxBytesPerPartition, excludedPartitions, plan)
		for (const { fetch, error } of failures) this.failCorruptFetch(fetch, error)
		this.scheduleStage()
		// With no records to deliver first, the corrupt data is raised from this poll.
		if (batches.length === 0) this.throwPendingError()
		return batches
	}

	/**
	 * Decode the next poll's records in the background, so decompression and parsing overlap the
	 * handlers instead of running between them. Deferred past the current poll's continuation so its
	 * caller starts the handlers first.
	 */
	private scheduleStage(): void {
		if (this.stageScheduled) return
		this.stageScheduled = true
		setImmediate(() => {
			this.stageScheduled = false
			const fetchBuffer = this.fetchBuffer
			if (!fetchBuffer) return
			const { maxBytesPerPartition, priority } = this.config
			let plan: ScheduleDecision['drain'] | undefined
			try {
				plan = priority?.schedule(this.buildSchedulerState(fetchBuffer)).drain
			} catch {
				// The same strategy runs again on the poll path, which raises its error.
				return
			}
			fetchBuffer.stage(maxBytesPerPartition, plan).catch((error: unknown) => {
				this.logger.error('decoding ahead failed', { error: (error as Error).message })
			})
		})
	}

	/**
	 * Surface corrupt data a drain reached like a broker CORRUPT_MESSAGE, after the records before it
	 * are delivered, naming the bad batch's offset when it is known. The fetch position rewinds to
	 * the first undelivered record, which re-fetching would fail on again, so the user can seek past
	 * the bad batch.
	 */
	private failCorruptFetch(fetch: CompletedFetch, error: unknown): void {
		const { topic, partition } = fetch
		this.fetchBuffer?.removePartitions([{ topic, partition }])
		const state = this.partitionStates.get(tpKey(topic, partition))
		if (state && fetch.assignmentEpoch === this.getAssignmentEpoch(topic, partition)) {
			state.offset = fetch.position
			// Fence a fetch already issued past the bad batch so it cannot move the position again.
			state.fetchEpoch++
		}
		this.pendingError = corruptDataError(topic, partition, fetch.failedAt(error), error)
	}

	/** Buffered partitions first (in buffer order), then idle assigned partitions with zero buffered. */
	private buildSchedulerState(fetchBuffer: FetchBuffer, excludedPartitions?: PartitionKeys): SchedulerState {
		const buffered = new Map(fetchBuffer.snapshot().map(stat => [tpKey(stat.topic, stat.partition), stat]))
		const partitions: SchedulerPartition[] = []
		const describe = (state: PartitionState, key: string): SchedulerPartition => {
			const stat = buffered.get(key)
			return {
				topic: state.topic,
				partition: state.partition,
				bufferedBytes: stat?.bytes ?? 0,
				busy: excludedPartitions?.has(key) ?? false,
			}
		}
		for (const key of buffered.keys()) {
			const state = this.partitionStates.get(key)
			if (state && !state.paused) partitions.push(describe(state, key))
		}
		for (const [key, state] of this.partitionStates) {
			if (!state.paused && !buffered.has(key)) partitions.push(describe(state, key))
		}
		return {
			partitions,
			byteBudget: this.config.maxBytesPerPartition,
			bufferCapacityBytes: fetchBuffer.maxBytes,
			bufferedBytes: fetchBuffer.sizeBytes,
		}
	}
}
