/**
 * Client half of a KIP-227 incremental fetch session with one broker, modelled on the Java client's
 * FetchSessionHandler.
 *
 * The first request is a full fetch that asks the broker to create a session. Later requests are
 * incremental: they name only the partitions that were added or whose fetch parameters changed, plus
 * the partitions to forget, and the broker reads every other partition from its session cache. The
 * broker also moves each partition that returned records to the back of the session, so a partition
 * with a backlog cannot take every response from the partitions after it.
 */

import { ErrorCode } from '@/protocol/messages/error-codes.js'
import type { FetchResponse } from '@/protocol/messages/responses/fetch.js'
import { tpKey } from '@/utils/topic-partition.js'

const INVALID_SESSION_ID = 0
const INITIAL_EPOCH = 0
/** Closes the session the request names; with no session it asks for a sessionless fetch. */
const FINAL_EPOCH = -1
const MAX_EPOCH = 2 ** 31 - 1

export interface FetchSessionPartition {
	topic: string
	partition: number
	fetchOffset: bigint
	maxBytes: number
}

export interface FetchSessionRequest {
	sessionId: number
	sessionEpoch: number
	/** Every partition on a full fetch; otherwise only those added or changed since the last request. */
	toSend: FetchSessionPartition[]
	/** Partitions the broker should drop from the session. Always empty on a full fetch. */
	toForget: Array<{ topic: string; partition: number }>
}

export class FetchSessionHandler {
	private sessionId = INVALID_SESSION_ID
	private epoch = INITIAL_EPOCH
	private sessionless = false
	// What the broker's session holds once the last request is applied. The broker owns the order.
	private partitions = new Map<string, FetchSessionPartition>()

	/** Session ID and epoch the next request will carry. */
	get metadata(): { sessionId: number; epoch: number } {
		return { sessionId: this.sessionId, epoch: this.epoch }
	}

	private get isFull(): boolean {
		return this.epoch === INITIAL_EPOCH || this.epoch === FINAL_EPOCH
	}

	/**
	 * Build the next request for `next`, the partitions to fetch from this broker now. The handler
	 * assumes the broker applies the request; any failure leads to a full fetch, which resends all.
	 */
	build(next: FetchSessionPartition[]): FetchSessionRequest {
		const metadata = { sessionId: this.sessionId, sessionEpoch: this.epoch }
		if (this.isFull) {
			this.partitions = new Map(next.map(partition => [tpKey(partition.topic, partition.partition), partition]))
			return { ...metadata, toSend: next, toForget: [] }
		}

		const toSend: FetchSessionPartition[] = []
		const toForget: FetchSessionRequest['toForget'] = []
		const nextKeys = new Set<string>()
		for (const partition of next) {
			const key = tpKey(partition.topic, partition.partition)
			nextKeys.add(key)
			const cached = this.partitions.get(key)
			if (!cached || cached.fetchOffset !== partition.fetchOffset || cached.maxBytes !== partition.maxBytes) {
				this.partitions.set(key, partition)
				toSend.push(partition)
			}
		}
		for (const [key, cached] of this.partitions) {
			if (nextKeys.has(key)) continue
			this.partitions.delete(key)
			toForget.push({ topic: cached.topic, partition: cached.partition })
		}
		return { ...metadata, toSend, toForget }
	}

	/**
	 * Apply a response to the session. Returns false when its partitions must be ignored: the broker
	 * rejected the session, or the response does not match it. The next request is then a full fetch.
	 */
	handleResponse(response: Pick<FetchResponse, 'errorCode' | 'sessionId' | 'topics'>): boolean {
		if (response.errorCode !== ErrorCode.None) {
			if (response.errorCode === ErrorCode.FetchSessionIdNotFound) {
				this.sessionId = INVALID_SESSION_ID
				this.epoch = INITIAL_EPOCH
			} else if (response.errorCode === ErrorCode.UnsupportedVersion) {
				// Keeping the session ID makes the next sessionless request close the session too.
				this.sessionless = true
				this.epoch = FINAL_EPOCH
			} else {
				this.epoch = this.sessionless ? FINAL_EPOCH : INITIAL_EPOCH
			}
			return false
		}

		if (this.sessionless) {
			this.sessionId = INVALID_SESSION_ID
			return true
		}

		if (this.isFull) {
			if (response.sessionId === INVALID_SESSION_ID) {
				this.sessionId = INVALID_SESSION_ID
				this.epoch = INITIAL_EPOCH
				return true
			}
			// The broker builds the session from the partitions in its response, so a partition missing
			// from it would never be fetched again.
			this.sessionId = response.sessionId
			if (!this.matchesSession(response, true)) {
				this.epoch = INITIAL_EPOCH
				return false
			}
			this.epoch = 1
			return true
		}

		if (!this.matchesSession(response, false)) {
			this.epoch = INITIAL_EPOCH
			return false
		}
		if (response.sessionId === INVALID_SESSION_ID) {
			// The broker closed the session, as it does once an incremental request forgets every partition.
			this.epoch = INITIAL_EPOCH
		} else {
			this.epoch = this.epoch === MAX_EPOCH ? 1 : this.epoch + 1
		}
		this.sessionId = response.sessionId
		return true
	}

	/** The request failed or its response was lost, so the broker's epoch is unknown. */
	handleError(): void {
		this.epoch = this.sessionless ? FINAL_EPOCH : INITIAL_EPOCH
	}

	/** Start over, returning the session ID the broker should close, or 0 when it holds none. */
	close(): number {
		const sessionId = this.sessionId
		this.sessionId = INVALID_SESSION_ID
		this.epoch = this.sessionless ? FINAL_EPOCH : INITIAL_EPOCH
		this.partitions.clear()
		return sessionId
	}

	private matchesSession(response: Pick<FetchResponse, 'topics'>, full: boolean): boolean {
		let count = 0
		for (const topic of response.topics) {
			for (const partition of topic.partitions) {
				if (!this.partitions.has(tpKey(topic.topic, partition.partitionIndex))) return false
				count++
			}
		}
		return !full || count === this.partitions.size
	}
}
