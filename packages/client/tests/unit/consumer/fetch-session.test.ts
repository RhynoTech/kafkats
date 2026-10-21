import { describe, expect, it } from 'vitest'

import { FetchSessionHandler, type FetchSessionPartition } from '@/consumer/fetch-session.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'

function part(topic: string, partition: number, fetchOffset: bigint, maxBytes = 1024): FetchSessionPartition {
	return { topic, partition, fetchOffset, maxBytes }
}

function response(sessionId: number, partitions: Array<[string, number]> = [], errorCode = ErrorCode.None) {
	const topics = new Map<string, number[]>()
	for (const [topic, partition] of partitions) topics.set(topic, [...(topics.get(topic) ?? []), partition])
	return {
		errorCode,
		sessionId,
		topics: Array.from(topics, ([topic, indexes]) => ({
			topic,
			topicId: '',
			partitions: indexes.map(partitionIndex => ({
				partitionIndex,
				errorCode: ErrorCode.None,
				highWatermark: 0n,
				lastStableOffset: 0n,
				logStartOffset: 0n,
				abortedTransactions: [],
				preferredReadReplica: -1,
				recordsData: null,
			})),
		})),
	}
}

/** A handler whose session 42 holds t-0@10, t-1@20 and u-0@30, at epoch 1. */
function establishedSession(): FetchSessionHandler {
	const handler = new FetchSessionHandler()
	handler.build([part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)])
	expect(
		handler.handleResponse(
			response(42, [
				['t', 0],
				['t', 1],
				['u', 0],
			])
		)
	).toBe(true)
	return handler
}

describe('FetchSessionHandler', () => {
	it('starts with a full fetch that asks the broker for a session', () => {
		const handler = new FetchSessionHandler()
		const next = [part('t', 0, 10n), part('t', 1, 20n)]

		const request = handler.build(next)

		expect(request).toEqual({ sessionId: 0, sessionEpoch: 0, toSend: next, toForget: [] })
	})

	it('sends only added and changed partitions once the broker created a session', () => {
		const handler = establishedSession()
		expect(handler.metadata).toEqual({ sessionId: 42, epoch: 1 })

		const request = handler.build([part('t', 0, 15n), part('t', 1, 20n), part('u', 0, 30n), part('v', 0, 0n)])

		expect(request).toEqual({
			sessionId: 42,
			sessionEpoch: 1,
			toSend: [part('t', 0, 15n), part('v', 0, 0n)],
			toForget: [],
		})
	})

	it('resends a partition whose per-partition byte limit changed', () => {
		const handler = establishedSession()

		const request = handler.build([part('t', 0, 10n, 2048), part('t', 1, 20n), part('u', 0, 30n)])

		expect(request.toSend).toEqual([part('t', 0, 10n, 2048)])
	})

	it('forgets partitions that are no longer fetched from this broker', () => {
		const handler = establishedSession()

		const request = handler.build([part('t', 1, 20n)])

		expect(request.toSend).toEqual([])
		expect(request.toForget).toEqual([
			{ topic: 't', partition: 0 },
			{ topic: 'u', partition: 0 },
		])
		expect(handler.handleResponse(response(42))).toBe(true)

		// A forgotten partition that comes back is added again.
		expect(handler.build([part('t', 0, 10n), part('t', 1, 20n)]).toSend).toEqual([part('t', 0, 10n)])
	})

	it('sends an empty incremental request when nothing changed', () => {
		const handler = establishedSession()

		const request = handler.build([part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)])

		expect(request).toMatchObject({ sessionId: 42, sessionEpoch: 1, toSend: [], toForget: [] })
	})

	it('accepts an incremental response that omits unchanged partitions and advances the epoch', () => {
		const handler = establishedSession()

		for (let epoch = 1; epoch <= 3; epoch++) {
			expect(handler.build([part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)]).sessionEpoch).toBe(epoch)
			expect(handler.handleResponse(response(42, [['u', 0]]))).toBe(true)
		}
		expect(handler.metadata).toEqual({ sessionId: 42, epoch: 4 })
	})

	it('wraps the epoch to 1 after the largest int32', () => {
		const handler = establishedSession()
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		;(handler as any).epoch = 2 ** 31 - 1

		handler.build([part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)])
		handler.handleResponse(response(42))

		expect(handler.metadata.epoch).toBe(1)
	})

	it('stays on full fetches when the broker could not create a session', () => {
		const handler = new FetchSessionHandler()
		const next = [part('t', 0, 10n)]
		handler.build(next)

		expect(handler.handleResponse(response(0, [['t', 0]]))).toBe(true)

		expect(handler.build(next)).toMatchObject({ sessionId: 0, sessionEpoch: 0, toSend: next })
	})

	it('starts a new session after FETCH_SESSION_ID_NOT_FOUND', () => {
		const handler = establishedSession()
		const next = [part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)]
		handler.build(next)

		expect(handler.handleResponse(response(0, [], ErrorCode.FetchSessionIdNotFound))).toBe(false)

		expect(handler.build(next)).toEqual({ sessionId: 0, sessionEpoch: 0, toSend: next, toForget: [] })
	})

	it.each([
		['INVALID_FETCH_SESSION_EPOCH', ErrorCode.InvalidFetchSessionEpoch],
		['FETCH_SESSION_TOPIC_ID_ERROR', ErrorCode.FetchSessionTopicIdError],
	])('closes the session and starts a new one after %s', (_name, errorCode) => {
		const handler = establishedSession()
		const next = [part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)]
		handler.build(next)

		expect(handler.handleResponse(response(42, [], errorCode))).toBe(false)

		// Epoch 0 with the old session ID tells the broker to drop that session before creating another.
		expect(handler.build(next)).toEqual({ sessionId: 42, sessionEpoch: 0, toSend: next, toForget: [] })
		expect(
			handler.handleResponse(
				response(43, [
					['t', 0],
					['t', 1],
					['u', 0],
				])
			)
		).toBe(true)
		expect(handler.metadata).toEqual({ sessionId: 43, epoch: 1 })
	})

	it('falls back to a full fetch after a failed request', () => {
		const handler = establishedSession()
		const next = [part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)]
		handler.build([part('t', 0, 10n)])

		handler.handleError()

		expect(handler.build(next)).toEqual({ sessionId: 42, sessionEpoch: 0, toSend: next, toForget: [] })
	})

	it('rejects a full response that omits a partition, since the new session would not hold it', () => {
		const handler = new FetchSessionHandler()
		const next = [part('t', 0, 10n), part('t', 1, 20n)]
		handler.build(next)

		expect(handler.handleResponse(response(42, [['t', 0]]))).toBe(false)

		expect(handler.build(next)).toMatchObject({ sessionId: 42, sessionEpoch: 0 })
	})

	it('rejects an incremental response with a partition outside the session', () => {
		const handler = establishedSession()
		handler.build([part('t', 1, 20n), part('u', 0, 30n)])

		expect(handler.handleResponse(response(42, [['t', 0]]))).toBe(false)

		expect(handler.metadata).toEqual({ sessionId: 42, epoch: 0 })
	})

	it('starts over when the broker closes the session in an incremental response', () => {
		const handler = establishedSession()
		handler.build([part('t', 0, 10n)])

		expect(handler.handleResponse(response(0, [['t', 0]]))).toBe(true)

		expect(handler.metadata).toEqual({ sessionId: 0, epoch: 0 })
	})

	it('falls back to sessionless fetches when the broker answers UNSUPPORTED_VERSION', () => {
		const handler = establishedSession()
		const next = [part('t', 0, 10n), part('t', 1, 20n), part('u', 0, 30n)]
		handler.build(next)

		expect(handler.handleResponse(response(0, [], ErrorCode.UnsupportedVersion))).toBe(false)

		// The first sessionless request also closes the old session; later ones carry no session.
		expect(handler.build(next)).toEqual({ sessionId: 42, sessionEpoch: -1, toSend: next, toForget: [] })
		expect(handler.handleResponse(response(0, [['t', 0]]))).toBe(true)
		expect(handler.build(next)).toMatchObject({ sessionId: 0, sessionEpoch: -1 })
		handler.handleError()
		expect(handler.build(next)).toMatchObject({ sessionId: 0, sessionEpoch: -1 })
	})

	it('returns the session to close and starts over', () => {
		const handler = establishedSession()

		expect(handler.close()).toBe(42)
		expect(handler.close()).toBe(0)
		expect(handler.build([part('t', 0, 10n)])).toMatchObject({ sessionId: 0, sessionEpoch: 0 })
	})
})
