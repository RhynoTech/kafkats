import { describe, expect, it } from 'vitest'
import { strict } from '@/consumer/priority.js'
import type { SchedulerPartition, SchedulerState } from '@/consumer/priority.js'

function partition(
	topic: string,
	partition: number,
	bufferedBytes: number,
	busy = false,
	deliverableBytes = bufferedBytes
): SchedulerPartition {
	return { topic, partition, bufferedBytes, deliverableBytes, busy }
}

function state(partitions: SchedulerPartition[], byteBudget = 100): SchedulerState {
	return { partitions, byteBudget, bufferCapacityBytes: 1000, bufferedBytes: 100 }
}

describe('strict topic priority', () => {
	it('drains configured topics first and leaves unlisted topics in state order', () => {
		const decision = strict({ order: ['live', 'bulk'] }).schedule(
			state([
				partition('other', 0, 30),
				partition('bulk', 0, 30),
				partition('live', 0, 30),
				partition('other', 1, 30),
			])
		)
		expect(decision.drain.map(p => `${p.topic}:${p.partition}`)).toEqual(['live:0', 'bulk:0', 'other:0', 'other:1'])
	})

	it('skips busy partitions and gates lower-tier fetches by available higher-priority bytes', () => {
		const strategy = strict({ order: ['live', 'bulk'] })
		expect(strategy.schedule(state([partition('live', 0, 100), partition('bulk', 0, 0)])).fetch).toEqual([
			{ topic: 'live', partition: 0 },
		])
		expect(strategy.schedule(state([partition('live', 0, 100, true), partition('bulk', 0, 0)])).fetch).toEqual([
			{ topic: 'live', partition: 0 },
			{ topic: 'bulk', partition: 0 },
		])
	})

	it('gates lower tiers by the deliverable bytes of higher ones, not their fetched bytes', () => {
		// live holds 10 compressed bytes that decode to a full 100-byte budget.
		const decision = strict({ order: ['live', 'bulk'] }).schedule(
			state([partition('live', 0, 10, false, 100), partition('bulk', 0, 0)])
		)
		expect(decision.fetch).toEqual([{ topic: 'live', partition: 0 }])
	})

	it('validates its order', () => {
		expect(() => strict({ order: [] })).toThrow('must not be empty')
		expect(() => strict({ order: ['live', 'live'] })).toThrow('duplicates')
	})
})
