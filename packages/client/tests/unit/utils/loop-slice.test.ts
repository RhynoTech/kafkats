import { afterEach, describe, expect, it, vi } from 'vitest'

import { LoopSlice } from '@/utils/loop-slice.js'

const nextCheckPhase = () => new Promise<void>(resolve => setImmediate(resolve))

describe('LoopSlice', () => {
	afterEach(() => {
		vi.restoreAllMocks()
	})

	it('expires once synchronous work in one loop turn reaches its length', () => {
		let now = 1000
		vi.spyOn(performance, 'now').mockImplementation(() => now)
		const slice = new LoopSlice(2)

		expect(slice.expired).toBe(false)
		now += 1.9
		expect(slice.expired).toBe(false)
		now += 0.1
		expect(slice.expired).toBe(true)
	})

	it('does not count time before its first check in a turn', async () => {
		let now = 1000
		vi.spyOn(performance, 'now').mockImplementation(() => now)
		const slice = new LoopSlice(2)
		expect(slice.expired).toBe(false)
		now += 5
		expect(slice.expired).toBe(true)

		await nextCheckPhase()
		now += 100
		expect(slice.expired).toBe(false)
	})

	it('keeps counting across microtasks until the loop reaches its check phase', async () => {
		let now = 1000
		vi.spyOn(performance, 'now').mockImplementation(() => now)
		const slice = new LoopSlice(2)
		expect(slice.expired).toBe(false)

		await Promise.resolve()
		now += 3
		expect(slice.expired).toBe(true)
		await slice.yield()
		expect(slice.expired).toBe(false)
	})
})
