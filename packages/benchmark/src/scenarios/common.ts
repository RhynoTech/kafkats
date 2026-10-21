import { performance, PerformanceObserver } from 'node:perf_hooks'

import type { Client, Lib } from './types.js'

type Producer = ReturnType<Client['producer']>

/**
 * Opens every transactional lane before the window. Failures are kept as errors and retried, so a
 * client that cannot start its lanes reliably shows up in the result instead of aborting the run.
 */
export async function warmTransactionalLanes(
	producer: Producer,
	lanes: number,
	topic: string,
	partitions: number,
	errors: string[]
): Promise<void> {
	for (let attempt = 1; attempt <= 5; attempt++) {
		const results = await Promise.allSettled(
			Array.from({ length: lanes }, (_, i) =>
				producer.transaction(async tx => {
					await tx.send(topic, { key: 'warmup', value: Buffer.from('warmup'), partition: i % partitions })
				})
			)
		)
		const failed = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected')
		for (const f of failed) errors.push(`lane warmup attempt ${attempt}: ${describeError(f.reason)}`)
		if (failed.length === 0) return
	}
	throw new Error('transactional lanes failed to start after 5 attempts')
}

export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))

export async function connect(lib: Lib, bootstrap: string[], clientId: string): Promise<Client> {
	const client = new lib.KafkaClient({ brokers: bootstrap, clientId, logLevel: 'error' })
	await client.connect()
	return client
}

export function describeError(error: unknown): string {
	const parts: string[] = []
	let current: unknown = error
	for (let depth = 0; depth < 5 && current; depth++) {
		const e = current as { name?: string; message?: string; errorCode?: number; cause?: unknown }
		const code = typeof e.errorCode === 'number' ? `[${e.errorCode}]` : ''
		parts.push(`${e.name ?? 'Error'}${code}: ${e.message ?? String(current)}`)
		current = e.cause
	}
	return parts.join(' <- ')
}

export function isMessageTooLarge(description: string): boolean {
	return /MessageTooLarge|MESSAGE_TOO_LARGE|RecordTooLarge|RecordListTooLarge|\[10\]|\[18\]/.test(description)
}

/**
 * A fixed window that opens on the first delivered batch, so group join and the first fetch are
 * excluded. If nothing arrives by the deadline the window closes anyway and reports why.
 */
export class MeasurementWindow {
	private t0 = -1
	private eluAtOpen: ReturnType<typeof performance.eventLoopUtilization> | null = null
	private timer: ReturnType<typeof setTimeout>
	private readonly onClose: () => void
	timedOutWaitingForData = false
	closed = false
	/** Main-thread busy share while the window was open. */
	eventLoopUtilization: number | null = null

	constructor(
		readonly windowMs: number,
		firstBatchTimeoutMs: number,
		onClose: () => void
	) {
		this.onClose = onClose
		this.timer = setTimeout(() => {
			this.timedOutWaitingForData = true
			this.close()
		}, firstBatchTimeoutMs)
	}

	get started(): boolean {
		return this.t0 >= 0
	}

	/** Milliseconds since the window opened; the first call opens it. */
	mark(): number {
		const now = performance.now()
		if (this.t0 < 0) {
			this.t0 = now
			this.eluAtOpen = performance.eventLoopUtilization()
			clearTimeout(this.timer)
			this.timer = setTimeout(() => this.close(), this.windowMs)
		}
		return now - this.t0
	}

	elapsed(): number {
		return this.t0 < 0 ? 0 : performance.now() - this.t0
	}

	close(): void {
		if (this.closed) return
		this.closed = true
		clearTimeout(this.timer)
		if (this.eluAtOpen) {
			this.eventLoopUtilization =
				Math.round(performance.eventLoopUtilization(this.eluAtOpen).utilization * 1000) / 1000
		}
		this.onClose()
	}
}

export async function settle(promise: Promise<unknown>, timeoutMs: number, errors: string[], label: string) {
	let timer: ReturnType<typeof setTimeout> | undefined
	const timeout = new Promise<'timeout'>(resolve => {
		timer = setTimeout(() => resolve('timeout'), timeoutMs)
	})
	try {
		const result = await Promise.race([promise.then(() => 'ok' as const), timeout])
		if (result === 'timeout') errors.push(`${label}: did not stop within ${timeoutMs} ms`)
	} catch (error) {
		errors.push(`${label}: ${describeError(error)}`)
	} finally {
		clearTimeout(timer)
	}
}

export interface MemorySample {
	t: number
	rss: number
	heapUsed: number
	external: number
	arrayBuffers: number
}

export function sampleMemory(t: number): MemorySample {
	const m = process.memoryUsage()
	return { t, rss: m.rss, heapUsed: m.heapUsed, external: m.external, arrayBuffers: m.arrayBuffers }
}

export function collectGc(): { stop: () => { count: number; totalMs: number; maxMs: number } } {
	let count = 0
	let totalMs = 0
	let maxMs = 0
	const observer = new PerformanceObserver(list => {
		for (const entry of list.getEntries()) {
			count++
			totalMs += entry.duration
			maxMs = Math.max(maxMs, entry.duration)
		}
	})
	observer.observe({ entryTypes: ['gc'] })
	return {
		stop: () => {
			observer.disconnect()
			return { count, totalMs, maxMs }
		},
	}
}
