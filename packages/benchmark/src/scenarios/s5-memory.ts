import { connect, describeError, MeasurementWindow, sampleMemory, settle, sleep, type MemorySample } from './common.js'
import { MiB, round } from './stats.js'
import { TM40_TOPICS, tm40Dataset } from './tm40.js'
import type { Scenario } from './types.js'

export const s5Memory: Scenario = {
	id: 's5',
	name: 'memory',
	description:
		'All 40 Ticketmaster partitions backlogged at start, sleep handler (93 ms + 0.15 ms/record); RSS, ' +
		'heapUsed and external sampled every 100 ms, peaks reported against the 10 x maxBytesPerPartition buffer.',
	datasets: () => [tm40Dataset],
	runTopics: () => [],

	async run({ lib, setup, scale, log }) {
		const config = {
			windowMs: Math.round(20_000 * scale),
			maxBytesPerPartition: 1_048_576,
			bufferCapBytes: 10 * 1_048_576,
			partitionConcurrency: 8,
			costMs: { fixed: 93, perRecord: 0.15 },
			sampleEveryMs: 100,
		}
		const errors: string[] = []
		const client = await connect(lib, setup.bootstrap, `scn-s5-${setup.runTag}`)
		;(globalThis as { gc?: () => void }).gc?.()
		await sleep(500)
		const baseline = sampleMemory(0)

		const consumer = client.consumer({
			groupId: `scn-s5-${setup.runTag}`,
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: config.maxBytesPerPartition,
		})
		consumer.on('error', (error: Error) => errors.push(`consumer: ${describeError(error)}`))
		const t0 = performance.now()
		const samples: MemorySample[] = []
		const sampler = setInterval(() => samples.push(sampleMemory(performance.now() - t0)), config.sampleEveryMs)
		let records = 0
		const served = new Set<string>()
		const window = new MeasurementWindow(config.windowMs, 90_000, () => consumer.stop())
		const run = consumer.runBatch(
			TM40_TOPICS.map(t => t.name),
			async (messages, ctx) => {
				window.mark()
				if (window.closed) return
				records += messages.length
				served.add(`${ctx.topic}-${ctx.partition}`)
				await sleep(config.costMs.fixed + config.costMs.perRecord * messages.length)
			},
			{ partitionConcurrency: config.partitionConcurrency, autoCommit: false, commitOffsets: false }
		)
		await settle(run, config.windowMs + 120_000, errors, 'runBatch')
		window.close()
		clearInterval(sampler)
		await client.disconnect().catch(() => {})
		if (window.timedOutWaitingForData) errors.push('no batch delivered within 90 s')

		const peak = (k: keyof MemorySample) => Math.max(...samples.map(s => s[k]))
		const deltaMiB = (k: keyof MemorySample) => round((peak(k) - baseline[k]) / MiB, 1)
		const overCap = (k: keyof MemorySample) => round((peak(k) - baseline[k]) / config.bufferCapBytes, 2)
		const peakAt = (k: keyof MemorySample) => {
			const max = peak(k)
			return Math.round(samples.find(s => s[k] === max)?.t ?? 0)
		}
		log(`s5: ${samples.length} samples, peak rss ${round(peak('rss') / MiB, 1)} MiB`)

		return {
			config,
			headline: {
				peakRssMiB: round(peak('rss') / MiB, 1),
				rssDeltaMiB: deltaMiB('rss'),
				heapUsedDeltaMiB: deltaMiB('heapUsed'),
				externalDeltaMiB: deltaMiB('external'),
				arrayBuffersDeltaMiB: deltaMiB('arrayBuffers'),
				rssDeltaOverBuffer: overCap('rss'),
				heapPlusExternalOverBuffer: round(
					(Math.max(...samples.map(s => s.heapUsed + s.external)) - baseline.heapUsed - baseline.external) /
						config.bufferCapBytes,
					2
				),
				recPerSec: Math.round((records / config.windowMs) * 1000),
				partitionsServed: served.size,
				eventLoopUtilization: window.eventLoopUtilization,
			},
			detected: {},
			detail: {
				baselineMiB: {
					rss: round(baseline.rss / MiB, 1),
					heapUsed: round(baseline.heapUsed / MiB, 1),
					external: round(baseline.external / MiB, 1),
					arrayBuffers: round(baseline.arrayBuffers / MiB, 1),
				},
				peakAtMs: { rss: peakAt('rss'), heapUsed: peakAt('heapUsed'), external: peakAt('external') },
				externalDeltaOverBuffer: overCap('external'),
				heapUsedDeltaOverBuffer: overCap('heapUsed'),
				samples: samples.map(s => [
					Math.round(s.t),
					round(s.rss / MiB, 1),
					round(s.heapUsed / MiB, 1),
					round(s.external / MiB, 1),
				]),
				sampleColumns: ['tMs', 'rssMiB', 'heapUsedMiB', 'externalMiB'],
				records,
			},
			errors,
		}
	},
}
