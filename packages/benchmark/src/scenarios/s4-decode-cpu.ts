import { monitorEventLoopDelay, performance } from 'node:perf_hooks'

import { collectGc, connect, describeError, settle } from './common.js'
import { distribution, median, MiB, round } from './stats.js'
import { TM40_TOPICS, tm40Dataset } from './tm40.js'
import type { Headline, Scenario } from './types.js'
import { ApiKeys, installWireProbe } from './wire-probe.js'

const PASSES = 3
const HEARTBEAT_INTERVAL_MS = 200
const LOOP_DELAY_RESOLUTION_MS = 1

// The histogram records whole sampling intervals; what is left after one interval is lateness.
const loopDelay = (ns: number) => round(Math.max(0, ns / 1e6 - LOOP_DELAY_RESOLUTION_MS), 1)

interface Snapshot {
	t: number
	thread: NodeJS.CpuUsage
	process: NodeJS.CpuUsage
	elu: ReturnType<typeof performance.eventLoopUtilization>
	rx: number
}

interface Marks {
	start: Snapshot | null
	end: Snapshot | null
	elu: number | null
	gc: ReturnType<typeof collectGc> | null
	gcStats: ReturnType<ReturnType<typeof collectGc>['stop']> | null
}

function snapshot(t: number, rx: number): Snapshot {
	return {
		t,
		thread: process.threadCpuUsage(),
		process: process.cpuUsage(),
		elu: performance.eventLoopUtilization(),
		rx,
	}
}

export const s4DecodeCpu: Scenario = {
	id: 's4',
	name: 'decode-cpu',
	description:
		'Drain a ~200 MiB-wire backlog of schemaless customers records (40 partitions, 7-record snappy ' +
		'batches) with a no-op handler; main-thread CPU, event-loop delay and heartbeat lateness per pass.',
	datasets: () => [tm40Dataset],
	runTopics: () => [],

	async run({ lib, setup, log }) {
		const manifest = setup.datasets[tm40Dataset.id]!
		const totalRecords = manifest.topics.reduce((a, t) => a + t.records, 0)
		const wireBytes = manifest.topics.reduce((a, t) => a + t.wireBytes, 0)
		const config = {
			passes: PASSES,
			maxBytesPerPartition: 1_048_576,
			partitionConcurrency: 8,
			heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
			records: totalRecords,
			wireMiB: round(wireBytes / MiB, 1),
			decodedPerWire: round(manifest.topics.reduce((a, t) => a + t.keyValueBytes, 0) / wireBytes, 2),
		}
		const errors: string[] = []
		const probe = installWireProbe([ApiKeys.Heartbeat])
		const passes: Array<Record<string, number | null>> = []

		for (let pass = 0; pass < PASSES; pass++) {
			// A client per pass, so a stopped consumer's long-poll Fetch cannot hold up the next pass.
			const client = await connect(lib, setup.bootstrap, `scn-s4-${setup.runTag}-${pass}`)
			;(globalThis as { gc?: () => void }).gc?.()
			const consumer = client.consumer({
				groupId: `scn-s4-${setup.runTag}-${pass}`,
				autoOffsetReset: 'earliest',
				maxBytesPerPartition: config.maxBytesPerPartition,
				heartbeatIntervalMs: HEARTBEAT_INTERVAL_MS,
			})
			consumer.on('error', (error: Error) => errors.push(`consumer: ${describeError(error)}`))

			const mark: Marks = { start: null, end: null, elu: null, gc: null, gcStats: null }
			const delay = monitorEventLoopDelay({ resolution: LOOP_DELAY_RESOLUTION_MS })
			// Fetching starts after the group join; the clock starts with the first Fetch on the wire.
			probe.onRequest(event => {
				if (mark.start || event.apiKey !== ApiKeys.Fetch) return
				mark.start = snapshot(event.t, probe.rxBytes)
				delay.enable()
				mark.gc = collectGc()
			})
			let records = 0
			let decoded = 0
			const heartbeatsBefore = probe.events.length
			const failsafe = setTimeout(() => consumer.stop(), 180_000)
			const run = consumer.runBatch(
				TM40_TOPICS.map(t => t.name),
				async messages => {
					for (const m of messages) decoded += (m.key?.length ?? 0) + (m.value?.length ?? 0)
					records += messages.length
					if (records >= totalRecords && !mark.end) {
						mark.end = snapshot(performance.now(), probe.rxBytes)
						if (mark.start)
							mark.elu = performance.eventLoopUtilization(mark.end.elu, mark.start.elu).utilization
						delay.disable()
						mark.gcStats = mark.gc?.stop() ?? null
						consumer.stop()
					}
				},
				{ partitionConcurrency: config.partitionConcurrency, autoCommit: false, commitOffsets: false }
			)
			await settle(run, 240_000, errors, `pass ${pass}`)
			clearTimeout(failsafe)
			await client.disconnect().catch(() => {})
			const gcStats = mark.gcStats
			const s = mark.start
			const e = mark.end
			if (!s || !e) {
				errors.push(`pass ${pass}: consumed ${records}/${totalRecords} records`)
				continue
			}
			const ms = e.t - s.t
			const wireMiB = wireBytes / MiB
			const threadCpuMs = (e.thread.user + e.thread.system - s.thread.user - s.thread.system) / 1000
			const processCpuMs = (e.process.user + e.process.system - s.process.user - s.process.system) / 1000
			const beats = probe.events.slice(heartbeatsBefore).filter(x => x.t >= s.t && x.t <= e.t)
			const lateness = beats.slice(1).map((b, i) => b.t - beats[i]!.t - HEARTBEAT_INTERVAL_MS)
			const late = distribution(lateness, 1)
			const result = {
				elapsedMs: Math.round(ms),
				wireMiBps: round(wireMiB / (ms / 1000), 1),
				rxMiBps: round((e.rx - s.rx) / MiB / (ms / 1000), 1),
				decodedMiBps: round(decoded / MiB / (ms / 1000), 1),
				recPerSec: Math.round(records / (ms / 1000)),
				mainCpuMsPerWireMiB: round(threadCpuMs / wireMiB, 2),
				processCpuMsPerWireMiB: round(processCpuMs / wireMiB, 2),
				eventLoopUtilization: mark.elu === null ? null : round(mark.elu, 3),
				loopDelayP50Ms: loopDelay(delay.percentile(50)),
				loopDelayP99Ms: loopDelay(delay.percentile(99)),
				loopDelayMaxMs: loopDelay(delay.max),
				heartbeatSamples: lateness.length,
				heartbeatLateP50Ms: late.p50,
				heartbeatLateMaxMs: late.max,
				gcMs: gcStats ? round(gcStats.totalMs, 1) : null,
				gcMaxPauseMs: gcStats ? round(gcStats.maxMs, 1) : null,
			}
			passes.push(result)
			log(`s4 pass ${pass}: ${JSON.stringify(result)}`)
		}
		const headline: Headline = {}
		for (const key of Object.keys(passes[0] ?? {})) {
			const values = passes.map(p => p[key]).filter((v): v is number => typeof v === 'number')
			const m = median(values)
			headline[key] = m === null ? null : round(m, 2)
		}
		return {
			config,
			headline,
			detected: {},
			detail: { passes },
			errors,
		}
	},
}
