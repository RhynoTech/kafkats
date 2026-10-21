import { connect, describeError, MeasurementWindow, settle, sleep } from './common.js'
import { mulberry32 } from './rng.js'
import { distribution, gini, round } from './stats.js'
import type { Scenario } from './types.js'

const TOPIC = 'scn.s1.events80.v2'
const PARTITIONS = 80
const RECORDS_PER_PARTITION = 50_000

export const s1Fairness: Scenario = {
	id: 's1',
	name: 'fairness',
	description:
		'80-partition backlog of ~500 B JSON records, runBatch partitionConcurrency 3, 20-70 ms sleep handler. ' +
		'Measures which partitions a fixed fetch order starves.',
	datasets: () => [
		{
			id: 's1-events80-v2',
			topics: [
				{
					name: TOPIC,
					partitions: PARTITIONS,
					recordsPerPartition: RECORDS_PER_PARTITION,
					batchRecords: 30,
					compression: 'none',
					kind: 'json500',
					seed: 101,
				},
			],
		},
	],
	runTopics: () => [],

	async run({ lib, setup, scale, log }) {
		const config = {
			topic: TOPIC,
			partitions: PARTITIONS,
			recordsPerPartition: RECORDS_PER_PARTITION,
			windowMs: Math.round(40_000 * scale),
			maxBytesPerPartition: 512 * 1024,
			partitionConcurrency: 3,
			handlerMs: [20, 70],
			seed: 42,
			checkpointsMs: [10_000, 30_000].map(t => Math.round(t * scale)),
		}
		const errors: string[] = []
		const client = await connect(lib, setup.bootstrap, `scn-s1-${setup.runTag}`)
		const consumer = client.consumer({
			groupId: `scn-s1-${setup.runTag}`,
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: config.maxBytesPerPartition,
		})
		consumer.on('error', (error: Error) => errors.push(`consumer: ${describeError(error)}`))

		const rand = mulberry32(config.seed)
		const batches: Array<{ t: number; p: number; n: number }> = []
		const window = new MeasurementWindow(config.windowMs, 60_000, () => consumer.stop())
		const run = consumer.runBatch(
			[TOPIC],
			async (messages, ctx) => {
				const t = window.mark()
				if (window.closed || t > config.windowMs) return
				batches.push({ t, p: ctx.partition, n: messages.length })
				await sleep(config.handlerMs[0]! + rand() * (config.handlerMs[1]! - config.handlerMs[0]!))
			},
			{ partitionConcurrency: config.partitionConcurrency, autoCommit: false, commitOffsets: false }
		)
		await settle(run, config.windowMs + 120_000, errors, 'runBatch')
		window.close()
		await client.disconnect().catch(() => {})
		if (window.timedOutWaitingForData) errors.push('no batch delivered within 60 s')
		log(`s1: ${batches.length} batches`)

		const W = config.windowMs
		const consumed = Array.from({ length: PARTITIONS }, () => 0)
		const serves: number[][] = Array.from({ length: PARTITIONS }, () => [])
		for (const b of batches) {
			consumed[b.p]! += b.n
			serves[b.p]!.push(b.t)
		}
		const firstServed = serves.map(ts => (ts.length > 0 ? ts[0]! : null))
		const servedBy = (t: number) => firstServed.filter(f => f !== null && f <= t).length
		const maxGap = serves.map((ts, p) => {
			let prev = 0
			let gap = 0
			for (const t of ts) {
				gap = Math.max(gap, t - prev)
				prev = t
			}
			return consumed[p]! < RECORDS_PER_PARTITION ? Math.max(gap, W - prev) : gap
		})
		const total = consumed.reduce((a, b) => a + b, 0)
		const neverServed = consumed.filter(c => c === 0).length

		return {
			config,
			headline: {
				neverServed,
				servedAt10s: servedBy(config.checkpointsMs[0]!),
				servedAt30s: servedBy(config.checkpointsMs[1]!),
				maxGapMs: Math.round(Math.max(...maxGap)),
				minPerPartition: Math.min(...consumed),
				maxPerPartition: Math.max(...consumed),
				gini: round(gini(consumed), 3),
				recPerSec: Math.round((total / W) * 1000),
				meanBatch: batches.length ? Math.round(total / batches.length) : 0,
				drained: consumed.filter(c => c >= RECORDS_PER_PARTITION).length,
				eventLoopUtilization: window.eventLoopUtilization,
			},
			detected: { starvation: neverServed > 0 },
			detail: {
				consumedPerPartition: consumed,
				firstServedMs: firstServed.map(f => (f === null ? null : Math.round(f))),
				maxGapMsPerPartition: maxGap.map(g => Math.round(g)),
				batchSize: distribution(
					batches.map(b => b.n),
					0
				),
				batches: batches.length,
			},
			errors,
		}
	},
}
