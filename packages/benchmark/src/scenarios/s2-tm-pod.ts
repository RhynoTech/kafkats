import { connect, describeError, MeasurementWindow, settle, sleep, warmTransactionalLanes } from './common.js'
import { tmCustomer, tmUserId } from './records.js'
import { mulberry32 } from './rng.js'
import { distribution, round } from './stats.js'
import type { Scenario } from './types.js'
import { ApiKeys, installWireProbe } from './wire-probe.js'

const CUSTOMERS = 'ticketmaster.public.ticketmaster_customers.scn-v1'
const CUSTOMERS_RECORDS = 120_000
// One ticketmaster-cdc pod of five under the range assignor: one customers partition plus its
// share of the other topics (10-partition topics give it two each); attendances has one partition.
const CO_LOCATED: ReadonlyArray<[table: string, partitions: number]> = [
	['attendances', 1],
	['events', 2],
	['manifests', 1],
	['plans', 2],
	['price_codes', 1],
	['seasons', 1],
	['tickets', 2],
]

const trickleTopic = (table: string, runTag: string) => `ticketmaster.public.ticketmaster_${table}.${runTag}`
const outputTopic = (runTag: string) => `scn.s2.user-unidentified-events.${runTag}`
const TIMESTAMP_HEADER = 'scn-sent-at'

export const s2TmPod: Scenario = {
	id: 's2',
	name: 'tm-pod',
	description:
		'Ticketmaster pod: 1 backlogged customers partition (schemaless Debezium, 7-record snappy batches) ' +
		'first in the subscription, 10 co-located partitions of 7 topics receiving 20 rec/s each. EOS handler ' +
		'sleeps 93 ms + 0.15 ms/record and commits outputs keyed by userID to 80 partitions.',
	datasets: () => [
		{
			id: 's2-tm-customers-v1',
			topics: [
				{
					name: CUSTOMERS,
					partitions: 1,
					recordsPerPartition: CUSTOMERS_RECORDS,
					batchRecords: 7,
					compression: 'snappy',
					kind: 'tm',
					seed: 201,
				},
			],
		},
	],
	runTopics: runTag => [
		...CO_LOCATED.map(([table, partitions]) => ({
			name: trickleTopic(table, runTag),
			partitions,
			colocateWith: { topic: CUSTOMERS, partition: 0 },
		})),
		{ name: outputTopic(runTag), partitions: 80 },
	],

	async run({ lib, setup, scale, log }) {
		const runTag = setup.runTag
		const out = outputTopic(runTag)
		const trickleTopics = CO_LOCATED.map(([table, partitions]) => ({
			name: trickleTopic(table, runTag),
			partitions,
		}))
		const tricklePartitions = trickleTopics.reduce((a, t) => a + t.partitions, 0)
		const config = {
			windowMs: Math.round(40_000 * scale),
			maxBytesPerPartition: 1_048_576,
			minBytes: 10_240,
			maxWaitMs: 100,
			partitionAssignmentStrategy: 'range',
			partitionConcurrency: 8,
			transactionConcurrency: 8,
			costMs: { fixed: 93, perRecord: 0.15 },
			trickleRecordsPerSecPerPartition: 20,
			tricklePartitions,
			outputPartitions: 80,
			leaders: { customers: setup.leaders[CUSTOMERS], trickle: setup.leaders[trickleTopics[0]!.name] },
		}
		const errors: string[] = []
		const probe = installWireProbe()
		const client = await connect(lib, setup.bootstrap, `scn-s2-${runTag}`)
		const trickleClient = await connect(lib, setup.bootstrap, `scn-s2-trickle-${runTag}`)
		const producer = client.producer({
			transactionalId: `scn-s2-${runTag}`,
			transactionConcurrency: config.transactionConcurrency,
			compression: 'snappy',
		})
		await warmTransactionalLanes(producer, config.transactionConcurrency, out, config.outputPartitions, errors)
		const trickleProducer = trickleClient.producer({ lingerMs: 0, compression: 'snappy' })

		const consumer = client.consumer({
			groupId: `scn-s2-${runTag}`,
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: config.maxBytesPerPartition,
			minBytes: config.minBytes,
			maxWaitMs: config.maxWaitMs,
			partitionAssignmentStrategy: 'range',
		})
		consumer.on('error', (error: Error) => errors.push(`consumer: ${describeError(error)}`))

		const batches: Array<{ t: number; topic: string; partition: number; n: number; committed: boolean }> = []
		const latencies: number[] = []
		const deliveredByTopic: Record<string, number> = {}
		let trickleSent = 0
		let trickleSendErrors = 0
		let txCommitted = 0
		const txOverheadMs: number[] = []
		let txFailed = 0
		let trickleTimer: ReturnType<typeof setInterval> | undefined
		const trickleRng = mulberry32(202)
		let trickleSeq = 0

		const startTrickle = () => {
			trickleTimer = setInterval(() => {
				for (const t of trickleTopics) {
					for (let p = 0; p < t.partitions; p++) {
						const r = tmCustomer(trickleRng, trickleSeq++)
						trickleSent++
						trickleProducer
							.send(t.name, {
								key: r.key,
								value: r.value,
								partition: p,
								headers: { [TIMESTAMP_HEADER]: String(Date.now()) },
							})
							.catch(() => trickleSendErrors++)
					}
				}
			}, 1000 / config.trickleRecordsPerSecPerPartition)
		}
		const fetches = { atOpen: 0, atClose: 0 }
		const window = new MeasurementWindow(config.windowMs, 90_000, () => {
			fetches.atClose = probe.count(ApiKeys.Fetch)
			clearInterval(trickleTimer)
			consumer.stop()
		})

		const run = consumer.runBatch(
			[CUSTOMERS, ...trickleTopics.map(t => t.name)],
			async (messages, ctx) => {
				const receivedAt = Date.now()
				const first = !window.started
				const t = window.mark()
				if (first) {
					fetches.atOpen = probe.count(ApiKeys.Fetch)
					startTrickle()
				}
				if (window.closed) return
				if (ctx.topic !== CUSTOMERS) {
					for (const m of messages) {
						const sentAt = m.headers[TIMESTAMP_HEADER]
						if (sentAt) latencies.push(receivedAt - Number(sentAt.toString()))
					}
					deliveredByTopic[ctx.topic] = (deliveredByTopic[ctx.topic] ?? 0) + messages.length
				}
				const batch = { t, topic: ctx.topic, partition: ctx.partition, n: messages.length, committed: false }
				batches.push(batch)
				const outputs = messages.map(m => ({ key: tmUserId(m.key), value: m.value }))
				const sleepMs = config.costMs.fixed + config.costMs.perRecord * messages.length
				const txStart = performance.now()
				try {
					await producer.transaction(async tx => {
						await sleep(sleepMs)
						await tx.send(out, outputs)
						await tx.sendOffsets(ctx)
					})
					batch.committed = true
					txCommitted++
					txOverheadMs.push(performance.now() - txStart - sleepMs)
				} catch (error) {
					txFailed++
					if (errors.length < 20) errors.push(`transaction: ${describeError(error)}`)
				}
			},
			{ partitionConcurrency: config.partitionConcurrency, autoCommit: false, commitOffsets: false }
		)
		await settle(run, config.windowMs + 120_000, errors, 'runBatch')
		window.close()
		if (window.timedOutWaitingForData) errors.push('no batch delivered within 90 s')
		await producer.disconnect().catch(() => {})
		await trickleProducer.disconnect().catch(() => {})
		await Promise.all([client.disconnect().catch(() => {}), trickleClient.disconnect().catch(() => {})])

		const W = config.windowMs
		const customers = batches.filter(b => b.topic === CUSTOMERS)
		const customerRecords = customers.reduce((a, b) => a + b.n, 0)
		const trickleBatches = batches.filter(b => b.topic !== CUSTOMERS)
		const served = new Set(trickleBatches.map(b => `${b.topic}-${b.partition}`))
		const latency = distribution(latencies, 0)
		log(`s2: customers ${customerRecords}, trickle ${latencies.length}/${trickleSent}`)

		return {
			config,
			headline: {
				customersRecPerSec: Math.round((customerRecords / W) * 1000),
				customersMeanBatch: customers.length ? Math.round(customerRecords / customers.length) : 0,
				trickleSent,
				trickleDelivered: latencies.length,
				trickleP50Ms: latency.p50,
				trickleP99Ms: latency.p99,
				tricklePartitionsServed: served.size,
				txCommitted,
				txFailed,
				txOverheadP50Ms: distribution(txOverheadMs, 0).p50,
				eventLoopUtilization: window.eventLoopUtilization,
				fetchesPerSec: round(((fetches.atClose - fetches.atOpen) / W) * 1000, 1),
			},
			detected: {
				trickleStarved: latencies.length < trickleSent * 0.5,
				laneWarmupFailed: errors.some(e => e.startsWith('lane warmup')),
			},
			detail: {
				transactionOverheadMs: distribution(txOverheadMs, 1),
				customersBatches: customers.length,
				customersCommittedRecords: customers.filter(b => b.committed).reduce((a, b) => a + b.n, 0),
				customersBatchSize: distribution(
					customers.map(b => b.n),
					0
				),
				trickleLatencyMs: latency,
				trickleDeliveredByTopic: deliveredByTopic,
				trickleSendErrors,
				trickleBatches: trickleBatches.length,
				tricklePartitions,
				deliveredShare: trickleSent ? round(latencies.length / trickleSent, 3) : null,
			},
			errors,
		}
	},
}
