import { connect, describeError, MeasurementWindow, settle, sleep, warmTransactionalLanes } from './common.js'
import { userEvent, workspaceOf, WORKSPACES } from './records.js'
import { mulberry32 } from './rng.js'
import { distribution, round } from './stats.js'
import type { Scenario } from './types.js'
import { ApiKeys, installWireProbe } from './wire-probe.js'

const FAN_USERS = 'bobcat-cdc-fans.public.fan_users.scn-v1'
const FANS_TOPICS = [
	FAN_USERS,
	'bobcat-cdc-fans.public.fan_identities.scn-v1',
	'bobcat-cdc-fans.public.fan_memberships.scn-v1',
]
const FAN_USERS_PER_PARTITION = 100_000
const OUTPUT_PARTITIONS = 10
const TIMESTAMP_HEADER = 'scn-sent-at'

const userEventsTopic = (runTag: string) => `bobcat-user-events.${runTag}`
const outputTopic = (runTag: string) => `scn.s3.by-workspace.${runTag}`

export const s3Rekey: Scenario = {
	id: 's3',
	name: 'rekey',
	description:
		'data-export-rekey: fans CDC (3 topics x 3 partitions, 300k fan_users updates backlogged) plus an ' +
		'80-partition user-events stream at 2 rec/s per partition. EOS over 10 lanes, minBytes 1 MiB, ' +
		'maxWaitMs 5000; output keyed by workspace (three hot workspaces) over 10 partitions.',
	datasets: () => [
		{
			id: 's3-fans-v1',
			topics: FANS_TOPICS.map((name, i) => ({
				name,
				partitions: 3,
				recordsPerPartition: name === FAN_USERS ? FAN_USERS_PER_PARTITION : 0,
				batchRecords: 73,
				compression: 'snappy' as const,
				kind: 'fans' as const,
				seed: 301 + i,
			})),
		},
	],
	runTopics: runTag => [
		{ name: userEventsTopic(runTag), partitions: 80 },
		{ name: outputTopic(runTag), partitions: OUTPUT_PARTITIONS },
	],

	async run({ lib, setup, scale, log }) {
		const runTag = setup.runTag
		const ue = userEventsTopic(runTag)
		const out = outputTopic(runTag)
		const config = {
			windowMs: Math.round(45_000 * scale),
			maxBytesPerPartition: 1_048_576,
			minBytes: 1_048_576,
			maxWaitMs: 5000,
			lanes: 10,
			costMs: { fans: { fixed: 78, perRecord: 0.15 }, userEvents: { fixed: 72, perRecord: 0.014 } },
			userEventsPartitions: 80,
			userEventsPerSecPerPartition: 2,
			outputPartitions: OUTPUT_PARTITIONS,
			outputPartitioning: 'workspace index mod 10, so the three hot workspaces get their own partitions',
		}
		const errors: string[] = []
		const probe = installWireProbe()
		const client = await connect(lib, setup.bootstrap, `scn-s3-${runTag}`)
		const streamClient = await connect(lib, setup.bootstrap, `scn-s3-stream-${runTag}`)
		let encodedMax = 0
		let encodedBatches = 0
		const producer = client.producer({
			transactionalId: `scn-s3-${runTag}`,
			transactionConcurrency: config.lanes,
			compression: 'snappy',
			trace: event => {
				if (event.stage === 'encode_batch' && event.topic === out && event.bytes !== undefined) {
					encodedBatches++
					encodedMax = Math.max(encodedMax, event.bytes)
				}
			},
		})
		await warmTransactionalLanes(producer, config.lanes, out, OUTPUT_PARTITIONS, errors)
		const streamProducer = streamClient.producer({ lingerMs: 0, compression: 'snappy' })
		const consumer = client.consumer({
			groupId: `scn-s3-${runTag}`,
			autoOffsetReset: 'earliest',
			maxBytesPerPartition: config.maxBytesPerPartition,
			minBytes: config.minBytes,
			maxWaitMs: config.maxWaitMs,
		})
		consumer.on('error', (error: Error) => errors.push(`consumer: ${describeError(error)}`))

		const batches: Array<{ t: number; topic: string; partition: number; n: number }> = []
		const latencies: number[] = []
		const outputPerPartitionPerSend: number[] = []
		let sent = 0
		let sendErrors = 0
		let txCommitted = 0
		const txOverheadMs: number[] = []
		let txFailed = 0
		let streamTimer: ReturnType<typeof setInterval> | undefined
		const rng = mulberry32(303)
		const startStream = () => {
			streamTimer = setInterval(() => {
				for (let p = 0; p < config.userEventsPartitions; p++) {
					const r = userEvent(rng)
					sent++
					streamProducer
						.send(ue, {
							key: r.key,
							value: r.value,
							partition: p,
							headers: { [TIMESTAMP_HEADER]: String(Date.now()) },
						})
						.catch(() => sendErrors++)
				}
			}, 1000 / config.userEventsPerSecPerPartition)
		}
		const fetches = { atOpen: 0, atClose: 0 }
		const window = new MeasurementWindow(config.windowMs, 90_000, () => {
			fetches.atClose = probe.count(ApiKeys.Fetch)
			clearInterval(streamTimer)
			consumer.stop()
		})

		const run = consumer.runBatch(
			[...FANS_TOPICS, ue],
			async (messages, ctx) => {
				const receivedAt = Date.now()
				const first = !window.started
				const t = window.mark()
				if (first) {
					fetches.atOpen = probe.count(ApiKeys.Fetch)
					startStream()
				}
				if (window.closed) return
				const isStream = ctx.topic === ue
				if (isStream) {
					for (const m of messages) {
						const sentAt = m.headers[TIMESTAMP_HEADER]
						if (sentAt) latencies.push(receivedAt - Number(sentAt.toString()))
					}
				}
				batches.push({ t, topic: ctx.topic, partition: ctx.partition, n: messages.length })
				const bytesByPartition = new Map<number, number>()
				const outputs = messages.map(m => {
					const ws = workspaceOf(isStream ? m.value : m.key)
					const partition = ws % OUTPUT_PARTITIONS
					const headers = {
						'source-topic': ctx.topic,
						'source-partition': String(ctx.partition),
						'source-offset': String(m.offset),
					}
					let bytes = 36 + (m.value?.length ?? 0)
					for (const [k, v] of Object.entries(headers)) bytes += k.length + v.length
					bytesByPartition.set(partition, (bytesByPartition.get(partition) ?? 0) + bytes)
					return { key: WORKSPACES[ws]!, value: m.value, partition, headers }
				})
				for (const bytes of bytesByPartition.values()) outputPerPartitionPerSend.push(bytes)
				const cost = isStream ? config.costMs.userEvents : config.costMs.fans
				const sleepMs = cost.fixed + cost.perRecord * messages.length
				const txStart = performance.now()
				try {
					await producer.transaction(async tx => {
						await sleep(sleepMs)
						await tx.send(out, outputs)
						await tx.sendOffsets(ctx)
					})
					txCommitted++
					txOverheadMs.push(performance.now() - txStart - sleepMs)
				} catch (error) {
					txFailed++
					if (errors.length < 20) errors.push(`transaction: ${describeError(error)}`)
				}
			},
			{ partitionConcurrency: config.lanes, autoCommit: false, commitOffsets: false }
		)
		await settle(run, config.windowMs + 120_000, errors, 'runBatch')
		window.close()
		if (window.timedOutWaitingForData) errors.push('no batch delivered within 90 s')
		await producer.disconnect().catch(() => {})
		await streamProducer.disconnect().catch(() => {})
		await Promise.all([client.disconnect().catch(() => {}), streamClient.disconnect().catch(() => {})])

		const W = config.windowMs
		const fans = batches.filter(b => b.topic === FAN_USERS)
		const fansPerPartition = [0, 1, 2].map(p => fans.filter(b => b.partition === p).reduce((a, b) => a + b.n, 0))
		const fansRecords = fans.reduce((a, b) => a + b.n, 0)
		const streamBatches = batches.filter(b => b.topic === ue)
		const latency = distribution(latencies, 0)
		const outKiB = distribution(
			outputPerPartitionPerSend.map(b => b / 1024),
			1
		)
		log(`s3: fans ${fansRecords}, user-events ${latencies.length}/${sent}`)

		return {
			config,
			headline: {
				fansRecPerSec: Math.round((fansRecords / W) * 1000),
				fansMeanBatch: fans.length ? Math.round(fansRecords / fans.length) : 0,
				fansMinPartitionRecords: Math.min(...fansPerPartition),
				fansMaxPartitionRecords: Math.max(...fansPerPartition),
				userEventsSent: sent,
				userEventsDelivered: latencies.length,
				userEventsP50Ms: latency.p50,
				userEventsP99Ms: latency.p99,
				userEventsPartitionsServed: new Set(streamBatches.map(b => b.partition)).size,
				outMaxPartitionSendKiB: outKiB.max,
				outMaxEncodedBatchKiB: encodedBatches ? round(encodedMax / 1024, 1) : null,
				txCommitted,
				txFailed,
				txOverheadP50Ms: distribution(txOverheadMs, 0).p50,
				eventLoopUtilization: window.eventLoopUtilization,
				fetchesPerSec: round(((fetches.atClose - fetches.atOpen) / W) * 1000, 1),
			},
			detected: {
				userEventsStarved: latencies.length < sent * 0.5,
				laneWarmupFailed: errors.some(e => e.startsWith('lane warmup')),
			},
			detail: {
				transactionOverheadMs: distribution(txOverheadMs, 1),
				fansBatches: fans.length,
				fansRecordsPerPartition: fansPerPartition,
				fansBatchSize: distribution(
					fans.map(b => b.n),
					0
				),
				userEventsLatencyMs: latency,
				userEventsBatches: streamBatches.length,
				outputPerPartitionPerSendKiB: outKiB,
				encodedOutputBatches: encodedBatches,
				sendErrors,
			},
			errors,
		}
	},
}
