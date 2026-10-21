import { connect, describeError, isMessageTooLarge, settle, sleep } from './common.js'
import { tmCustomer } from './records.js'
import { mulberry32, randomBytes } from './rng.js'
import { round } from './stats.js'
import type { Scenario } from './types.js'
import { ApiKeys, installWireProbe } from './wire-probe.js'

const SIZES_MIB = [0.5, 1.5, 3, 6]
const KINDS = ['json', 'random'] as const
const REPS = 2
const PARTITION = 1
const RANDOM_VALUE_BYTES = 3000

const defaultTopic = (runTag: string) => `scn.s6.default.${runTag}`
const lowTopic = (runTag: string) => `scn.s6.low.${runTag}`

type Kind = (typeof KINDS)[number]
interface Message {
	key: Buffer
	value: Buffer
	partition: number
	headers: Record<string, string>
}

interface CaseResult {
	case: string
	topic: 'default' | 'low'
	kind: Kind | 'health'
	sizeMiB: number
	records: number
	rawBytes: number
	ok: boolean
	error?: string
	messageTooLarge?: boolean
	ms: number
	produceRequests: number
	produceRequestBytes: number[]
	encodedBatches: number
	encodedBytes: number
}

export const s6ProducerSplit: Scenario = {
	id: 's6',
	name: 'producer-split',
	description:
		'One transactional tx.send of 0.5/1.5/3/6 MiB to a single partition, for ~3.5x-compressible JSON ' +
		'(Ticketmaster customers) and random bytes, on a default topic and on max.message.bytes=100000; ' +
		'then a read_committed consumer checks every committed record arrives once and in order.',
	datasets: () => [],
	runTopics: runTag => [
		{ name: defaultTopic(runTag), partitions: 3 },
		{ name: lowTopic(runTag), partitions: 3, configs: { 'max.message.bytes': '100000' } },
	],

	async run({ lib, setup, log }) {
		const runTag = setup.runTag
		const topics = { default: defaultTopic(runTag), low: lowTopic(runTag) }
		const config = { sizesMiB: SIZES_MIB, kinds: KINDS, reps: REPS, partition: PARTITION, compression: 'snappy' }
		const errors: string[] = []
		const probe = installWireProbe([ApiKeys.Produce])
		const client = await connect(lib, setup.bootstrap, `scn-s6-${runTag}`)
		const traces: Array<{ topic?: string; bytes?: number }> = []
		const producer = client.producer({
			transactionalId: `scn-s6-${runTag}`,
			compression: 'snappy',
			trace: event => {
				if (event.stage === 'encode_batch') traces.push({ topic: event.topic, bytes: event.bytes })
			},
		})
		const jsonPool = (() => {
			const rng = mulberry32(601)
			return Array.from({ length: 2500 }, (_, i) => tmCustomer(rng, i))
		})()
		const randomRng = mulberry32(602)
		const build = (kind: Kind, id: string, bytes: number): Message[] => {
			const out: Message[] = []
			for (let i = 0, total = 0; total < bytes; i++) {
				const r =
					kind === 'json'
						? jsonPool[i % jsonPool.length]!
						: { key: Buffer.from(`random-${i}`), value: randomBytes(randomRng, RANDOM_VALUE_BYTES) }
				out.push({ key: r.key, value: r.value, partition: PARTITION, headers: { case: id, seq: String(i) } })
				total += r.key.length + r.value.length
			}
			return out
		}

		const results: CaseResult[] = []
		const runCase = async (
			id: string,
			topicKey: 'default' | 'low',
			kind: Kind | 'health',
			sizeMiB: number,
			messages: Message[]
		) => {
			const topic = topics[topicKey]
			const requestsBefore = probe.events.length
			const tracesBefore = traces.length
			const t0 = performance.now()
			const r: CaseResult = {
				case: id,
				topic: topicKey,
				kind,
				sizeMiB,
				records: messages.length,
				rawBytes: messages.reduce((a, m) => a + m.key.length + m.value.length, 0),
				ok: false,
				ms: 0,
				produceRequests: 0,
				produceRequestBytes: [],
				encodedBatches: 0,
				encodedBytes: 0,
			}
			try {
				await producer.transaction(async tx => {
					await tx.send(topic, messages)
					await tx.sendOffsets({
						groupId: `scn-s6-offsets-${runTag}`,
						offsets: [{ topic: topics.default, partition: 0, offset: BigInt(results.length + 1) }],
					})
				})
				r.ok = true
			} catch (error) {
				r.error = describeError(error)
				r.messageTooLarge = isMessageTooLarge(r.error)
			}
			r.ms = round(performance.now() - t0, 1)
			await sleep(200)
			// Health checks are single small records; only data-sized requests are attributed to the case.
			const produce = probe.events.slice(requestsBefore).filter(e => e.apiKey === ApiKeys.Produce)
			r.produceRequests = produce.length
			r.produceRequestBytes = produce.map(e => e.bytes)
			const encoded = traces.slice(tracesBefore).filter(t => t.topic === topic)
			r.encodedBatches = encoded.length
			r.encodedBytes = encoded.reduce((a, t) => a + (t.bytes ?? 0), 0)
			results.push(r)
			log(
				`s6 ${id.padEnd(24)} ${r.records} recs ${(r.rawBytes / 1048576).toFixed(2)} MiB -> ` +
					`${r.ok ? 'ok' : 'FAIL'} ${r.ms} ms, ${r.produceRequests} produce, ${r.encodedBatches} batches` +
					`${r.error ? ` ${r.error.split(' <- ')[0]}` : ''}`
			)
			return r
		}

		for (let i = 0; i < 10; i++) {
			const warm = await runCase(`warmup-${i}`, 'default', 'health', 0, build('json', `warmup-${i}`, 1))
			if (warm.ok) break
			await sleep(1000)
		}
		for (const topicKey of ['default', 'low'] as const) {
			for (const kind of KINDS) {
				for (const sizeMiB of SIZES_MIB) {
					for (let rep = 0; rep < REPS; rep++) {
						const id = `${topicKey}-${kind}-${sizeMiB}-r${rep}`
						const r = await runCase(id, topicKey, kind, sizeMiB, build(kind, id, sizeMiB * 1048576))
						if (!r.ok) {
							const h = await runCase(
								`health-${id}`,
								'default',
								'health',
								0,
								build('json', `health-${id}`, 1)
							)
							if (!h.ok) errors.push(`producer unhealthy after ${id}: ${h.error}`)
						}
					}
				}
			}
		}
		await producer.disconnect().catch(() => {})

		// read_committed verification over both topics
		const seen = new Map<string, { n: number; lastSeq: number; outOfOrder: number; duplicates: number }>()
		const expectedTotal = results.filter(r => r.ok).reduce((a, r) => a + r.records, 0)
		let total = 0
		let idle: ReturnType<typeof setTimeout> | undefined
		const consumer = client.consumer({
			groupId: `scn-s6-verify-${runTag}`,
			autoOffsetReset: 'earliest',
			isolationLevel: 'read_committed',
		})
		consumer.on('error', (error: Error) => errors.push(`verify consumer: ${describeError(error)}`))
		const arm = (ms: number) => {
			clearTimeout(idle)
			idle = setTimeout(() => consumer.stop(), ms)
		}
		arm(30_000)
		const verify = consumer.runBatch(
			[topics.default, topics.low],
			async messages => {
				for (const m of messages) {
					const id = m.headers.case?.toString() ?? '?'
					const seq = Number(m.headers.seq?.toString() ?? -1)
					const s = seen.get(id) ?? { n: 0, lastSeq: -1, outOfOrder: 0, duplicates: 0 }
					if (seq === s.lastSeq) s.duplicates++
					else if (seq < s.lastSeq) s.outOfOrder++
					s.lastSeq = seq
					s.n++
					seen.set(id, s)
					total++
				}
				arm(total >= expectedTotal ? 2000 : 10_000)
			},
			{ autoCommit: false, commitOffsets: false }
		)
		await settle(verify, 120_000, errors, 'verify')
		clearTimeout(idle)
		await client.disconnect().catch(() => {})

		const mismatches = results
			.map(r => {
				const s = seen.get(r.case)
				const expected = r.ok ? r.records : 0
				return {
					case: r.case,
					expected,
					seen: s?.n ?? 0,
					outOfOrder: s?.outOfOrder ?? 0,
					duplicates: s?.duplicates ?? 0,
				}
			})
			.filter(v => v.seen !== v.expected || v.outOfOrder > 0 || v.duplicates > 0)
		const data = results.filter(r => r.kind !== 'health')
		const grid: Record<string, string> = {}
		for (const topicKey of ['default', 'low'] as const) {
			for (const kind of KINDS) {
				grid[`${kind}-${topicKey}`] = SIZES_MIB.map(size => {
					const rs = data.filter(r => r.topic === topicKey && r.kind === kind && r.sizeMiB === size)
					const ok = rs.filter(r => r.ok).length
					const status = ok === rs.length ? 'ok' : rs.some(r => r.messageTooLarge) ? 'MTL' : 'fail'
					return `${size}:${status}/${Math.max(...rs.map(r => r.produceRequests))}`
				}).join(' ')
			}
		}
		const jsonRatio = data
			.filter(r => r.kind === 'json' && r.encodedBytes > 0)
			.map(r => r.rawBytes / r.encodedBytes)

		return {
			config,
			headline: {
				...grid,
				okCases: data.filter(r => r.ok).length,
				failedCases: data.filter(r => !r.ok).length,
				messageTooLarge: data.filter(r => r.messageTooLarge).length,
				verifyMismatches: mismatches.length,
				totalMs: Math.round(data.reduce((a, r) => a + r.ms, 0)),
				jsonCompressionRatio: jsonRatio.length
					? round(jsonRatio.reduce((a, b) => a + b, 0) / jsonRatio.length, 2)
					: null,
			},
			detected: {
				messageTooLarge: data.some(r => r.messageTooLarge),
				verifyFailed: mismatches.length > 0,
			},
			detail: {
				legend: '<sizeMiB>:<ok|MTL|fail>/<max produce requests for that send>',
				cases: results,
				mismatches,
				verifiedRecords: total,
				expectedRecords: expectedTotal,
			},
			errors,
		}
	},
}
