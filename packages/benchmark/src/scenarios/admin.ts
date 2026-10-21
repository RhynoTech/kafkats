import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { generatorFor } from './records.js'
import { mulberry32 } from './rng.js'
import type {
	Client,
	DatasetManifest,
	DatasetSpec,
	DatasetTopicManifest,
	DatasetTopicSpec,
	Lib,
	RunTopicSpec,
} from './types.js'

// Loader-side setup. It runs in its own process with a fixed client tree, so every client under
// test reads byte-identical data and never needs admin or protocol APIs of its own.

const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))
const PRODUCE_IN_FLIGHT = 8

export async function connect(lib: Lib, bootstrap: string[], clientId: string): Promise<Client> {
	const client = new lib.KafkaClient({ brokers: bootstrap, clientId, logLevel: 'error' })
	await client.connect()
	// Cluster.getBroker() treats a broker as ready once its sockets open, before ApiVersions is
	// negotiated, so concurrent first produces can fail with UnsupportedVersionError.
	for (const nodeId of (await client.getMetadata()).brokers.keys()) await client.cluster.getBroker(nodeId)
	return client
}

export async function waitForLeaders(
	client: Client,
	topics: Array<{ name: string; partitions: number }>,
	timeoutMs = 60_000
): Promise<Record<string, number[]>> {
	const deadline = Date.now() + timeoutMs
	for (;;) {
		const metadata = await client.getMetadata(topics.map(t => t.name))
		const leaders: Record<string, number[]> = {}
		let ready = true
		for (const t of topics) {
			const partitions = metadata.topics.get(t.name)?.partitions
			const ids = Array.from({ length: t.partitions }, (_, p) => partitions?.get(p)?.leaderId ?? -1)
			if (ids.some(id => id < 0)) ready = false
			leaders[t.name] = ids
		}
		if (ready) return leaders
		if (Date.now() > deadline) throw new Error(`leaders not available: ${JSON.stringify(leaders)}`)
		await sleep(250)
	}
}

/** __consumer_offsets and __transaction_state are created on first use; do that outside any window. */
export async function warmCoordinators(client: Client): Promise<void> {
	for (const type of ['GROUP', 'TRANSACTION'] as const) {
		const deadline = Date.now() + 90_000
		for (;;) {
			try {
				await client.cluster.getCoordinator(type, 'scenario-warmup')
				break
			} catch (error) {
				if (Date.now() > deadline) throw error
				await sleep(500)
			}
		}
	}
}

export async function clusterId(client: Client): Promise<string | null> {
	return (await client.getMetadata()).clusterId
}

function manifestPath(stateDir: string, cluster: string | null, id: string): string {
	return join(stateDir, `${cluster ?? 'unknown-cluster'}.${id}.json`)
}

async function datasetIsLoaded(client: Client, spec: DatasetSpec): Promise<boolean> {
	const metadata = await client.getMetadata(spec.topics.map(t => t.name))
	const admin = client.admin()
	for (const t of spec.topics) {
		if (metadata.topics.get(t.name)?.partitions.size !== t.partitions) return false
		const partitions = Array.from({ length: t.partitions }, (_, p) => p)
		const ends = await admin.fetchTopicOffsets(t.name, partitions, 'latest')
		for (const p of partitions) if (ends.get(p) !== BigInt(t.recordsPerPartition)) return false
	}
	return true
}

export async function ensureDataset(
	lib: Lib,
	client: Client,
	spec: DatasetSpec,
	stateDir: string,
	log: (message: string) => void
): Promise<DatasetManifest> {
	const cluster = await clusterId(client)
	const path = manifestPath(stateDir, cluster, spec.id)
	if (existsSync(path) && (await datasetIsLoaded(client, spec))) {
		return JSON.parse(readFileSync(path, 'utf8')) as DatasetManifest
	}

	const t0 = performance.now()
	const names = spec.topics.map(t => t.name)
	const existing = await client.getMetadata(names)
	const stale = names.filter(name => existing.topics.has(name))
	if (stale.length > 0) {
		log(`dataset ${spec.id}: deleting incomplete topics ${stale.join(', ')}`)
		await client.admin().deleteTopics(stale)
		for (let i = 0; i < 120 && (await client.getMetadata(stale)).topics.size > 0; i++) await sleep(500)
	}
	await client.createTopics(
		spec.topics.map(t => ({
			name: t.name,
			numPartitions: t.partitions,
			replicationFactor: 1,
			configs: { 'compression.type': 'producer', ...t.configs },
		}))
	)
	await waitForLeaders(client, spec.topics)

	const topics: DatasetTopicManifest[] = []
	for (const t of spec.topics) {
		const tt = performance.now()
		const m = await loadTopic(lib, client, t)
		topics.push(m)
		if (m.records > 0) {
			log(
				`dataset ${spec.id}: ${t.name} ${m.records} records, ${(m.wireBytes / 1048576).toFixed(1)} MiB wire, ` +
					`${(m.keyValueBytes / m.records).toFixed(0)} B decoded and ${(m.wireBytes / m.records).toFixed(0)} B wire ` +
					`per record, ${Math.round(performance.now() - tt)} ms`
			)
		}
	}
	if (!(await datasetIsLoaded(client, spec))) throw new Error(`dataset ${spec.id} did not load completely`)
	const manifest: DatasetManifest = {
		id: spec.id,
		clusterId: cluster,
		createdAt: new Date().toISOString(),
		preloadMs: Math.round(performance.now() - t0),
		topics,
	}
	mkdirSync(stateDir, { recursive: true })
	writeFileSync(path, JSON.stringify(manifest, null, '\t'))
	return manifest
}

async function loadTopic(lib: Lib, client: Client, t: DatasetTopicSpec): Promise<DatasetTopicManifest> {
	const generate = generatorFor(t.kind)
	const compression = t.compression === 'snappy' ? lib.CompressionType.Snappy : lib.CompressionType.None
	const rngs = Array.from({ length: t.partitions }, (_, p) => mulberry32(t.seed * 1_000_003 + p))
	const seq = Array.from({ length: t.partitions }, () => 0)
	const manifest: DatasetTopicManifest = {
		name: t.name,
		partitions: t.partitions,
		recordsPerPartition: t.recordsPerPartition,
		records: 0,
		batches: 0,
		wireBytes: 0,
		keyValueBytes: 0,
	}
	const inFlight = new Set<Promise<void>>()
	let failure: unknown = null
	const rounds = Math.ceil(t.recordsPerPartition / t.batchRecords)
	for (let round = 0; round < rounds && !failure; round++) {
		const count = Math.min(t.batchRecords, t.recordsPerPartition - round * t.batchRecords)
		const encoded = await Promise.all(
			rngs.map((rng, p) => {
				const messages = []
				for (let i = 0; i < count; i++) {
					const r = generate(rng, seq[p]++, p)
					manifest.keyValueBytes += r.key.length + r.value.length
					messages.push({ key: r.key, value: r.value })
				}
				return lib.encodeRecordBatch(lib.createRecordBatch(messages, 0n), { compression })
			})
		)
		const batches = new Map<number, Buffer>()
		encoded.forEach((buf, p) => {
			manifest.wireBytes += buf.length
			manifest.records += count
			manifest.batches++
			batches.set(p, buf)
		})
		const request = produceWithRetry(client, t.name, batches).catch(error => {
			failure = error
		})
		inFlight.add(request)
		void request.finally(() => inFlight.delete(request))
		if (inFlight.size >= PRODUCE_IN_FLIGHT) await Promise.race(inFlight)
	}
	await Promise.all(inFlight)
	if (failure) throw failure
	return manifest
}

async function produceWithRetry(client: Client, topic: string, batches: Map<number, Buffer>): Promise<void> {
	let pending = batches
	for (let attempt = 1; ; attempt++) {
		const responses = await client.produceToMultiplePartitions(topic, pending, { acks: -1, timeoutMs: 30_000 })
		const failed = new Map<number, Buffer>()
		let lastError = ''
		for (const [p, buf] of pending) {
			const response = responses.get(p)
			if (!response || response.errorCode !== 0) {
				failed.set(p, buf)
				lastError = `${topic}-${p} error ${response?.errorCode ?? 'missing'} ${response?.errorMessage ?? ''}`
			}
		}
		if (failed.size === 0) return
		if (attempt >= 8) throw new Error(`preload produce failed: ${lastError}`)
		pending = failed
		await sleep(250 * attempt)
		await client.getMetadata([topic])
	}
}

export async function createRunTopics(
	client: Client,
	specs: RunTopicSpec[],
	leaders: Record<string, number[]>
): Promise<void> {
	const plain = specs.filter(s => !s.colocateWith)
	if (plain.length > 0) {
		await client.createTopics(
			plain.map(s => ({ name: s.name, numPartitions: s.partitions, replicationFactor: 1, configs: s.configs }))
		)
	}
	const pinned = specs.filter(s => s.colocateWith)
	if (pinned.length > 0) {
		const controller = await client.cluster.getControllerBroker()
		const response = await controller.createTopics({
			topics: pinned.map(s => {
				const leader = leaders[s.colocateWith!.topic]?.[s.colocateWith!.partition]
				if (leader === undefined || leader < 0) throw new Error(`no leader to colocate ${s.name} with`)
				return {
					name: s.name,
					numPartitions: -1,
					replicationFactor: -1,
					assignments: Array.from({ length: s.partitions }, (_, p) => ({
						partitionIndex: p,
						brokerIds: [leader],
					})),
					configs: Object.entries(s.configs ?? {}).map(([name, value]) => ({ name, value })),
				}
			}),
			timeoutMs: 30_000,
			validateOnly: false,
		})
		for (const t of response.topics) {
			if (t.errorCode !== 0) throw new Error(`create ${t.name} failed: ${t.errorCode} ${t.errorMessage ?? ''}`)
		}
	}
}

export async function deleteTopics(client: Client, names: string[]): Promise<void> {
	if (names.length > 0) await client.admin().deleteTopics(names)
}
