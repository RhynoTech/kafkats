import type * as Kafkats from '@kafkats/client'

/** The client module loaded from `--client`. Typed against this workspace's client; other trees may differ. */
export type Lib = typeof Kafkats
export type Client = InstanceType<Lib['KafkaClient']>

export type RecordKind = 'json500' | 'tm' | 'fans'
export type Compression = 'none' | 'snappy'

export interface DatasetTopicSpec {
	name: string
	partitions: number
	/** Records preloaded into every partition; 0 creates an empty topic. */
	recordsPerPartition: number
	/** Records per stored RecordBatch, which fixes the on-disk layout the consumer fetches. */
	batchRecords: number
	compression: Compression
	kind: RecordKind
	seed: number
	configs?: Record<string, string>
}

export interface DatasetSpec {
	id: string
	topics: DatasetTopicSpec[]
}

export interface RunTopicSpec {
	name: string
	partitions: number
	configs?: Record<string, string>
	/** Place every replica on the leader of this partition, so the topic shares its broker. */
	colocateWith?: { topic: string; partition: number }
}

export interface DatasetTopicManifest {
	name: string
	partitions: number
	recordsPerPartition: number
	records: number
	batches: number
	/** Sum of encoded RecordBatch sizes, which is what a Fetch transfers. */
	wireBytes: number
	keyValueBytes: number
}

export interface DatasetManifest {
	id: string
	clusterId: string | null
	createdAt: string
	preloadMs: number
	topics: DatasetTopicManifest[]
}

export interface ScenarioSetup {
	scenario: string
	runTag: string
	bootstrap: string[]
	clusterId: string | null
	brokerCount: number
	datasets: Record<string, DatasetManifest>
	/** Leader node id per partition, for dataset and run topics. */
	leaders: Record<string, number[]>
	runTopics: string[]
}

export type Headline = Record<string, number | string | boolean | null>

export interface ScenarioOutput {
	headline: Headline
	config: Record<string, unknown>
	detail: Record<string, unknown>
	/** Signals a scenario was built to expose, e.g. starvation or MessageTooLarge. */
	detected: Record<string, boolean>
	errors: string[]
}

export interface RunContext {
	lib: Lib
	clientPath: string
	setup: ScenarioSetup
	/** Multiplies every measurement window; datasets are not resized. */
	scale: number
	outDir: string
	log: (message: string) => void
}

export interface Scenario {
	id: string
	name: string
	description: string
	datasets(): DatasetSpec[]
	runTopics(runTag: string): RunTopicSpec[]
	run(ctx: RunContext): Promise<ScenarioOutput>
}
