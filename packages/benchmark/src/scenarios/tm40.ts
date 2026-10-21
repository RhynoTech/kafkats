import type { DatasetSpec } from './types.js'

// Every Ticketmaster topic backlogged with customers-shaped records, 40 partitions in all.
const SHAPE: ReadonlyArray<[table: string, partitions: number]> = [
	['customers', 5],
	['events', 8],
	['plans', 8],
	['tickets', 8],
	['manifests', 4],
	['price_codes', 4],
	['seasons', 2],
	['attendances', 1],
]
export const TM40_RECORDS_PER_PARTITION = 5_250

export const TM40_TOPICS = SHAPE.map(([table, partitions]) => ({
	name: `ticketmaster.public.ticketmaster_${table}.scn40-v1`,
	partitions,
}))

export const tm40Dataset: DatasetSpec = {
	id: 'tm40-v1',
	topics: TM40_TOPICS.map((t, i) => ({
		...t,
		recordsPerPartition: TM40_RECORDS_PER_PARTITION,
		batchRecords: 7,
		compression: 'snappy' as const,
		kind: 'tm' as const,
		seed: 401 + i,
	})),
}
