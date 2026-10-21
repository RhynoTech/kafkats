import { CompletedFetch, readFetchedRecords } from '@/consumer/completed-fetch.js'
import type { FetchedRecords } from '@/consumer/completed-fetch.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'
import type { DecodedRecord } from '@/protocol/records/index.js'

/**
 * One uncompressed batch holding a record at each offset, with `valueBytes`-byte values (null for
 * none). A 36-byte value plus the 64-byte per-record overhead estimates each record at 100 bytes.
 */
export function encodeBatch(offsets: bigint[], valueBytes: number | null = 36): Buffer {
	const batch = createRecordBatch(
		offsets.map(() => ({ value: valueBytes === null ? null : Buffer.alloc(valueBytes), timestamp: 0 })),
		offsets[0]!,
		0n
	)
	batch.records.forEach((record, index) => {
		record.offsetDelta = Number(offsets[index]! - offsets[0]!)
	})
	batch.lastOffsetDelta = Number(offsets[offsets.length - 1]! - offsets[0]!)
	return encodeRecordBatchSync(batch)
}

export function fetchedRecords(data: Buffer, fetchOffset = data.readBigInt64BE(0)): FetchedRecords {
	const fetched = readFetchedRecords(data, {
		fetchOffset,
		abortedTransactions: [],
		lastStableOffset: 1n << 62n,
		readCommitted: false,
		verifyCrc: true,
	})
	if (fetched instanceof Promise) throw new Error('expected header-only read')
	return fetched
}

export function completedFetch(
	topic: string,
	partition: number,
	offsets: bigint[],
	options: { valueBytes?: number | null; assignmentEpoch?: number } = {}
): CompletedFetch {
	const data = encodeBatch(offsets, options.valueBytes === undefined ? 36 : options.valueBytes)
	return new CompletedFetch(topic, partition, options.assignmentEpoch ?? 0, fetchedRecords(data))
}

/** Consume every record left in a fetch, decoding as a drain would. */
export async function drainAll(fetch: CompletedFetch): Promise<DecodedRecord[]> {
	const records: DecodedRecord[] = []
	for (;;) {
		const peeked = fetch.peek()
		const record = peeked instanceof Promise ? await peeked : peeked
		if (record === null) return records
		records.push(record)
		fetch.consume()
	}
}
