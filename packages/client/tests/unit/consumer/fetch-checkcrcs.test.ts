import { describe, expect, it, vi } from 'vitest'

import { FetchManager } from '@/consumer/fetch-manager.js'
import { OffsetManager } from '@/consumer/offset-manager.js'
import { ErrorCode } from '@/protocol/messages/error-codes.js'
import { createRecordBatch, encodeRecordBatchSync } from '@/protocol/records/index.js'

async function makeFetchManager(checkCrcs: boolean) {
	const cluster = {
		getLeaderForPartition: vi.fn().mockRejectedValue(new Error('no broker in unit test')),
		getLogger: () => null,
	}
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const offsetManager = new OffsetManager(cluster as any, 'g1')
	const fm = new FetchManager(
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		cluster as any,
		offsetManager,
		'earliest',
		{
			maxBytesPerPartition: 1024,
			minBytes: 1,
			maxWaitMs: 20,
			partitionConcurrency: 1,
			isolationLevel: 'read_uncommitted',
			checkCrcs,
		}
	)
	await fm.poll()
	fm.addPartitions([{ topic: 't', partition: 0, offset: 0n }])
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	const fmAny = fm as any
	const fetch = vi.fn().mockResolvedValue({
		topics: [
			{
				topic: 't',
				partitions: [
					{
						partitionIndex: 0,
						errorCode: ErrorCode.None,
						highWatermark: 1n,
						lastStableOffset: 1n,
						logStartOffset: 0n,
						abortedTransactions: [],
						preferredReadReplica: -1,
						recordsData: batchWithBadCrc(),
					},
				],
			},
		],
		throttleTimeMs: 0,
		errorCode: ErrorCode.None,
		sessionId: 0,
	})
	await fmAny.fetchFromBrokerToBuffer({ nodeId: 1, fetch }, [fmAny.partitionStates.get('t:0')], 1048576)
	return fm
}

// A valid batch with its CRC field corrupted (the record data itself is intact, so decode
// succeeds when CRC checking is off and only the checksum comparison fails when it is on).
function batchWithBadCrc(): Buffer {
	const buf = Buffer.from(encodeRecordBatchSync(createRecordBatch([{ value: 'hello', timestamp: 0 }], 0n, 0n)))
	// CRC is the uint32 at offset 17: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1).
	buf[17] = buf[17]! ^ 0xff
	return buf
}

describe('FetchManager checkCrcs', () => {
	it('surfaces a corrupt batch CRC from poll() when checkCrcs is true', async () => {
		const fm = await makeFetchManager(true)
		await expect(fm.poll()).rejects.toThrow(/CRC mismatch/)
		fm.stop()
	})

	it('skips CRC verification and decodes the batch when checkCrcs is false', async () => {
		const fm = await makeFetchManager(false)
		const batches = await fm.poll()
		expect(batches.flatMap(batch => batch.records.map(record => record.value?.toString('utf-8')))).toEqual([
			'hello',
		])
		fm.stop()
	})
})
