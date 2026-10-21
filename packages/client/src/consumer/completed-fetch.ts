/**
 * One partition's records from a Fetch response, held as fetched and decoded while they are
 * drained, like the Java client's CompletedFetch. Only batch headers are read when the response
 * arrives: enough to advance the fetch position and to count the records a drain will deliver.
 */

import { Decoder } from '@/protocol/primitives/decoder.js'
import { decodeRecord, decodeRecordInBatch } from '@/protocol/records/record.js'
import type { DecodedRecord, DecodeRecordInBatchOptions } from '@/protocol/records/record.js'
import {
	getCompressionType,
	isControlBatch,
	isLogAppendTime,
	isTransactional,
} from '@/protocol/records/record-batch.js'
import { CompressionType } from '@/protocol/records/compression.js'
import { compressionCodecs, missingCodecError } from '@/protocol/records/codec-registry.js'
import { crc32c } from '@/utils/crc32c.js'

/** baseOffset(8) and batchLength(4) precede the bytes that batchLength counts. */
const LENGTH_PREFIX_BYTES = 12
/** Header fields from baseOffset through recordCount; the records follow. */
const HEADER_BYTES = 61
/** The CRC covers the batch from its attributes to its end. */
const CRC_START = 21

interface AbortedRange {
	start: bigint
	endExclusive: bigint
}

interface FetchedBatch {
	start: number
	end: number
	baseOffset: bigint
	lastOffsetDelta: number
	attributes: number
	baseTimestamp: bigint
	maxTimestamp: bigint
	recordCount: number
	/** Aborted transactions of this batch's producer; empty unless the batch is transactional. */
	aborted: AbortedRange[]
	/** Every record passes the offset and abort filters, so a drain skips the per-record check. */
	allVisible: boolean
}

interface BatchHeader extends Omit<FetchedBatch, 'aborted' | 'allVisible'> {
	producerId: bigint
}

interface Corruption {
	/** The bad batch's base offset; null when too few bytes are left to hold one. */
	offset: bigint | null
	error: unknown
}

export interface FetchedRecords {
	/** Offset after the last complete batch before any corrupt one, control and empty batches included; else null. */
	nextOffset: bigint | null
	/** Records at or after the fetch offset that are neither control records nor aborted. */
	recordCount: number
	fetchOffset: bigint
	/** The batches that hold those records, located in `data`. */
	batches: FetchedBatch[]
	/** A copy of the bytes from the first to the last of those batches. */
	data: Buffer
	/** A batch after those that failed validation or decoding, raised once their records are drained. */
	corruption: Corruption | null
}

export interface ReadFetchedRecordsOptions {
	fetchOffset: bigint
	abortedTransactions: Array<{ producerId: bigint; firstOffset: bigint }>
	lastStableOffset: bigint
	readCommitted: boolean
	verifyCrc: boolean
}

/**
 * Locate the record batches in a partition's fetched bytes and count the records a drain will
 * deliver once it applies the offset, control-batch and aborted-transaction filters. Corrupt data
 * ends the batches read at the bad batch, which is returned as `corruption`, as the Java client's
 * CompletedFetch delivers the records before a corrupt batch.
 *
 * Returns a promise only when a compacted batch straddles the fetch offset or an aborted range:
 * its surviving offsets are known only after decoding it.
 */
export function readFetchedRecords(
	data: Buffer,
	options: ReadFetchedRecordsOptions
): FetchedRecords | Promise<FetchedRecords> {
	let { headers, corruption } = readBatchHeaders(data, options.verifyCrc)

	const abortedRanges = options.readCommitted ? abortedRangesOf(headers, options) : []
	const { fetchOffset } = options
	const counted: Array<{ batch: FetchedBatch; count: number | null }> = []
	for (const header of headers) {
		if (isControlBatch(header.attributes)) continue
		const { producerId, ...fields } = header
		const aborted = isTransactional(header.attributes)
			? abortedRanges.filter(range => range.producerId === producerId)
			: []
		const batch: FetchedBatch = { ...fields, aborted, allVisible: false }
		counted.push({ batch, count: countFromHeader(batch, fetchOffset) })
	}

	const finish = (): FetchedRecords => {
		const batches: FetchedBatch[] = []
		let recordCount = 0
		for (const { batch, count } of counted) {
			if (!count) continue
			batch.allVisible = count === batch.recordCount
			batches.push(batch)
			recordCount += count
		}
		return { nextOffset: endOffset(headers), recordCount, fetchOffset, corruption, ...copyBatches(data, batches) }
	}

	if (counted.every(({ count }) => count !== null)) return finish()
	return (async () => {
		for (const [index, entry] of counted.entries()) {
			if (entry.count !== null) continue
			try {
				entry.count = await countByDecoding(data, entry.batch, fetchOffset)
			} catch (error) {
				const { start, baseOffset } = entry.batch
				counted.length = index
				headers = headers.filter(header => header.start < start)
				corruption = { offset: baseOffset, error }
				break
			}
		}
		return finish()
	})()
}

/** Offset after the last of the given batches, control and empty batches included; null if none. */
function endOffset(headers: BatchHeader[]): bigint | null {
	let nextOffset: bigint | null = null
	for (const header of headers) {
		const end = header.baseOffset + BigInt(header.lastOffsetDelta) + 1n
		if (nextOffset === null || end > nextOffset) nextOffset = end
	}
	return nextOffset
}

/** Estimated in-memory size of a decoded record; drives the per-partition batch cap and the poll budget. */
export function recordBytes(record: DecodedRecord): number {
	return (record.key?.length ?? 0) + (record.value?.length ?? 0) + 64
}

/**
 * A partition's fetched batches, decoded one batch at a time as records are drained. Records can be
 * decoded ahead of the drain; they stay here, counted, until it consumes them.
 */
export class CompletedFetch {
	/** Set when the buffer discards this fetch, so a drain awaiting decompression drops what it read. */
	dropped = false
	/** The offset to fetch again from to get back every record not yet delivered. */
	position: bigint
	private consumedPosition: bigint
	private undecoded: number
	private readonly data: Buffer
	private readonly batches: FetchedBatch[]
	private readonly fetchOffset: bigint
	private nextBatch = 0
	private reader: BatchReader | null = null
	/** Records decoded but not yet consumed, at [head, tail). The array is reused, never trimmed. */
	private readonly decoded: Array<DecodedRecord | undefined> = []
	private head = 0
	private tail = 0
	private decodedBytes = 0
	/** A batch being decompressed; decoding waits for it so records stay in order. */
	private opening: Promise<void> | null = null
	private failure: { error: unknown } | null = null
	/** A batch found corrupt when fetched, raised once every record before it is consumed. */
	readonly corruption: Corruption | null

	constructor(
		readonly topic: string,
		readonly partition: number,
		readonly assignmentEpoch: number,
		records: FetchedRecords
	) {
		this.data = records.data
		this.batches = records.batches
		this.fetchOffset = records.fetchOffset
		this.undecoded = records.recordCount
		this.position = records.fetchOffset
		this.consumedPosition = records.fetchOffset
		this.corruption = records.corruption
	}

	/**
	 * Bytes this fetch keeps alive: its batches as fetched, a batch decompressed for decoding, and
	 * the records decoded but not yet consumed at their estimated size.
	 */
	get heldBytes(): number {
		return this.data.length + (this.reader?.decompressedBytes ?? 0) + this.decodedBytes
	}

	get isEmpty(): boolean {
		return this.undecoded === 0 && this.head === this.tail && this.corruption === null
	}

	/** Where a drain's error from this fetch lies: the corrupt batch, else the first undelivered record. */
	failedAt(error: unknown): bigint {
		const corruption = this.corruption
		return (corruption !== null && error === corruption.error ? corruption.offset : null) ?? this.position
	}

	/** The next deliverable record, without consuming it; null once every record was consumed. */
	peek(): DecodedRecord | null | Promise<DecodedRecord | null> {
		const record = this.ahead(0)
		return record instanceof Promise ? record.then(() => this.peek()) : record
	}

	/**
	 * The record `index` places after the next one, decoding up to it; null if fewer are left. While
	 * a batch is being decompressed it returns a promise to wait on before asking again.
	 */
	ahead(index: number): DecodedRecord | null | Promise<void> {
		while (this.head + index >= this.tail) {
			const decoded = this.decodeNext()
			if (decoded === false) return null
			if (decoded !== true) return decoded
		}
		return this.decoded[this.head + index]!
	}

	/** Consume the record peek() returned. */
	consume(): void {
		const record = this.decoded[this.head]!
		this.consumedPosition = record.offset + 1n
		this.decodedBytes -= recordBytes(record)
		// Consumed records belong to the handler now; holding them would pin their buffers.
		this.decoded[this.head++] = undefined
		if (this.head === this.tail) {
			this.head = 0
			this.tail = 0
		}
	}

	/** Mark consumed records delivered, so a pause no longer rewinds to them. */
	commit(): void {
		this.position = this.consumedPosition
	}

	/** Decode the next deliverable record; false once all are decoded. A failure is kept and rethrown. */
	private decodeNext(): boolean | Promise<void> {
		if (this.opening) return this.opening
		if (this.failure) throw this.failure.error
		try {
			while (this.undecoded > 0) {
				const reader = this.reader
				if (reader === null) {
					const batch = this.batches[this.nextBatch++]
					if (batch === undefined) break
					const opened = openBatch(this.data, batch)
					if (opened instanceof Promise) return this.open(opened)
					this.reader = opened
					continue
				}
				const record = reader.next()
				if (record === null) {
					this.reader = null
				} else if (
					reader.batch.allVisible ||
					isVisible(record.offset, this.fetchOffset, reader.batch.aborted)
				) {
					this.decoded[this.tail++] = record
					this.decodedBytes += recordBytes(record)
					if (--this.undecoded === 0) this.reader = null
					return true
				}
			}
		} catch (error) {
			this.failure = { error }
			throw error
		}
		this.undecoded = 0
		if (this.corruption) throw this.corruption.error
		return false
	}

	private open(opened: Promise<BatchReader>): Promise<void> {
		this.opening = opened
			.then(
				reader => {
					this.reader = reader
				},
				(error: unknown) => {
					this.failure = { error }
					throw error
				}
			)
			.finally(() => {
				this.opening = null
			})
		return this.opening
	}
}

class BatchReader {
	private readonly decoder: Decoder
	private readonly dense: boolean
	private readonly options: DecodeRecordInBatchOptions
	private left: number
	private offset: bigint

	constructor(
		readonly batch: FetchedBatch,
		records: Buffer,
		readonly decompressedBytes: number
	) {
		// The count is attacker-controllable and every record takes at least one byte.
		if (batch.recordCount > records.length) {
			throw new Error(
				`Invalid record count ${batch.recordCount} in record batch (record data is ${records.length} bytes)`
			)
		}
		this.decoder = new Decoder(records)
		// A compacted batch has gaps in its offsets, so only a dense one may take offsets in sequence.
		this.dense = batch.recordCount === batch.lastOffsetDelta + 1
		this.options = {
			verifyLength: true,
			logAppendTime: isLogAppendTime(batch.attributes) ? batch.maxTimestamp : undefined,
		}
		this.left = batch.recordCount
		this.offset = batch.baseOffset
	}

	next(): DecodedRecord | null {
		if (this.left === 0) return null
		this.left--
		if (!this.dense) {
			return decodeRecord(
				this.decoder,
				this.batch.baseOffset,
				this.batch.baseTimestamp,
				this.options.logAppendTime
			)
		}
		const record = decodeRecordInBatch(this.decoder, this.offset, this.batch.baseTimestamp, this.options)
		this.offset += 1n
		return record
	}
}

function openBatch(data: Buffer, batch: FetchedBatch): BatchReader | Promise<BatchReader> {
	const records = data.subarray(batch.start + HEADER_BYTES, batch.end)
	const compression = getCompressionType(batch.attributes)
	if (compression === CompressionType.None) return new BatchReader(batch, records, 0)
	const codec = compressionCodecs.get(compression)
	if (!codec) throw missingCodecError(compression)
	return codec.decompress(records).then(decompressed => new BatchReader(batch, decompressed, decompressed.length))
}

/** The headers of the complete batches up to the first one that fails validation. */
function readBatchHeaders(data: Buffer, verifyCrc: boolean): { headers: BatchHeader[]; corruption: Corruption | null } {
	const headers: BatchHeader[] = []
	let start = 0
	try {
		while (start < data.length) {
			const remaining = data.length - start
			if (remaining < LENGTH_PREFIX_BYTES || remaining < LENGTH_PREFIX_BYTES + data.readInt32BE(start + 8)) {
				// The broker may cut the trailing batch at maxBytes but always returns the first one whole,
				// so only a cut after a complete batch is truncation; anything else is corruption.
				if (headers.length > 0) break
				throw new Error(`truncated record batch: ${remaining} bytes`)
			}
			const end = start + LENGTH_PREFIX_BYTES + data.readInt32BE(start + 8)
			if (end - start < HEADER_BYTES)
				throw new Error(`Invalid record batch length ${end - start - LENGTH_PREFIX_BYTES}`)
			const magic = data.readInt8(start + 16)
			if (magic !== 2) throw new Error(`Unsupported record batch magic: ${magic}, expected 2`)
			if (verifyCrc) {
				const storedCrc = data.readUInt32BE(start + 17)
				const computedCrc = crc32c(data, start + CRC_START, end - start - CRC_START)
				if (storedCrc !== computedCrc) {
					throw new Error(
						`CRC mismatch: stored=${storedCrc.toString(16)}, computed=${computedCrc.toString(16)}`
					)
				}
			}
			const recordCount = data.readInt32BE(start + 57)
			if (recordCount < 0) throw new Error(`Invalid record count ${recordCount} in record batch`)
			headers.push({
				start,
				end,
				baseOffset: data.readBigInt64BE(start),
				attributes: data.readInt16BE(start + 21),
				lastOffsetDelta: data.readInt32BE(start + 23),
				baseTimestamp: data.readBigInt64BE(start + 27),
				maxTimestamp: data.readBigInt64BE(start + 35),
				producerId: data.readBigInt64BE(start + 43),
				recordCount,
			})
			start = end
		}
	} catch (error) {
		const offset = data.length - start >= 8 ? data.readBigInt64BE(start) : null
		return { headers, corruption: { offset, error } }
	}
	return { headers, corruption: null }
}

/** Each aborted transaction ends at its producer's next marker in this response, else at the LSO. */
function abortedRangesOf(
	headers: BatchHeader[],
	options: ReadFetchedRecordsOptions
): Array<AbortedRange & { producerId: bigint }> {
	if (options.abortedTransactions.length === 0) return []
	const markers = headers
		.filter(header => isControlBatch(header.attributes))
		.sort((a, b) => (a.baseOffset < b.baseOffset ? -1 : a.baseOffset > b.baseOffset ? 1 : 0))
	return options.abortedTransactions.map(tx => ({
		producerId: tx.producerId,
		start: tx.firstOffset,
		endExclusive:
			markers.find(marker => marker.producerId === tx.producerId && marker.baseOffset > tx.firstOffset)
				?.baseOffset ?? options.lastStableOffset,
	}))
}

function isVisible(offset: bigint, fetchOffset: bigint, aborted: AbortedRange[]): boolean {
	return offset >= fetchOffset && !aborted.some(range => offset >= range.start && offset < range.endExclusive)
}

/** Deliverable records of a batch from its header alone, or null when that needs the record offsets. */
function countFromHeader(batch: FetchedBatch, fetchOffset: bigint): number | null {
	const first = batch.baseOffset
	const endExclusive = first + BigInt(batch.lastOffsetDelta) + 1n
	if (batch.recordCount === batch.lastOffsetDelta + 1) {
		return Number(countVisibleOffsets(first > fetchOffset ? first : fetchOffset, endExclusive, batch.aborted))
	}
	const inside = (offset: bigint) => offset > first && offset < endExclusive
	const straddles =
		inside(fetchOffset) || batch.aborted.some(range => inside(range.start) || inside(range.endExclusive))
	if (straddles) return null
	return isVisible(first, fetchOffset, batch.aborted) ? batch.recordCount : 0
}

/** Offsets in [from, to) outside every aborted range; ranges may overlap. */
function countVisibleOffsets(from: bigint, to: bigint, aborted: AbortedRange[]): bigint {
	if (to <= from) return 0n
	let visible = to - from
	let covered = from
	for (const range of [...aborted].sort((a, b) => (a.start < b.start ? -1 : a.start > b.start ? 1 : 0))) {
		const start = range.start > covered ? range.start : covered
		const end = range.endExclusive < to ? range.endExclusive : to
		if (end > start) {
			visible -= end - start
			covered = end
		}
	}
	return visible
}

async function countByDecoding(data: Buffer, batch: FetchedBatch, fetchOffset: bigint): Promise<number> {
	const reader = await openBatch(data, batch)
	let count = 0
	for (let record = reader.next(); record !== null; record = reader.next()) {
		if (isVisible(record.offset, fetchOffset, batch.aborted)) count++
	}
	return count
}

/**
 * Copy the bytes spanning the given batches. Views into the response would keep the whole response
 * alive until the last partition in it drains, so the buffer could not account for what it holds.
 */
function copyBatches(data: Buffer, batches: FetchedBatch[]): Pick<FetchedRecords, 'batches' | 'data'> {
	if (batches.length === 0) return { batches, data: Buffer.alloc(0) }
	const from = batches[0]!.start
	const copy = Buffer.allocUnsafeSlow(batches[batches.length - 1]!.end - from)
	data.copy(copy, 0, from, from + copy.length)
	for (const batch of batches) {
		batch.start -= from
		batch.end -= from
	}
	return { batches, data: copy }
}
