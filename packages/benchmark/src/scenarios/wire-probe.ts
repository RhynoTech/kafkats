import net from 'node:net'

// Counts Kafka requests and socket bytes below the client, so it works for any client tree.
// Requests are recovered from the outgoing byte stream: each frame is a 4-byte size followed by
// the request header, whose first field is the API key.

export const ApiKeys = {
	Produce: 0,
	Fetch: 1,
	Heartbeat: 12,
	InitProducerId: 22,
	AddPartitionsToTxn: 24,
	AddOffsetsToTxn: 25,
	EndTxn: 26,
	TxnOffsetCommit: 28,
} as const

export interface RequestEvent {
	t: number
	apiKey: number
	bytes: number
	remotePort: number | undefined
}

interface FrameState {
	header: Buffer
	headerLength: number
	remaining: number
}

const HEADER_PREFIX = 6

export class WireProbe {
	rxBytes = 0
	txBytes = 0
	readonly requests = new Map<number, { count: number; bytes: number }>()
	readonly events: RequestEvent[] = []
	private readonly recorded: Set<number>
	private readonly listeners: Array<(event: RequestEvent) => void> = []

	constructor(recordApiKeys: readonly number[]) {
		this.recorded = new Set(recordApiKeys)
	}

	count(apiKey: number): number {
		return this.requests.get(apiKey)?.count ?? 0
	}

	onRequest(listener: (event: RequestEvent) => void): void {
		this.listeners.push(listener)
	}

	snapshot(): { rxBytes: number; txBytes: number; requests: Record<number, { count: number; bytes: number }> } {
		const requests: Record<number, { count: number; bytes: number }> = {}
		for (const [k, v] of this.requests) requests[k] = { ...v }
		return { rxBytes: this.rxBytes, txBytes: this.txBytes, requests }
	}

	/** @internal */
	frame(apiKey: number, bytes: number, remotePort: number | undefined): void {
		const entry = this.requests.get(apiKey)
		if (entry) {
			entry.count++
			entry.bytes += bytes
		} else {
			this.requests.set(apiKey, { count: 1, bytes })
		}
		if (!this.recorded.has(apiKey) && this.listeners.length === 0) return
		const event = { t: performance.now(), apiKey, bytes, remotePort }
		if (this.recorded.has(apiKey)) this.events.push(event)
		for (const listener of this.listeners) listener(event)
	}
}

let installed: WireProbe | null = null

export function installWireProbe(recordApiKeys: readonly number[] = []): WireProbe {
	if (installed) return installed
	const probe = new WireProbe(recordApiKeys)
	const states = new WeakMap<net.Socket, FrameState>()
	const proto = net.Socket.prototype as unknown as {
		write: (this: net.Socket, chunk: unknown, ...rest: unknown[]) => boolean
		push: (this: net.Socket, chunk: unknown, encoding?: BufferEncoding) => boolean
	}
	const originalWrite = proto.write
	const originalPush = proto.push

	proto.write = function (chunk, ...rest) {
		const buf =
			typeof chunk === 'string'
				? Buffer.from(chunk, typeof rest[0] === 'string' ? (rest[0] as BufferEncoding) : 'utf8')
				: chunk instanceof Uint8Array
					? chunk
					: null
		if (buf) {
			probe.txBytes += buf.length
			let state = states.get(this)
			if (!state) {
				state = { header: Buffer.alloc(HEADER_PREFIX), headerLength: 0, remaining: 0 }
				states.set(this, state)
			}
			feed(probe, state, buf, this.remotePort)
		}
		return originalWrite.call(this, chunk, ...rest)
	}

	// net.Socket hands every received chunk to push(), whether the client reads it via 'data' or read().
	proto.push = function (chunk, encoding) {
		if (chunk != null)
			probe.rxBytes += typeof chunk === 'string' ? Buffer.byteLength(chunk) : (chunk as Buffer).length
		return originalPush.call(this, chunk, encoding)
	}

	installed = probe
	return probe
}

function feed(probe: WireProbe, state: FrameState, chunk: Uint8Array, remotePort: number | undefined): void {
	let offset = 0
	while (offset < chunk.length) {
		if (state.remaining > 0) {
			const n = Math.min(state.remaining, chunk.length - offset)
			state.remaining -= n
			offset += n
			continue
		}
		const n = Math.min(HEADER_PREFIX - state.headerLength, chunk.length - offset)
		state.header.set(chunk.subarray(offset, offset + n), state.headerLength)
		state.headerLength += n
		offset += n
		if (state.headerLength < HEADER_PREFIX) return
		const size = state.header.readInt32BE(0)
		const apiKey = state.header.readInt16BE(4)
		state.headerLength = 0
		state.remaining = Math.max(0, size - 2)
		probe.frame(apiKey, size + 4, remotePort)
	}
}
