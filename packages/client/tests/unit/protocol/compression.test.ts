import { describe, expect, it, vi } from 'vitest'

import { createRequire } from 'node:module'
import { gzipSync, gunzipSync } from 'node:zlib'

import {
	CompressionType,
	createLz4Codec,
	createSnappyCodec,
	createZstdCodec,
	getCompressionTypeName,
	gzipCodec,
	type SnappyNativeLib,
} from '@/protocol/records/compression.js'
import {
	CodecRegistry,
	compressionCodecs,
	missingCodecError,
	type ModuleLoader,
} from '@/protocol/records/codec-registry.js'

describe('compression registry', () => {
	it('registers and retrieves codecs', async () => {
		const payload = Buffer.from('hello')
		const snappy = createSnappyCodec({
			compress: async (data: Buffer | Uint8Array | string) => Buffer.from(data),
			uncompress: async (data: Buffer) => data,
		})
		compressionCodecs.register(CompressionType.Snappy, snappy)

		const codec = compressionCodecs.get(CompressionType.Snappy)
		expect(codec).toBeDefined()
		const compressed = await codec!.compress(payload)
		const decompressed = await codec!.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('handles built-in gzip codec', async () => {
		const payload = Buffer.from('gzip-test')
		const gzip = compressionCodecs.get(CompressionType.Gzip)
		expect(gzip).toBeDefined()
		const compressed = await gzip!.compress(payload)
		const decompressed = await gzip!.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('guards invalid registrations and lookups', () => {
		expect(compressionCodecs.has(CompressionType.None)).toBe(true)
		expect(compressionCodecs.get(CompressionType.None)).toBeUndefined()
		expect(() =>
			compressionCodecs.register(CompressionType.None, {
				compress: async () => Buffer.alloc(0),
				decompress: async () => Buffer.alloc(0),
			})
		).toThrow('Cannot register codec for CompressionType.None')
		expect(getCompressionTypeName(CompressionType.Lz4)).toBe('Lz4')
		expect(getCompressionTypeName(99 as CompressionType)).toBe('Unknown(99)')
	})

	it('creates an LZ4 codec from sync functions', async () => {
		const codec = createLz4Codec({
			encode: data => data,
			decode: data => data,
		})
		const payload = Buffer.from('lz4')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('prefers the framed lz4-napi API (compressFrame/decompressFrame) over raw block', async () => {
		let framedCompress = 0
		let framedDecompress = 0
		const codec = createLz4Codec({
			compress: async () => Buffer.from('raw-block-must-not-be-used'),
			uncompress: async () => Buffer.from('raw-block-must-not-be-used'),
			compressFrame: async (data: Buffer | Uint8Array | string) => {
				framedCompress++
				return Buffer.from(data as Uint8Array)
			},
			decompressFrame: async (data: Buffer | Uint8Array | string) => {
				framedDecompress++
				return Buffer.from(data as Uint8Array)
			},
		})
		const payload = Buffer.from('lz4-frame')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(framedCompress).toBe(1)
		expect(framedDecompress).toBe(1)
		expect(decompressed).toEqual(payload)
	})

	it('throws for raw-block lz4-napi (< 2.x) lacking compressFrame — Kafka RecordBatch v2 requires LZ4 framing', () => {
		expect(() =>
			createLz4Codec({
				compress: async () => Buffer.alloc(0),
				uncompress: async () => Buffer.alloc(0),
			})
		).toThrow(/lz4-napi >= 2\.x/)
	})

	it('creates a Zstd codec from async functions', async () => {
		const codec = createZstdCodec({
			compress: async (data: Buffer) => data,
			decompress: async (data: Buffer) => data,
		})
		const payload = Buffer.from('zstd')
		const compressed = await codec.compress(payload)
		const decompressed = await codec.decompress(compressed)
		expect(decompressed).toEqual(payload)
	})

	it('returns undefined for unknown codec', () => {
		// Keep the result independent of which libraries are installed in the workspace
		compressionCodecs.autoRegister = false
		try {
			expect(compressionCodecs.get(CompressionType.Zstd)).toBeUndefined()
		} finally {
			compressionCodecs.autoRegister = true
		}
	})
})

describe('snappy Xerial framing', () => {
	// Loaded with require like the production module loader; a static import
	// trips on snappy's WASI fallback in vitest's ESM transform.
	const snappy = createRequire(import.meta.url)('snappy') as SnappyNativeLib & {
		compressSync: (data: Buffer) => Buffer
	}

	function int32(n: number): Buffer {
		const b = Buffer.alloc(4)
		b.writeInt32BE(n)
		return b
	}

	const XERIAL_HEADER = Buffer.concat([
		Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]),
		int32(1),
		int32(1),
	])

	function xerialFrame(...blocks: Buffer[]): Buffer {
		return Buffer.concat([XERIAL_HEADER, ...blocks.flatMap(b => [int32(b.length), b])])
	}

	it('decodes Xerial-framed payloads produced by Java clients (real snappy)', async () => {
		const codec = createSnappyCodec(snappy)
		const part1 = Buffer.from('hello ')
		const part2 = Buffer.from('xerial world')
		const framed = xerialFrame(snappy.compressSync(part1), snappy.compressSync(part2))

		expect(await codec.decompress(framed)).toEqual(Buffer.concat([part1, part2]))
	})

	it('round-trips raw blocks with the real snappy library', async () => {
		const codec = createSnappyCodec(snappy)
		const payload = Buffer.from('raw snappy round-trip')

		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('decodes Xerial-framed payloads with a sync (snappyjs-style) library', async () => {
		const codec = createSnappyCodec({
			compress: (data: ArrayBuffer | Buffer | Uint8Array) => Buffer.from(data as Uint8Array),
			uncompress: (data: ArrayBuffer | Buffer | Uint8Array) => Buffer.from(data as Uint8Array),
		})
		const framed = xerialFrame(Buffer.from('chunk1-'), Buffer.from('chunk2'))

		expect((await codec.decompress(framed)).toString()).toBe('chunk1-chunk2')
	})

	it('passes payloads without the Xerial magic straight to the library', async () => {
		const codec = createSnappyCodec(snappy)
		const raw = await snappy.compress(Buffer.from('no framing'))

		expect((await codec.decompress(raw)).toString()).toBe('no framing')
	})

	it('rejects malformed Xerial chunks instead of over-reading', async () => {
		const codec = createSnappyCodec(snappy)
		const oversized = Buffer.concat([XERIAL_HEADER, int32(1000), Buffer.from('short')])
		const truncated = Buffer.concat([XERIAL_HEADER, Buffer.from([0x00, 0x00])])

		await expect(codec.decompress(oversized)).rejects.toThrow('Invalid Xerial-framed Snappy data')
		await expect(codec.decompress(truncated)).rejects.toThrow('Invalid Xerial-framed Snappy data')
	})
})

const json = (bytes: number) =>
	Buffer.from('{"id":"8f1c","name":"Jane","city":"Detroit"},'.repeat(bytes / 45 + 1)).subarray(0, bytes)

describe('snappy small-block fast path', () => {
	type SyncSnappy = SnappyNativeLib & {
		compressSync: (data: Buffer) => Buffer
		uncompressSync: (data: Buffer) => Buffer
	}
	const snappy = createRequire(import.meta.url)('snappy') as SyncSnappy

	function countingSnappy() {
		const calls = { compress: 0, compressSync: 0, uncompress: 0, uncompressSync: 0 }
		const lib: SyncSnappy = {
			compress: data => (calls.compress++, snappy.compress(data)),
			compressSync: data => (calls.compressSync++, snappy.compressSync(data)),
			uncompress: data => (calls.uncompress++, snappy.uncompress(data)),
			uncompressSync: data => (calls.uncompressSync++, snappy.uncompressSync(data)),
		}
		const codec = createSnappyCodec(lib)
		calls.compress = 0 // the async-detection probe
		return { codec, calls }
	}

	function xerialFrame(...blocks: Buffer[]): Buffer {
		const int32 = (n: number) => {
			const b = Buffer.alloc(4)
			b.writeInt32BE(n)
			return b
		}
		const header = Buffer.concat([
			Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00]),
			int32(1),
			int32(1),
		])
		return Buffer.concat([header, ...blocks.flatMap(b => [int32(b.length), b])])
	}

	it('compresses and decompresses small blocks synchronously', async () => {
		const { codec, calls } = countingSnappy()
		const payload = json(4096)

		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
		expect(calls).toEqual({ compress: 0, compressSync: 1, uncompress: 0, uncompressSync: 1 })
	})

	it('keeps large blocks on the async API', async () => {
		const { codec, calls } = countingSnappy()
		const payload = json(256 * 1024)

		// Buffer.equals: toEqual walks large buffers element by element
		expect((await codec.decompress(await codec.compress(payload))).equals(payload)).toBe(true)
		expect(calls).toEqual({ compress: 1, compressSync: 0, uncompress: 1, uncompressSync: 0 })
	})

	it('decodes Xerial chunks in order when only some go async', async () => {
		const { codec, calls } = countingSnappy()
		const parts = [json(1000), json(128 * 1024), Buffer.from('tail')]
		const framed = xerialFrame(...parts.map(p => snappy.compressSync(p)))

		expect((await codec.decompress(framed)).equals(Buffer.concat(parts))).toBe(true)
		expect(calls.uncompressSync).toBe(2)
		expect(calls.uncompress).toBe(1)
	})

	it('yields to the event loop between slices of a large Xerial batch', async () => {
		const { codec, calls } = countingSnappy()
		// snappy-java writes 32 KiB chunks, each small enough to decompress synchronously
		const parts = Array.from({ length: 24 }, () => json(32 * 1024))
		const framed = xerialFrame(...parts.map(p => snappy.compressSync(p)))

		const chunksPerTurn: number[] = []
		let seen = 0
		const endTurn = () => {
			chunksPerTurn.push(calls.uncompressSync - seen)
			seen = calls.uncompressSync
		}
		const tick = () => {
			endTurn()
			if (seen < parts.length) setImmediate(tick)
		}
		setImmediate(tick)
		const decoded = await codec.decompress(framed)
		endTurn()

		expect(decoded.equals(Buffer.concat(parts))).toBe(true)
		expect(calls.uncompressSync).toBe(24)
		expect(Math.max(...chunksPerTurn)).toBeLessThanOrEqual(8)
	})

	it('hands a block with an unreadable length preamble to the async API', async () => {
		const { codec, calls } = countingSnappy()

		await expect(codec.decompress(Buffer.from([0xff, 0xff, 0xff, 0xff, 0xff, 0xff]))).rejects.toThrow()
		expect(calls.uncompress).toBe(1)
	})
})

describe('gzip codec', () => {
	it('round-trips payloads on both sides of the sync size limits', async () => {
		for (const size of [100, 6 * 1024, 48 * 1024, 300 * 1024]) {
			const payload = json(size)
			const compressed = await gzipCodec.compress(payload)
			expect(gunzipSync(compressed).equals(payload)).toBe(true)
			expect((await gzipCodec.decompress(compressed)).equals(payload)).toBe(true)
		}
	})

	it('decodes concatenated members whatever the last trailer claims', async () => {
		const [big, bigger, tail] = [json(200 * 1024), json(300 * 1024), Buffer.from('tail')]

		const understated = Buffer.concat([gzipSync(big), gzipSync(tail)])
		expect((await gzipCodec.decompress(understated)).equals(Buffer.concat([big, tail]))).toBe(true)
		const chunked = Buffer.concat([gzipSync(bigger), gzipSync(big)])
		expect((await gzipCodec.decompress(chunked)).equals(Buffer.concat([bigger, big]))).toBe(true)
	})

	it('does not pin an oversized buffer when bytes after the member overstate the trailer', async () => {
		// zlib ignores bytes after the gzip member; read as ISIZE, these claim 4 GiB of output
		const payload = json(10_000)
		const padded = Buffer.concat([gzipSync(payload), Buffer.from([0, 0, 0, 0xff])])

		const decoded = await gzipCodec.decompress(padded)
		expect(decoded.equals(payload)).toBe(true)
		// Record keys and values are views of this buffer, so its whole backing store stays alive
		expect(decoded.buffer.byteLength).toBeLessThan(2 * payload.length)
	})

	it('rejects corrupt input instead of throwing synchronously', async () => {
		const corrupt = Buffer.from('definitely not a gzip stream')
		let result: Promise<Buffer> | undefined
		expect(() => (result = gzipCodec.decompress(corrupt))).not.toThrow()
		await expect(result).rejects.toThrow()
	})
})

describe('automatic codec registration', () => {
	/** A loader that serves fake modules by name and records the ids it was asked for */
	function fakeLoader(modules: Record<string, unknown>): { loader: ModuleLoader; requested: string[] } {
		const requested: string[] = []
		const loader: ModuleLoader = id => {
			requested.push(id)
			if (id in modules) {
				return modules[id]
			}
			throw Object.assign(new Error(`Cannot find module '${id}'`), { code: 'MODULE_NOT_FOUND' })
		}
		return { loader, requested }
	}

	const fakeSnappyJs = {
		compress: (data: ArrayBuffer | Buffer | Uint8Array) => Buffer.from(data as Uint8Array),
		uncompress: (data: ArrayBuffer | Buffer | Uint8Array) => Buffer.from(data as Uint8Array),
	}

	it('auto-registers the real snappy library via the default loader', async () => {
		const registry = new CodecRegistry()
		const codec = registry.get(CompressionType.Snappy)
		expect(codec).toBeDefined()

		const payload = Buffer.from('default-loader')
		expect(await codec!.decompress(await codec!.compress(payload))).toEqual(payload)
	})

	it('auto-registers snappy when installed', async () => {
		const { loader } = fakeLoader({ snappy: fakeSnappyJs })
		const registry = new CodecRegistry(loader)

		expect(registry.has(CompressionType.Snappy)).toBe(true)
		const codec = registry.get(CompressionType.Snappy)!
		const payload = Buffer.from('auto-snappy')
		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('tries candidate libraries in preference order and falls back to the next one', () => {
		const { loader, requested } = fakeLoader({ snappyjs: fakeSnappyJs })
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeDefined()
		expect(requested).toEqual(['snappy', 'snappyjs'])
	})

	it('unwraps a default export', () => {
		const { loader } = fakeLoader({ snappy: { default: fakeSnappyJs } })
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeDefined()
	})

	it('auto-registers lz4 via the node-lz4 encode/decode API', async () => {
		const { loader } = fakeLoader({ lz4: { encode: (data: Buffer) => data, decode: (data: Buffer) => data } })
		const registry = new CodecRegistry(loader)

		const codec = registry.get(CompressionType.Lz4)!
		const payload = Buffer.from('auto-lz4')
		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('auto-registers zstd via an async compress/decompress API', async () => {
		const { loader } = fakeLoader({
			'@mongodb-js/zstd': {
				compress: async (data: Buffer) => data,
				decompress: async (data: Buffer) => data,
			},
		})
		const registry = new CodecRegistry(loader)

		const codec = registry.get(CompressionType.Zstd)!
		const payload = Buffer.from('auto-zstd')
		expect(await codec.decompress(await codec.compress(payload))).toEqual(payload)
	})

	it('only attempts auto-registration once per compression type', () => {
		const { loader, requested } = fakeLoader({})
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		const attempts = requested.length
		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		expect(registry.has(CompressionType.Snappy)).toBe(false)
		expect(requested.length).toBe(attempts)
	})

	it('skips an installed library that does not expose the expected API and warns', () => {
		const warnSpy = vi.spyOn(process, 'emitWarning').mockImplementation(() => {})
		try {
			const { loader } = fakeLoader({ snappy: { notTheApi: true }, snappyjs: fakeSnappyJs })
			const registry = new CodecRegistry(loader)
			expect(registry.get(CompressionType.Snappy)).toBeDefined()
			expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("found 'snappy'"))
		} finally {
			warnSpy.mockRestore()
		}
	})

	it('warns when an installed library fails to load (e.g. Node version mismatch) and falls back', () => {
		const warnSpy = vi.spyOn(process, 'emitWarning').mockImplementation(() => {})
		try {
			const loader: ModuleLoader = id => {
				if (id === 'snappy') {
					throw new Error('native binding failed to load')
				}
				if (id === 'snappyjs') {
					return fakeSnappyJs
				}
				throw Object.assign(new Error(`Cannot find module '${id}'`), { code: 'MODULE_NOT_FOUND' })
			}
			const registry = new CodecRegistry(loader)
			expect(registry.get(CompressionType.Snappy)).toBeDefined()
			expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("found 'snappy'"))
		} finally {
			warnSpy.mockRestore()
		}
	})

	it('does not warn about libraries that are simply not installed', () => {
		const warnSpy = vi.spyOn(process, 'emitWarning').mockImplementation(() => {})
		try {
			const { loader } = fakeLoader({})
			const registry = new CodecRegistry(loader)
			expect(registry.get(CompressionType.Snappy)).toBeUndefined()
			expect(warnSpy).not.toHaveBeenCalled()
		} finally {
			warnSpy.mockRestore()
		}
	})

	it('does not auto-register when autoRegister is disabled', () => {
		const { loader, requested } = fakeLoader({ snappy: fakeSnappyJs })
		const registry = new CodecRegistry(loader)
		registry.autoRegister = false

		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		expect(requested).toEqual([])

		// Re-enabling makes the next lookup attempt auto-registration
		registry.autoRegister = true
		expect(registry.get(CompressionType.Snappy)).toBeDefined()
	})

	it('manual registration still works after a failed auto-registration attempt', async () => {
		const { loader } = fakeLoader({})
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Snappy)).toBeUndefined()
		registry.register(CompressionType.Snappy, createSnappyCodec(fakeSnappyJs))
		expect(registry.get(CompressionType.Snappy)).toBeDefined()
	})

	it('does not auto-register gzip or none (no lookup needed)', () => {
		const { loader, requested } = fakeLoader({})
		const registry = new CodecRegistry(loader)

		expect(registry.get(CompressionType.Gzip)).toBeDefined()
		expect(registry.get(CompressionType.None)).toBeUndefined()
		expect(requested).toEqual([])
	})

	it('includes an install hint in the missing codec error', () => {
		expect(missingCodecError(CompressionType.Snappy).message).toMatch(/Install one of: snappy, snappyjs/)
		expect(missingCodecError(CompressionType.Lz4).message).toMatch(/lz4-napi, lz4, lz4js/)
		expect(missingCodecError(CompressionType.Zstd).message).toMatch(/@mongodb-js\/zstd, zstd-napi/)
	})
})

describe('async codec async-detection probe', () => {
	async function expectNoUnhandledRejection(create: () => void): Promise<void> {
		const rejections: unknown[] = []
		const handler = (reason: unknown) => rejections.push(reason)
		process.on('unhandledRejection', handler)
		try {
			create()
			// Let the probe promise settle and Node's unhandled-rejection detection run.
			await new Promise(resolve => setTimeout(resolve, 20))
		} finally {
			process.off('unhandledRejection', handler)
		}
		expect(rejections).toHaveLength(0)
	}

	it('does not leak an unhandled rejection when the async snappy probe rejects', async () => {
		await expectNoUnhandledRejection(() => {
			createSnappyCodec({
				compress: () => Promise.reject(new Error('probe failed')),
				uncompress: () => Promise.resolve(Buffer.alloc(0)),
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
			} as any)
		})
	})

	it('does not leak an unhandled rejection when the async zstd probe rejects', async () => {
		await expectNoUnhandledRejection(() => {
			createZstdCodec({
				compress: () => Promise.reject(new Error('probe failed')),
				decompress: () => Promise.resolve(Buffer.alloc(0)),
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
			} as any)
		})
	})
})
