import { describe, expect, it } from 'vitest'

import { createRequire } from 'node:module'

import { crc32c, crc32cJS, crc32cSigned, verifyCrc32c } from '@/utils/crc32c.js'

describe('crc32c', () => {
	it('matches known CRC32C vector', () => {
		const data = Buffer.from('123456789', 'utf-8')
		const checksum = crc32c(data)
		expect(checksum).toBe(0xe3069283)
		expect(verifyCrc32c(data, checksum)).toBe(true)
	})

	it('supports slicing with start and length', () => {
		const data = Buffer.from('xx123456789yy', 'utf-8')
		const checksum = crc32c(data, 2, 9)
		expect(checksum).toBe(0xe3069283)
	})

	it('returns signed values when requested', () => {
		const data = Buffer.from('123456789', 'utf-8')
		const signed = crc32cSigned(data)
		expect(signed).toBe(0xe3069283 - 0x100000000)
	})

	it('verifies mismatched checksums', () => {
		const data = Buffer.from('123456789', 'utf-8')
		expect(verifyCrc32c(data, 0)).toBe(false)
	})

	it('handles empty buffers', () => {
		const checksum = crc32c(Buffer.alloc(0))
		expect(checksum).toBe(0)
	})

	it('supports checksum of partial buffer', () => {
		const data = Buffer.from('abcdef', 'utf-8')
		const checksum = crc32c(data, 1, 3)
		expect(checksum).toBe(crc32c(Buffer.from('bcd', 'utf-8')))
	})
})

describe('crc32cJS fallback', () => {
	const native = (createRequire(import.meta.url)('@node-rs/crc32') as { crc32c: (data: Uint8Array) => number }).crc32c

	it('matches the known CRC32C vector', () => {
		expect(crc32cJS(Buffer.from('123456789', 'utf-8'), 0, 9)).toBe(0xe3069283)
	})

	it('matches the native implementation across lengths and byte offsets', () => {
		const pool = Buffer.alloc(4200)
		for (let i = 0; i < pool.length; i++) pool[i] = (i * 2654435761) >>> 24
		for (const length of [0, 1, 15, 16, 17, 63, 64, 65, 79, 100, 255, 256, 1000, 4096]) {
			for (const offset of [0, 1, 2, 3, 5, 13]) {
				const view = pool.subarray(offset, offset + length)
				const expected = native(view) >>> 0
				expect(crc32cJS(view, 0, length)).toBe(expected)
				expect(crc32cJS(pool, offset, offset + length)).toBe(expected)
			}
		}
	})
})
