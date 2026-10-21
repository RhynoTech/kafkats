import { describe, expect, it } from 'vitest'

import { Decoder } from '@/protocol/primitives/decoder.js'
import { Encoder } from '@/protocol/primitives/encoder.js'
import {
	decodeUVarInt,
	decodeVarInt,
	decodeVarLong,
	encodeUVarInt,
	encodeVarInt,
	encodeVarLong,
	varIntSize,
	varLongSize,
	uvarIntSize,
	zigZagDecode32,
	zigZagDecode64,
	zigZagEncode32,
	zigZagEncode64,
} from '@/protocol/primitives/varint.js'

describe('varint helpers', () => {
	it('zigzag encodes and decodes 32-bit values', () => {
		const values = [0, -1, 1, -2, 2, 123456, -123456]
		for (const value of values) {
			const encoded = zigZagEncode32(value)
			const decoded = zigZagDecode32(encoded)
			expect(decoded).toBe(value)
		}
	})

	it('zigzag encodes and decodes 64-bit values', () => {
		const values = [0n, -1n, 1n, -2n, 2n, 9876543210n, -9876543210n]
		for (const value of values) {
			const encoded = zigZagEncode64(value)
			const decoded = zigZagDecode64(encoded)
			expect(decoded).toBe(value)
		}
	})

	it('roundtrips VARINT and VARLONG', () => {
		const intValues = [0, -1, 1, 12345, -12345, 0x3fffffff, -0x3fffffff]
		for (const value of intValues) {
			const encoded = encodeVarInt(value)
			const decoded = decodeVarInt(encoded)
			expect(decoded.value).toBe(value)
			expect(encoded.length).toBe(varIntSize(value))
		}

		const maxLong = BigInt('0x7fffffffffffffff')
		const longValues = [0n, -1n, 1n, 9876543210n, -9876543210n, maxLong, -maxLong]
		for (const value of longValues) {
			const encoded = encodeVarLong(value)
			const decoded = decodeVarLong(encoded)
			expect(decoded.value).toBe(value)
			expect(encoded.length).toBe(varLongSize(value))
		}
	})

	it('roundtrips UVARINT and sizes', () => {
		const values = [0, 1, 127, 128, 300, 16384, 0x7fffffff]
		for (const value of values) {
			const encoded = encodeUVarInt(value)
			const decoded = decodeUVarInt(encoded)
			expect(decoded.value).toBe(value)
			expect(encoded.length).toBe(uvarIntSize(value))
		}
	})

	it('rejects negative UVARINT', () => {
		expect(() => encodeUVarInt(-1)).toThrow('UVARINT cannot encode negative numbers')
	})

	it('fails decoding UVARINT when buffer underflows', () => {
		expect(() => decodeUVarInt(Buffer.alloc(0))).toThrow('Buffer underflow while reading UVARINT')
	})

	it('fails decoding VARLONG when buffer underflows', () => {
		expect(() => decodeVarLong(Buffer.alloc(0))).toThrow('Buffer underflow while reading VARLONG')
	})

	it('rejects UVARINT that is too long', () => {
		const bytes = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80])
		expect(() => decodeUVarInt(bytes)).toThrow('UVARINT is too long for 32-bit integer')
	})

	it('rejects VARLONG that is too long', () => {
		const bytes = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])
		expect(() => decodeVarLong(bytes)).toThrow('VARLONG is too long for 64-bit integer')
	})

	it('decodes UVARINT values >= 2^31 as unsigned, not as a negative int32', () => {
		// 0xFFFFFFFF (4_294_967_295) encodes to these 5 bytes.
		const maxU32 = Buffer.from([0xff, 0xff, 0xff, 0xff, 0x0f])
		expect(decodeUVarInt(maxU32).value).toBe(4_294_967_295)
		expect(new Decoder(maxU32).readUVarInt()).toBe(4_294_967_295)

		// 2^31 exactly (0x80000000) — the boundary where a signed int32 flips negative.
		const twoPow31 = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x08])
		expect(decodeUVarInt(twoPow31).value).toBe(2_147_483_648)
		expect(new Decoder(twoPow31).readUVarInt()).toBe(2_147_483_648)
	})

	it('Decoder.readVarLong rejects a VARLONG that overflows 64 bits', () => {
		// 10 continuation bytes never terminate within 64 bits. Without a shift guard the
		// decoder over-reads past the value (failing later with a misleading underflow);
		// it should reject it as too long instead.
		const bytes = Buffer.from([0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80])
		expect(() => new Decoder(bytes).readVarLong()).toThrow('VARLONG is too long for 64-bit integer')
	})

	it('Decoder.readVarLong still round-trips valid 64-bit values (guard does not over-reject)', () => {
		const values = [0n, 1n, -1n, 2n ** 62n, -(2n ** 62n), 9223372036854775807n, -9223372036854775808n]
		for (const v of values) {
			expect(new Decoder(encodeVarLong(v)).readVarLong()).toBe(v)
		}
	})

	it('Decoder.readVarLong round-trips values around the 7-byte (49-bit) number fast path', () => {
		const edges = [2n ** 47n, 2n ** 48n, 2n ** 49n, 2n ** 55n, 2n ** 56n]
		const values = edges.flatMap(e => [e - 1n, e, e + 1n, -e - 1n, -e, -e + 1n])
		values.push(63n, 64n, -64n, -65n, 8191n, 8192n, -8192n, -8193n, 1_792_559_112_263n)
		for (const v of values) {
			const decoder = new Decoder(Buffer.concat([encodeVarLong(v), Buffer.from([0x2a])]))
			expect(decoder.readVarLong()).toBe(v)
			expect(decoder.readInt8()).toBe(0x2a)
		}
	})
})

describe('32-bit zigzag boundary values (must stay unsigned, not overflow int32)', () => {
	it('zigZagEncode32 matches the protobuf/Kafka zigzag spec for large magnitudes', () => {
		// zigzag(n) = 2n for n >= 0, zigzag(n) = -2n - 1 for n < 0 — always in [0, 2^32).
		expect(zigZagEncode32(2 ** 30)).toBe(2 ** 31)
		expect(zigZagEncode32(2 ** 31 - 1)).toBe(2 ** 32 - 2)
		expect(zigZagEncode32(-(2 ** 30))).toBe(2 ** 31 - 1)
		expect(zigZagEncode32(-(2 ** 31))).toBe(2 ** 32 - 1)
	})

	it('round-trips VARINT boundary values through encode/decode and sizes', () => {
		const values = [0, -1, 2 ** 30, 2 ** 31 - 1, -(2 ** 30), -(2 ** 31)]
		for (const value of values) {
			const encoded = encodeVarInt(value)
			expect(decodeVarInt(encoded).value).toBe(value)
			expect(encoded.length).toBe(varIntSize(value))
			expect(zigZagDecode32(zigZagEncode32(value))).toBe(value)
		}
	})

	it('emits the spec 5-byte uvarint encodings at the boundaries', () => {
		// zigzag(2^30) === 2^31 → uvarint 80 80 80 80 08
		expect([...encodeVarInt(2 ** 30)]).toEqual([0x80, 0x80, 0x80, 0x80, 0x08])
		// zigzag(-2^31) === 2^32 - 1 → uvarint ff ff ff ff 0f
		expect([...encodeVarInt(-(2 ** 31))]).toEqual([0xff, 0xff, 0xff, 0xff, 0x0f])
	})

	it('Encoder.writeVarInt / Decoder.readVarInt round-trip boundary values', () => {
		const values = [2 ** 30, 2 ** 31 - 1, -(2 ** 30), -(2 ** 31)]
		for (const value of values) {
			const enc = new Encoder()
			enc.writeVarInt(value)
			expect(new Decoder(enc.toBuffer()).readVarInt()).toBe(value)
		}
	})

	it('encodeUVarInt and uvarIntSize accept the full uint32 range', () => {
		expect([...encodeUVarInt(2 ** 32 - 1)]).toEqual([0xff, 0xff, 0xff, 0xff, 0x0f])
		expect(uvarIntSize(2 ** 32 - 1)).toBe(5)
		expect(uvarIntSize(2 ** 31)).toBe(5)
		expect(decodeUVarInt(encodeUVarInt(2 ** 31)).value).toBe(2 ** 31)
	})
})
