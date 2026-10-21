export type Rng = () => number

export function mulberry32(seed: number): Rng {
	let a = seed >>> 0
	return () => {
		a = (a + 0x6d2b79f5) | 0
		let t = Math.imul(a ^ (a >>> 15), 1 | a)
		t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
		return ((t ^ (t >>> 14)) >>> 0) / 4294967296
	}
}

export function pick<T>(rng: Rng, items: readonly T[]): T {
	return items[Math.floor(rng() * items.length)] as T
}

export function digits(rng: Rng, n: number): string {
	let out = ''
	while (out.length < n) out += String(Math.floor(rng() * 1e9)).padStart(9, '0')
	return out.slice(0, n)
}

export function hex(rng: Rng, n: number): string {
	let out = ''
	while (out.length < n) out += ((rng() * 4294967296) >>> 0).toString(16).padStart(8, '0')
	return out.slice(0, n)
}

export function uuid(rng: Rng): string {
	return `${hex(rng, 8)}-${hex(rng, 4)}-4${hex(rng, 3)}-a${hex(rng, 3)}-${hex(rng, 12)}`
}

export function randomBytes(rng: Rng, n: number): Buffer {
	const out = Buffer.allocUnsafe(n)
	let i = 0
	for (; i + 4 <= n; i += 4) out.writeUInt32LE((rng() * 4294967296) >>> 0, i)
	for (; i < n; i++) out[i] = (rng() * 256) >>> 0
	return out
}
