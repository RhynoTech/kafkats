export interface Distribution {
	n: number
	mean: number | null
	p50: number | null
	p90: number | null
	p99: number | null
	max: number | null
}

export function round(x: number, digits = 1): number {
	const f = 10 ** digits
	return Math.round(x * f) / f
}

export function quantile(sorted: readonly number[], p: number): number | null {
	if (sorted.length === 0) return null
	return sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] ?? null
}

export function distribution(values: readonly number[], digits = 1): Distribution {
	if (values.length === 0) return { n: 0, mean: null, p50: null, p90: null, p99: null, max: null }
	const sorted = [...values].sort((a, b) => a - b)
	let sum = 0
	for (const v of sorted) sum += v
	const q = (p: number) => round(quantile(sorted, p)!, digits)
	return {
		n: sorted.length,
		mean: round(sum / sorted.length, digits),
		p50: q(0.5),
		p90: q(0.9),
		p99: q(0.99),
		max: round(sorted[sorted.length - 1]!, digits),
	}
}

/** 0 when every partition got the same share, approaching 1 when one partition got everything. */
export function gini(values: readonly number[]): number {
	const sorted = [...values].sort((a, b) => a - b)
	const n = sorted.length
	let sum = 0
	for (const v of sorted) sum += v
	if (n === 0 || sum === 0) return 0
	let acc = 0
	for (let i = 0; i < n; i++) acc += (2 * (i + 1) - n - 1) * sorted[i]!
	return acc / (n * sum)
}

export function median(values: readonly number[]): number | null {
	if (values.length === 0) return null
	const sorted = [...values].sort((a, b) => a - b)
	const mid = Math.floor(sorted.length / 2)
	return sorted.length % 2 === 0 ? (sorted[mid - 1]! + sorted[mid]!) / 2 : sorted[mid]!
}

export const MiB = 1024 * 1024
