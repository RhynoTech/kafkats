import { readFileSync, writeFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { parseArgs, userPath } from './args.js'
import type { ResultFile } from './cli.js'
import { SCENARIOS } from './registry.js'

function variant(r: ResultFile): string {
	const c = r.client
	return `${r.label} (${c.branch ?? '?'}@${c.sha ?? '?'}${c.dirty ? '*' : ''}, ${r.brokerCount ?? '?'} broker)`
}

function cell(value: unknown): string {
	if (value === null || value === undefined) return '–'
	return String(value).replace(/\|/g, '\\|')
}

export function renderReport(results: ResultFile[]): string {
	const lines: string[] = ['# kafkats scenario benchmarks', '']
	const brokers = [...new Set(results.map(r => `${r.brokerCount ?? '?'} broker(s) at ${r.bootstrap.join(',')}`))]
	lines.push(`Cluster: ${brokers.join('; ')}. Scale ${[...new Set(results.map(r => r.scale))].join(', ')}.`)
	lines.push('Each value is the median over completed runs; `detected` counts runs where the signal fired.', '')
	for (const scenario of SCENARIOS) {
		const rows = results.filter(r => r.scenarios[scenario.id])
		if (rows.length === 0) continue
		lines.push(`## ${scenario.id} ${scenario.name}`, '', scenario.description, '')
		const keys = [...new Set(rows.flatMap(r => Object.keys(r.scenarios[scenario.id]!.headline)))]
		lines.push(`| variant | runs | ${keys.join(' | ')} | detected |`)
		lines.push(`|---|---|${keys.map(() => '---').join('|')}|---|`)
		for (const r of rows) {
			const s = r.scenarios[scenario.id]!
			const detected = Object.entries(s.detected)
				.map(([k, n]) => `${k} ${n}/${s.completedRuns}`)
				.join(', ')
			lines.push(
				`| ${cell(variant(r))} | ${s.completedRuns}/${s.runs.length} | ${keys.map(k => cell(s.headline[k])).join(' | ')} | ${cell(detected)} |`
			)
		}
		const problems = rows.flatMap(r =>
			r.scenarios[scenario.id]!.runs.flatMap(run => [
				...(run.failedPhase ? [`${r.label}: ${run.failedPhase}`] : []),
				...(run.errors ?? []).slice(0, 3).map(e => `${r.label}: ${e}`),
			])
		)
		if (problems.length > 0) {
			lines.push('', 'Errors:', ...problems.slice(0, 12).map(p => `- ${p.slice(0, 300)}`))
		}
		lines.push('')
	}
	return lines.join('\n')
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	const args = parseArgs(process.argv.slice(2))
	const results = args._.map(f => JSON.parse(readFileSync(userPath(f), 'utf8')) as ResultFile)
	const markdown = renderReport(results)
	if (typeof args.out === 'string') writeFileSync(userPath(args.out), markdown)
	else process.stdout.write(markdown)
}
