import { execFileSync, spawn } from 'node:child_process'
import { createWriteStream, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { basename, dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { parseArgs, requireArg, userPath, type Args } from './args.js'
import { selectScenarios } from './registry.js'
import { median } from './stats.js'
import type { Headline } from './types.js'

const HERE = dirname(fileURLToPath(import.meta.url))
const RUNNER = join(HERE, 'runner.ts')
const DEFAULT_LOADER = resolve(HERE, '../../../client/src/index.ts')
const PHASE_TIMEOUT_MS = { preload: 20 * 60_000, run: 15 * 60_000, cleanup: 2 * 60_000 }

export const PORT_BASE = Number(process.env.SCENARIO_PORT_BASE ?? 39100)

/** Matches scripts/scenario-cluster.sh: one broker on the base port, three on the next three. */
export function bootstrapFor(brokers: number): string[] {
	if (brokers === 1) return [`localhost:${PORT_BASE}`]
	return Array.from({ length: brokers }, (_, i) => `localhost:${PORT_BASE + 1 + i}`)
}

export interface TreeInfo {
	path: string
	sha: string | null
	branch: string | null
	dirty: boolean | null
}

export function treeInfo(clientPath: string): TreeInfo {
	const cwd = dirname(clientPath)
	const git = (...args: string[]) => {
		try {
			return execFileSync('git', ['-C', cwd, ...args], {
				encoding: 'utf8',
				stdio: ['ignore', 'pipe', 'ignore'],
			}).trim()
		} catch {
			return null
		}
	}
	const status = git('status', '--porcelain', '--', '..')
	return {
		path: clientPath,
		sha: git('rev-parse', '--short=12', 'HEAD'),
		branch: git('rev-parse', '--abbrev-ref', 'HEAD'),
		dirty: status === null ? null : status.length > 0,
	}
}

export interface ScenarioRun {
	headline?: Headline
	detected?: Record<string, boolean>
	errors?: string[]
	failedPhase?: string
	[key: string]: unknown
}

export interface ScenarioSummary {
	name: string
	description: string
	runs: ScenarioRun[]
	headline: Headline
	detected: Record<string, number>
	completedRuns: number
}

export interface ResultFile {
	label: string
	client: TreeInfo
	loader: TreeInfo
	bootstrap: string[]
	brokerCount: number | null
	scale: number
	repeat: number
	startedAt: string
	finishedAt: string | null
	scenarios: Record<string, ScenarioSummary>
}

export interface RunOptions {
	client: string
	scenario: string
	bootstrap: string[]
	out: string
	loader: string
	scale: number
	repeat: number
	stateDir: string
	cpuProf: boolean
	label: string
}

function tsconfigFor(clientPath: string): string | undefined {
	const candidate = resolve(dirname(clientPath), '..', 'tsconfig.json')
	return existsSync(candidate) ? candidate : undefined
}

function runChild(
	clientPath: string,
	args: string[],
	logFile: string,
	timeoutMs: number,
	nodeArgs: string[] = []
): Promise<{ ok: boolean; reason: string }> {
	const tsx = import.meta.resolve('tsx/esm')
	const env: NodeJS.ProcessEnv = { ...process.env }
	const tsconfig = tsconfigFor(clientPath)
	if (tsconfig) env.TSX_TSCONFIG_PATH = tsconfig
	const child = spawn(process.execPath, ['--expose-gc', ...nodeArgs, '--import', tsx, RUNNER, ...args], {
		env,
		stdio: ['ignore', 'pipe', 'pipe'],
	})
	const log = createWriteStream(logFile, { flags: 'a' })
	child.stdout.on('data', (chunk: Buffer) => {
		log.write(chunk)
		process.stdout.write(chunk)
	})
	child.stderr.on('data', (chunk: Buffer) => log.write(chunk))
	return new Promise(resolvePromise => {
		const timer = setTimeout(() => child.kill('SIGKILL'), timeoutMs)
		child.on('close', (code, signal) => {
			clearTimeout(timer)
			log.end()
			resolvePromise({ ok: code === 0, reason: signal ? `killed by ${signal}` : `exit ${code}` })
		})
	})
}

function summarize(runs: ScenarioRun[]): Pick<ScenarioSummary, 'headline' | 'detected' | 'completedRuns'> {
	const done = runs.filter(r => r.headline)
	const headline: Headline = {}
	const keys = [...new Set(done.flatMap(r => Object.keys(r.headline!)))]
	for (const key of keys) {
		const values = done.map(r => r.headline![key]).filter(v => v !== null && v !== undefined)
		const numbers = values.filter((v): v is number => typeof v === 'number')
		if (values.length === 0) {
			headline[key] = null
		} else if (numbers.length === values.length) {
			const m = median(numbers)!
			headline[key] = Number.isInteger(m) ? m : Math.round(m * 1000) / 1000
		} else {
			headline[key] = [...new Set(values.map(v => String(v)))].join(' | ')
		}
	}
	const detected: Record<string, number> = {}
	for (const r of done)
		for (const [k, v] of Object.entries(r.detected ?? {})) detected[k] = (detected[k] ?? 0) + (v ? 1 : 0)
	return { headline, detected, completedRuns: done.length }
}

export async function runScenarios(opts: RunOptions): Promise<ResultFile> {
	const scenarios = selectScenarios(opts.scenario)
	const logDir = opts.out.replace(/\.json$/, '') + '.logs'
	mkdirSync(logDir, { recursive: true })
	mkdirSync(opts.stateDir, { recursive: true })
	const result: ResultFile = {
		label: opts.label,
		client: treeInfo(opts.client),
		loader: treeInfo(opts.loader),
		bootstrap: opts.bootstrap,
		brokerCount: null,
		scale: opts.scale,
		repeat: opts.repeat,
		startedAt: new Date().toISOString(),
		finishedAt: null,
		scenarios: {},
	}
	const save = () => writeFileSync(opts.out, JSON.stringify(result, null, '\t'))
	for (const scenario of scenarios) {
		const summary: ScenarioSummary = {
			name: scenario.name,
			description: scenario.description,
			runs: [],
			headline: {},
			detected: {},
			completedRuns: 0,
		}
		result.scenarios[scenario.id] = summary
		for (let rep = 0; rep < opts.repeat; rep++) {
			const runTag = `r${Date.now().toString(36)}${Math.floor(Math.random() * 1296).toString(36)}`
			const base = join(logDir, `${scenario.id}-${runTag}`)
			const common = ['--scenario', scenario.id, '--setup', `${base}.setup.json`]
			console.log(`== ${opts.label} ${scenario.id}-${scenario.name} run ${rep + 1}/${opts.repeat} (${runTag})`)
			const preload = await runChild(
				opts.loader,
				[
					'--phase',
					'preload',
					...common,
					'--client',
					opts.loader,
					'--bootstrap',
					opts.bootstrap.join(','),
					'--run-tag',
					runTag,
					'--state-dir',
					opts.stateDir,
				],
				`${base}.preload.log`,
				PHASE_TIMEOUT_MS.preload
			)
			if (!preload.ok) {
				summary.runs.push({ runTag, failedPhase: `preload (${preload.reason}), see ${base}.preload.log` })
				save()
				continue
			}
			const nodeArgs = opts.cpuProf ? ['--cpu-prof', '--cpu-prof-dir', `${base}.cpuprof`] : []
			const run = await runChild(
				opts.client,
				[
					'--phase',
					'run',
					...common,
					'--client',
					opts.client,
					'--result',
					`${base}.result.json`,
					'--scale',
					String(opts.scale),
					'--out-dir',
					logDir,
				],
				`${base}.run.log`,
				PHASE_TIMEOUT_MS.run,
				nodeArgs
			)
			if (run.ok && existsSync(`${base}.result.json`)) {
				const r = JSON.parse(readFileSync(`${base}.result.json`, 'utf8')) as ScenarioRun & {
					brokerCount?: number
				}
				result.brokerCount = r.brokerCount ?? result.brokerCount
				summary.runs.push(r)
			} else {
				summary.runs.push({ runTag, failedPhase: `run (${run.reason}), see ${base}.run.log` })
			}
			await runChild(
				opts.loader,
				['--phase', 'cleanup', ...common, '--client', opts.loader],
				`${base}.cleanup.log`,
				PHASE_TIMEOUT_MS.cleanup
			)
			Object.assign(summary, summarize(summary.runs))
			save()
		}
	}
	result.finishedAt = new Date().toISOString()
	save()
	return result
}

export function optionsFromArgs(args: Args, client: string, out: string, label?: string): RunOptions {
	const bootstrap =
		typeof args.bootstrap === 'string' ? args.bootstrap.split(',') : bootstrapFor(Number(args.brokers ?? 1))
	return {
		client,
		scenario: typeof args.scenario === 'string' ? args.scenario : 'all',
		bootstrap,
		out,
		loader: typeof args.loader === 'string' ? userPath(args.loader) : DEFAULT_LOADER,
		scale: Number(args.scale ?? 1),
		repeat: Number(args.repeat ?? 1),
		stateDir:
			typeof args['state-dir'] === 'string' ? userPath(args['state-dir']) : join(dirname(out), '.scenario-state'),
		cpuProf: args['cpu-prof'] === true,
		label: label ?? (typeof args.label === 'string' ? args.label : basename(resolve(dirname(client), '../../..'))),
	}
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
	const args = parseArgs(process.argv.slice(2))
	const client = userPath(requireArg(args, 'client'))
	const out = userPath(requireArg(args, 'out'))
	mkdirSync(dirname(out), { recursive: true })
	const result = await runScenarios(optionsFromArgs(args, client, out))
	for (const [id, s] of Object.entries(result.scenarios)) {
		console.log(`${id}-${s.name}: ${JSON.stringify(s.headline)} detected=${JSON.stringify(s.detected)}`)
	}
	console.log(`wrote ${out}`)
}
