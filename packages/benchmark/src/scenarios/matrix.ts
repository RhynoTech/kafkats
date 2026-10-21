import { existsSync, mkdirSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'

import { parseArgs, requireArg, userPath } from './args.js'
import { optionsFromArgs, runScenarios, type ResultFile } from './cli.js'
import { renderReport } from './report.js'

// Runs the selected scenarios against each worktree in turn (never concurrently, so they do not
// compete for the broker or CPU), then writes one JSON per worktree and a combined summary.md.

const args = parseArgs(process.argv.slice(2))
const outDir = userPath(requireArg(args, 'out-dir'))
const worktrees = args._.map(userPath)
if (worktrees.length === 0) throw new Error('usage: scenarios:matrix --out-dir <dir> [--brokers 1|3] <worktree>...')
mkdirSync(outDir, { recursive: true })

const results: ResultFile[] = []
for (const worktree of worktrees) {
	const client = join(worktree, 'packages/client/src/index.ts')
	if (!existsSync(client)) throw new Error(`${client} not found`)
	const label = basename(worktree)
	const out = join(outDir, `${label}.json`)
	results.push(await runScenarios(optionsFromArgs(args, client, out, label)))
	writeFileSync(join(outDir, 'summary.md'), renderReport(results))
}
console.log(`wrote ${join(outDir, 'summary.md')}`)
