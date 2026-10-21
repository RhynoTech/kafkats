import { readFileSync, writeFileSync } from 'node:fs'
import { pathToFileURL } from 'node:url'

import {
	clusterId,
	connect,
	createRunTopics,
	deleteTopics,
	ensureDataset,
	waitForLeaders,
	warmCoordinators,
} from './admin.js'
import { parseArgs, requireArg } from './args.js'
import { selectScenarios } from './registry.js'
import type { Lib, ScenarioSetup } from './types.js'

// One phase of one scenario, in a child process so each measurement starts from a clean heap and
// loads exactly one client tree (tsx resolves the tree's `@/` imports from TSX_TSCONFIG_PATH).
//   preload: loader client creates datasets and this run's topics, writes the setup file
//   run:     client under test measures, writes the result file
//   cleanup: loader client deletes this run's topics

const args = parseArgs(process.argv.slice(2))
const phase = requireArg(args, 'phase')
const scenario = selectScenarios(requireArg(args, 'scenario'))[0]!
const clientPath = requireArg(args, 'client')
const setupPath = requireArg(args, 'setup')
const log = (message: string) => console.log(`[${new Date().toISOString()}] ${message}`)

const lib = (await import(pathToFileURL(clientPath).href)) as Lib

if (phase === 'preload') {
	const bootstrap = requireArg(args, 'bootstrap').split(',')
	const runTag = requireArg(args, 'run-tag')
	const client = await connect(lib, bootstrap, `scn-loader-${runTag}`)
	await warmCoordinators(client)
	const datasets: ScenarioSetup['datasets'] = {}
	for (const spec of scenario.datasets()) {
		datasets[spec.id] = await ensureDataset(lib, client, spec, requireArg(args, 'state-dir'), log)
	}
	const datasetTopics = scenario.datasets().flatMap(d => d.topics)
	const leaders = datasetTopics.length > 0 ? await waitForLeaders(client, datasetTopics) : {}
	const runTopics = scenario.runTopics(runTag)
	await createRunTopics(client, runTopics, leaders)
	Object.assign(leaders, runTopics.length > 0 ? await waitForLeaders(client, runTopics) : {})
	const metadata = await client.getMetadata()
	const setup: ScenarioSetup = {
		scenario: scenario.id,
		runTag,
		bootstrap,
		clusterId: await clusterId(client),
		brokerCount: metadata.brokers.size,
		datasets,
		leaders,
		runTopics: runTopics.map(t => t.name),
	}
	writeFileSync(setupPath, JSON.stringify(setup, null, '\t'))
	await client.disconnect()
	log(`preload ${scenario.id} ready: ${Object.keys(datasets).join(', ') || 'no datasets'}`)
} else if (phase === 'run') {
	const setup = JSON.parse(readFileSync(setupPath, 'utf8')) as ScenarioSetup
	const started = new Date()
	const output = await scenario.run({
		lib,
		clientPath,
		setup,
		scale: Number(args.scale ?? 1),
		outDir: requireArg(args, 'out-dir'),
		log,
	})
	const result = {
		scenario: scenario.id,
		name: scenario.name,
		runTag: setup.runTag,
		brokerCount: setup.brokerCount,
		startedAt: started.toISOString(),
		durationMs: Date.now() - started.getTime(),
		...output,
	}
	writeFileSync(requireArg(args, 'result'), JSON.stringify(result, null, '\t'))
	log(`run ${scenario.id}: ${JSON.stringify(output.headline)}`)
	if (output.errors.length > 0) log(`run ${scenario.id} errors: ${output.errors.join(' | ')}`)
} else if (phase === 'cleanup') {
	const setup = JSON.parse(readFileSync(setupPath, 'utf8')) as ScenarioSetup
	const client = await connect(lib, setup.bootstrap, `scn-cleanup-${setup.runTag}`)
	await deleteTopics(client, setup.runTopics)
	await client.disconnect()
} else {
	throw new Error(`unknown phase ${phase}`)
}
// Clients under test may leave timers or sockets behind; the phase is done either way.
process.exit(0)
