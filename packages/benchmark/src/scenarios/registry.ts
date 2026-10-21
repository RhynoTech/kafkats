import { s1Fairness } from './s1-fairness.js'
import { s2TmPod } from './s2-tm-pod.js'
import { s3Rekey } from './s3-rekey.js'
import { s4DecodeCpu } from './s4-decode-cpu.js'
import { s5Memory } from './s5-memory.js'
import { s6ProducerSplit } from './s6-producer-split.js'
import type { Scenario } from './types.js'

export const SCENARIOS: readonly Scenario[] = [s1Fairness, s2TmPod, s3Rekey, s4DecodeCpu, s5Memory, s6ProducerSplit]

/** Accepts `all`, ids (`s1`), names (`fairness`) or `s1-fairness`, comma-separated. */
export function selectScenarios(selector: string): Scenario[] {
	if (selector === 'all') return [...SCENARIOS]
	return selector.split(',').map(token => {
		const t = token.trim()
		const found = SCENARIOS.find(s => s.id === t || s.name === t || `${s.id}-${s.name}` === t)
		if (!found) {
			const known = SCENARIOS.map(s => `${s.id}-${s.name}`).join(', ')
			throw new Error(`unknown scenario '${t}' (known: ${known})`)
		}
		return found
	})
}
