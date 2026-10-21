import { isAbsolute, resolve } from 'node:path'

export interface Args {
	_: string[]
	[key: string]: string | boolean | undefined | string[]
}

export function parseArgs(argv: string[]): Args {
	const out: Args = { _: [] }
	for (let i = 0; i < argv.length; i++) {
		const raw = argv[i]!
		if (raw === '--') continue
		if (!raw.startsWith('--')) {
			out._.push(raw)
			continue
		}
		const eq = raw.indexOf('=')
		if (eq > 0) {
			out[raw.slice(2, eq)] = raw.slice(eq + 1)
			continue
		}
		const next = argv[i + 1]
		if (next !== undefined && !next.startsWith('--')) {
			out[raw.slice(2)] = next
			i++
		} else {
			out[raw.slice(2)] = true
		}
	}
	return out
}

export function requireArg(args: Args, key: string): string {
	const value = args[key]
	if (typeof value !== 'string' || value.length === 0) throw new Error(`missing --${key}`)
	return value
}

/** pnpm -C changes the working directory; resolve user paths against where the command was typed. */
export function userPath(path: string): string {
	return isAbsolute(path) ? path : resolve(process.env.INIT_CWD ?? process.cwd(), path)
}
