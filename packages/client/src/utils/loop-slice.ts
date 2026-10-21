/** Tracks how long synchronous work has run since it last yielded to the event loop. */
export class LoopSlice {
	private startedAt = performance.now()

	constructor(private readonly maxMs: number) {}

	get expired(): boolean {
		return performance.now() - this.startedAt >= this.maxMs
	}

	async yield(): Promise<void> {
		await new Promise<void>(resolve => setImmediate(resolve))
		this.startedAt = performance.now()
	}
}
