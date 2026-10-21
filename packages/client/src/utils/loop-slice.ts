/**
 * Tracks how long synchronous work has run in the current event-loop turn. A turn lasts until the
 * loop next reaches its check phase, so a chain of microtasks, such as polls whose handlers settle
 * without I/O, counts as one turn, and time the loop spent idle or on other work never counts.
 */
export class LoopSlice {
	private startedAt = 0
	private inTurn = false

	constructor(private readonly maxMs: number) {}

	get expired(): boolean {
		const now = performance.now()
		if (!this.inTurn) {
			this.inTurn = true
			this.startedAt = now
			setImmediate(() => {
				this.inTurn = false
			})
		}
		return now - this.startedAt >= this.maxMs
	}

	async yield(): Promise<void> {
		await new Promise<void>(resolve => setImmediate(resolve))
	}
}
