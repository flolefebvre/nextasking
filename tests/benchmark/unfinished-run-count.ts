import type { RunState } from "../../src/index.js";

export type RunCountsByTask = Record<string, Record<RunState, number>>;

const ENDED_STATES: ReadonlySet<RunState> = new Set(["completed", "failed"]);

export function unfinishedRunCountIn(runCountsByTask: RunCountsByTask): number {
  return Object.values(runCountsByTask)
    .flatMap((runCountsByState) => Object.entries(runCountsByState))
    .filter(([state]) => !ENDED_STATES.has(state as RunState))
    .reduce((unfinishedRunCount, [, runCount]) => unfinishedRunCount + runCount, 0);
}
