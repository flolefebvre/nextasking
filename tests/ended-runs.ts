import type { DurationInMilliseconds } from "../src/durations.js";
import type { Engine, PruneRequest } from "../src/engine/engine.js";
import type { RunId } from "../src/ids.js";
import { DEFAULT_WORKER_TIMING } from "../src/worker-timing.js";
import { freshLockTokensOf } from "./lock-tokens.js";

export type EndingOutcomeKind = "complete" | "fail";

export const MINUTE_IN_MILLISECONDS: DurationInMilliseconds = 60 * 1000;

export const DAY_IN_MILLISECONDS: DurationInMilliseconds = 24 * 60 * MINUTE_IN_MILLISECONDS;

export const SEND_EMAIL_PRUNE_REQUEST: PruneRequest = {
  task: "send-email",
  completedRetention: DEFAULT_WORKER_TIMING.completedRunRetention,
  failedRetention: DEFAULT_WORKER_TIMING.failedRunRetention,
};

const MOST_RUNS_ENDED_PER_FETCH = 50;

const ENDING_ERROR = { attempt: 1, name: "Error", message: "boom", stack: "Error: boom" };

export function runIdsOf(prefix: string, count: number): RunId[] {
  return Array.from({ length: count }, (_, position) => `${prefix}-${String(position + 1).padStart(5, "0")}`);
}

export async function endRuns(
  engine: Pick<Engine, "enqueue" | "fetch" | "settle">,
  task: string,
  runIds: readonly RunId[],
  outcomeKind: EndingOutcomeKind,
): Promise<void> {
  for (let offset = 0; offset < runIds.length; offset += MOST_RUNS_ENDED_PER_FETCH) {
    const batchRunIds = runIds.slice(offset, offset + MOST_RUNS_ENDED_PER_FETCH);
    await engine.enqueue({ runs: batchRunIds.map((runId) => ({ runId, task, payload: `"${runId}"`, retries: 0 })) });
    const reply = await engine.fetch({
      lockTokens: freshLockTokensOf(batchRunIds.length),
      concurrencyLimitsByTask: {},
      lockDurationInMilliseconds: 120_000,
    });
    if (reply.kind !== "fetched" || reply.lockedRuns.length !== batchRunIds.length) {
      throw new Error(`expected ${String(batchRunIds.length)} locked runs, got ${JSON.stringify(reply)}`);
    }
    await Promise.all(
      reply.lockedRuns.map((lockedRun) =>
        engine.settle({
          ...lockedRun,
          outcome:
            outcomeKind === "complete"
              ? { kind: "complete" }
              : { kind: "fail", reason: "non-retriable", error: ENDING_ERROR, failureNotice: undefined },
        }),
      ),
    );
  }
}
