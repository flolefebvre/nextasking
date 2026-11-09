import { expect } from "vitest";

import { fixtureRecordsOf } from "./fixture-records.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const FAST_STALLS = {
  lockDurationInMilliseconds: 3000,
  lockTimeLeftThatTriggersRenewalInMilliseconds: 2500,
  tickIntervalInMilliseconds: 500,
};

export type StallCheckedWorkerProcesses = {
  nextasking: ReturnType<typeof createWorkerProcessNextasking>;
  stallCheckingProcess: WorkerProcess;
  blockingProcess: WorkerProcess;
  stop: () => Promise<void>;
};

export async function startStallCheckedWorkerProcesses(
  redisUrl: string,
  keyPrefix: string,
): Promise<StallCheckedWorkerProcesses> {
  const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
  const stallCheckingProcess = await startWorkerProcess({
    redisUrl,
    keyPrefix,
    timing: FAST_STALLS,
    startWorkerOptions: { concurrency: 1 },
  });
  const blockingProcess = await (async () => {
    await nextasking.dispatch("wait-for-abort", {});
    await expect
      .poll(() => fixtureRecordsOf(redisUrl, keyPrefix, "wait-for-abort"), { timeout: 10_000 })
      .toHaveLength(1);
    return startWorkerProcess({ redisUrl, keyPrefix, timing: FAST_STALLS });
  })().catch(async (error: unknown) => {
    await stallCheckingProcess.stop();
    await nextasking.close();
    throw error;
  });
  return {
    nextasking,
    stallCheckingProcess,
    blockingProcess,
    stop: async () => {
      await Promise.all([stallCheckingProcess.stop(), blockingProcess.stop()]);
      await nextasking.close();
    },
  };
}
