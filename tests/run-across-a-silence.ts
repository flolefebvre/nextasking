import { expect, vi } from "vitest";

import type { StartWorkerOptions } from "../src/index.js";
import { fixtureRecordsOf } from "./fixture-records.js";
import { waitForLogMessage } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { createWorkerProcessNextasking } from "./worker-process-nextasking.js";

const LOCK_DURATION_IN_MILLISECONDS = 6_000;

export type RunAcrossASilence = { proxy: TcpProxy; workerProcess: WorkerProcess };

export type RunAcrossASilenceOptions = {
  redisUrl: string;
  keyPrefix: string;
  startWorkerOptions?: StartWorkerOptions;
};

export async function logLinesOfARunCompletedInItsFirstAttemptAcrossASilence(
  { redisUrl, keyPrefix, startWorkerOptions }: RunAcrossASilenceOptions,
  silence: (runAcrossASilence: RunAcrossASilence) => Promise<void>,
): Promise<readonly RecordedLogLine[]> {
  const proxy = await startTcpProxy(redisUrl);
  try {
    const workerProcess = await startWorkerProcess({
      redisUrl,
      keyPrefix,
      connection: { url: proxy.url, giveUpAfter: "1s" },
      timing: {
        lockDurationInMilliseconds: LOCK_DURATION_IN_MILLISECONDS,
        lockTimeLeftThatTriggersRenewalInMilliseconds: LOCK_DURATION_IN_MILLISECONDS - 1_000,
      },
      ...(startWorkerOptions === undefined ? {} : { startWorkerOptions }),
    });
    const nextasking = createWorkerProcessNextasking(redisUrl, keyPrefix);
    try {
      const { runId } = await nextasking.dispatch("wait-then-record", {
        waitInMilliseconds: LOCK_DURATION_IN_MILLISECONDS + 2_000,
      });
      await vi.waitFor(
        async () => {
          expect(await fixtureRecordsOf(redisUrl, keyPrefix, "wait-then-record")).toHaveLength(1);
        },
        { timeout: 10_000 },
      );

      await silence({ proxy, workerProcess });
      await waitForLogMessage(workerProcess.logLines, "run completed");

      expect(await fixtureRecordsOf(redisUrl, keyPrefix, "wait-then-record")).toEqual([
        { step: "wait", attempt: 1 },
        { step: "record", attempt: 1 },
      ]);
      expect(workerProcess.logLines.map(({ message }) => message)).not.toContain("attempt aborted: lock lost");
      expect(workerProcess.logLines).toContainEqual(
        expect.objectContaining({ message: "run completed", fields: expect.objectContaining({ runId }) }),
      );
      return workerProcess.logLines;
    } finally {
      await nextasking.close();
      await workerProcess.stop();
    }
  } finally {
    await proxy.stop();
  }
}

export function connectionLinesOf(lines: readonly RecordedLogLine[]): RecordedLogLine[] {
  return lines.filter(({ message }) => message.startsWith("connection "));
}
