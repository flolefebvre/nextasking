import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRunQueue } from "./with-run-queue.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { runQueueKeyPrefix } from "./run-job-key.js";

const MOVE_TO_ACTIVE_KEY_COUNT_ARGUMENT = "$2\r\n11\r\n";

const ACCOUNT_IDS = ["account-1", "account-2", "account-3"];

const WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS = 10_000;

integrationTest(
  "a connection cut after a fetch moves a run to active, before its reply arrives, starts that run once, right after the reconnect, with no stall, and never hands back the runs in hand, the one the resent fetch took under the same lock included",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    const startedRunIds: string[] = [];
    let openGate = () => undefined as void;
    const gateOpened = new Promise<void>((resolve) => {
      openGate = resolve;
    });
    const nextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("close-account", {
          schema: z.object({ accountId: z.string() }),
          handler: async ({ runId }) => {
            startedRunIds.push(runId);
            if (startedRunIds.length === ACCOUNT_IDS.length) {
              openGate();
            }
            await gateOpened;
          },
        }),
      ],
    });
    const markerKey = `${runQueueKeyPrefix(keyPrefix)}marker`;

    try {
      const runIdsInDispatchOrder: string[] = [];
      for (const accountId of ACCOUNT_IDS) {
        runIdsInDispatchOrder.push((await nextasking.dispatch("close-account", { accountId })).runId);
      }
      const fetchReplyCut = proxy.cutBeforeReplyTo(
        (request) => request.includes(MOVE_TO_ACTIVE_KEY_COUNT_ARGUMENT) && request.includes(markerKey),
      );
      await nextasking.startWorker({ concurrency: ACCOUNT_IDS.length });
      await fetchReplyCut;
      await proxy.restore();
      await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(ACCOUNT_IDS.length);
        },
        { timeout: WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS },
      );
      const stalledCounters = await withRunQueue(redisUrl, keyPrefix, async (runQueue) =>
        (await runQueue.getCompleted(0, -1)).map((completedJob) => completedJob.stalledCounter),
      );

      expect([...startedRunIds].sort()).toEqual([...runIdsInDispatchOrder].sort());
      expect(startedRunIds.at(-1)).toBe(runIdsInDispatchOrder[0]);
      expect(stalledCounters).toEqual([0, 0, 0]);
      expectOneConnectionErrorAndOneConnectionRestored(lines);
      expect(
        lines.filter((line) =>
          ["worker error", "attempt aborted: lock lost", "attempt stalled", "next job hand-back failed"].includes(
            line.message,
          ),
        ),
      ).toEqual([]);
    } finally {
      openGate();
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
);
