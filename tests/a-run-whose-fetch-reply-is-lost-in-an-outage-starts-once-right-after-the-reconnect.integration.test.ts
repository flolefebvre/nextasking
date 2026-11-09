import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { isFetchRequest } from "./engine-requests.js";
import { storedRun } from "./stored-run.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";

const ACCOUNT_IDS = ["account-1", "account-2", "account-3"];

const WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS = 10_000;

integrationTest(
  "a connection cut after a fetch locks runs, before its reply arrives, starts each of them once, right after the reconnect, under the lock the resent fetch finds, with no stall",
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

    try {
      const runIdsInDispatchOrder: string[] = [];
      for (const accountId of ACCOUNT_IDS) {
        runIdsInDispatchOrder.push((await nextasking.dispatch("close-account", { accountId })).runId);
      }
      const fetchReplyCut = proxy.cutBeforeReplyTo((request) => isFetchRequest(request, keyPrefix));
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
      const stalledCounters = await Promise.all(
        runIdsInDispatchOrder.map(async (runId) =>
          Number((await storedRun(redisUrl, keyPrefix, "close-account", runId))?.stalls),
        ),
      );

      expect([...startedRunIds].sort()).toEqual([...runIdsInDispatchOrder].sort());
      expect(stalledCounters).toEqual([0, 0, 0]);
      expectOneConnectionErrorAndOneConnectionRestored(lines);
      expect(
        lines.filter((line) =>
          ["worker error", "attempt aborted: lock lost", "attempt stalled"].includes(line.message),
        ),
      ).toEqual([]);
    } finally {
      openGate();
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
);
