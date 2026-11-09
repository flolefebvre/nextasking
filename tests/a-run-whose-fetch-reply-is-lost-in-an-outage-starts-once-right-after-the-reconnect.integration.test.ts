import { expect, vi } from "vitest";
import { z } from "zod";

import { createEngine } from "../src/engine/engine.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { freshLockTokensOf } from "./lock-tokens.js";
import { withRedis } from "./with-redis.js";
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

async function lockTokensOf(
  redisUrl: string,
  keyPrefix: string,
  runIds: readonly string[],
): Promise<(string | undefined)[]> {
  return Promise.all(
    runIds.map(async (runId) => (await storedRun(redisUrl, keyPrefix, "close-account", runId))?.lockToken),
  );
}

integrationTest(
  "a connection cut after a fetch locks runs, before its reply arrives, starts each of them once, right after the reconnect, under the lock the resent fetch finds, with no stall",
  { timeout: 40_000 },
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
            await gateOpened;
          },
        }),
      ],
    });

    try {
      await withRedis(redisUrl, (_redis, client) =>
        createEngine(client, keyPrefix).fetch({
          lockTokens: freshLockTokensOf(1),
          concurrencyLimitsByTask: {},
          lockDurationInMilliseconds: WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS,
        }),
      );
      const runIdsInDispatchOrder: string[] = [];
      for (const accountId of ACCOUNT_IDS) {
        runIdsInDispatchOrder.push((await nextasking.dispatch("close-account", { accountId })).runId);
      }
      const fetchReplyCut = proxy.cutBeforeReplyTo((request) => isFetchRequest(request, keyPrefix));
      await nextasking.startWorker({ concurrency: ACCOUNT_IDS.length });
      await fetchReplyCut;
      const lockTokensStoredByTheLostFetch = await lockTokensOf(redisUrl, keyPrefix, runIdsInDispatchOrder);
      await proxy.restore();
      await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
      await vi.waitFor(
        () => {
          expect(startedRunIds).toHaveLength(ACCOUNT_IDS.length);
        },
        { timeout: WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS },
      );
      const lockTokensTheAttemptsRunUnder = await lockTokensOf(redisUrl, keyPrefix, runIdsInDispatchOrder);
      openGate();
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

      expect(lockTokensStoredByTheLostFetch.every((lockToken) => lockToken !== undefined)).toBe(true);
      expect(lockTokensTheAttemptsRunUnder).toEqual(lockTokensStoredByTheLostFetch);
      expect([...startedRunIds].sort()).toEqual([...runIdsInDispatchOrder].sort());
      expect(stalledCounters).toEqual([0, 0, 0]);
      expectOneConnectionErrorAndOneConnectionRestored(lines);
      expect(lines.filter((line) => ["attempt aborted: lock lost", "attempt stalled"].includes(line.message))).toEqual(
        [],
      );
    } finally {
      openGate();
      await closeNextaskingAndCheckItsDuration(nextasking);
      await proxy.stop();
    }
  },
);
