import { Redis } from "ioredis";
import { expect, vi } from "vitest";
import { z } from "zod";

import { integrationTest } from "../tests/integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "../tests/recording-logger.js";
import { runJobKey } from "../tests/run-job-key.js";
import { defineScheduledTask, defineTask } from "./definitions.js";
import { deriveJobId } from "./ids.js";
import { closeNextasking, createNextasking } from "./registry.js";
import { DEFAULT_WORKER_TIMING } from "./worker-timing.js";

const { lockDurationInMilliseconds } = DEFAULT_WORKER_TIMING;

async function serverTimeInMillisecondsOf(redis: Redis): Promise<number> {
  const [seconds, microseconds] = await redis.time();
  return Number(seconds) * 1000 + Math.floor(Number(microseconds) / 1000);
}

function openGate() {
  let open = () => undefined as void;
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { open, opened };
}

integrationTest(
  "slot holders and waiting runs are stored under the run queue, per task and encoded key",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const gate = openGate();
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string(), shouldWaitForGate: z.boolean() }),
      concurrency: { key: (payload) => payload.accountId, limit: 1 },
      handler: async ({ payload }) => {
        if (payload.shouldWaitForGate) {
          await gate.opened;
        }
      },
    });
    const purgeSessions = defineScheduledTask("reports:purge", {
      schedule: { cron: "0 0 1 1 *" },
      concurrency: { limit: 1 },
      handler: async () => {
        await gate.opened;
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [rebuildIndex, purgeSessions],
    });
    const redis = new Redis(redisUrl, { lazyConnect: true });
    redis.on("error", () => undefined);
    const runQueueKey = `${keyPrefix}:runs`;

    try {
      await redis.connect();
      await nextasking.startWorker();
      const beforeTakeAt = await serverTimeInMillisecondsOf(redis);
      const holdingRun = await nextasking.dispatch("rebuild-index", { accountId: "eu:7 b", shouldWaitForGate: true });
      await waitForLogMessage(lines, "attempt started");
      const waitingRun = await nextasking.dispatch("rebuild-index", { accountId: "eu:7 b", shouldWaitForGate: false });
      await waitForLogMessage(lines, "run waiting for a concurrency slot");
      await nextasking.dispatch("reports:purge");
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "attempt started")).toHaveLength(2);
        },
        { timeout: 10_000 },
      );
      const holdersOfKey = await redis.zrange(
        `${runQueueKey}:concurrency-slots:rebuild-index:eu%3A7%20b`,
        0,
        -1,
        "WITHSCORES",
      );
      const afterReadAt = await serverTimeInMillisecondsOf(redis);
      const holdersOfTask = await redis.zrange(`${runQueueKey}:concurrency-slots:reports%3Apurge`, 0, -1);
      const waitingJobIds = await redis.lrange(`${runQueueKey}:concurrency-waiting:rebuild-index:eu%3A7%20b`, 0, -1);
      const holdingRunLockToken = await redis.get(`${runJobKey(keyPrefix, "rebuild-index", holdingRun.runId)}:lock`);
      gate.open();
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(3);
        },
        { timeout: 10_000 },
      );

      expect(holdersOfKey).toEqual([holdingRunLockToken, expect.any(String)]);
      expect(Number(holdersOfKey[1])).toBeGreaterThanOrEqual(beforeTakeAt + lockDurationInMilliseconds);
      expect(Number(holdersOfKey[1])).toBeLessThanOrEqual(afterReadAt + lockDurationInMilliseconds);
      expect(holdersOfTask).toHaveLength(1);
      expect(waitingJobIds).toEqual([deriveJobId("rebuild-index", waitingRun.runId)]);
      expect(await redis.keys(`${runQueueKey}:concurrency-*`)).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
      await redis.quit().catch(() => undefined);
    }
  },
);

integrationTest(
  "a waiting-list head whose run already failed is dropped, so it never blocks its key",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const gate = openGate();
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ outcome: z.enum(["fail", "wait for gate", "complete"]) }),
      retries: 0,
      concurrency: { limit: 1 },
      handler: async ({ payload }) => {
        if (payload.outcome === "fail") {
          throw new Error("index unavailable");
        }
        if (payload.outcome === "wait for gate") {
          await gate.opened;
        }
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [rebuildIndex],
    });
    const redis = new Redis(redisUrl, { lazyConnect: true });
    redis.on("error", () => undefined);

    try {
      await redis.connect();
      await nextasking.startWorker();
      const failedRun = await nextasking.dispatch("rebuild-index", { outcome: "fail" });
      await waitForLogMessage(lines, "run failed");
      await redis.rpush(
        `${keyPrefix}:runs:concurrency-waiting:rebuild-index`,
        deriveJobId("rebuild-index", failedRun.runId),
      );
      await nextasking.dispatch("rebuild-index", { outcome: "wait for gate" });
      await waitForLogMessage(lines, "attempt started");
      const waitingRun = await nextasking.dispatch("rebuild-index", { outcome: "complete" });
      await waitForLogMessage(lines, "run waiting for a concurrency slot");
      gate.open();

      await vi.waitFor(
        () => {
          expect(
            lines.filter((line) => line.message === "run completed" && line.fields.runId === waitingRun.runId),
          ).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
    } finally {
      await closeNextasking(nextasking);
      await redis.quit().catch(() => undefined);
    }
  },
);
