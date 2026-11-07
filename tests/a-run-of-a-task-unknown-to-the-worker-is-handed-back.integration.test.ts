import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { deriveJobId } from "../src/ids.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";
import { withRunQueue } from "./with-run-queue.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { workerProcessTasks } from "./worker-process-tasks.js";

const FAST_HAND_BACK = {
  unknownNameHandBackDelayInMilliseconds: 300,
  unknownNameGiveUpDelayInMilliseconds: 3000,
};

const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const accountClosed = defineEvent("account.closed", { schema: z.object({ accountId: z.string() }) });

type NextReleaseAndOlderReleaseWorker = {
  nextasking: ReturnType<typeof createNextReleaseNextasking>["nextasking"];
  lines: RecordedLogLine[];
  olderReleaseWorker: WorkerProcess;
  handledAccountIds: string[];
};

async function withNextReleaseAndOlderReleaseWorker(
  redisUrl: string,
  keyPrefix: string,
  use: (releases: NextReleaseAndOlderReleaseWorker) => Promise<void>,
): Promise<void> {
  const handledAccountIds: string[] = [];
  const { nextasking, lines } = createNextReleaseNextasking(redisUrl, keyPrefix, handledAccountIds);
  const olderReleaseWorker = await startWorkerProcess({ redisUrl, keyPrefix, timing: FAST_HAND_BACK });
  try {
    await use({ nextasking, lines, olderReleaseWorker, handledAccountIds });
  } finally {
    await olderReleaseWorker.stop();
    await closeNextasking(nextasking);
  }
}

function createNextReleaseNextasking(redisUrl: string, keyPrefix: string, handledAccountIds: string[]) {
  const { logger, lines } = createRecordingLogger();
  const archiveAccount = defineTask("archive-account", {
    schema: z.object({ accountId: z.string() }),
    concurrency: { limit: 1 },
    handler: async ({ payload }) => {
      handledAccountIds.push(payload.accountId);
    },
  });
  const notifyAccountClosed = defineListener("notify-account-closed", {
    on: accountClosed,
    handler: async ({ payload }) => {
      handledAccountIds.push(payload.accountId);
    },
  });
  const nextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    logger,
    events: [accountClosed],
    tasks: [
      ...workerProcessTasks(async () => {
        throw new Error("The test process runs only the next release's tasks");
      }),
      archiveAccount,
      notifyAccountClosed,
    ],
  });
  return { nextasking, lines };
}

integrationTest(
  "a run of a task the worker doesn't define is handed back without using a retry, a stall or a slot, until a worker that defines it runs it",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withNextReleaseAndOlderReleaseWorker(
      redisUrl,
      keyPrefix,
      async ({ nextasking, lines, olderReleaseWorker, handledAccountIds }) => {
        const { runId } = await nextasking.dispatch("archive-account", { accountId: "account-7" });
        await vi.waitFor(
          () => {
            expect(
              olderReleaseWorker.logLines.filter((line) => line.message === "run handed back: unknown task"),
            ).toHaveLength(2);
          },
          { timeout: 10_000 },
        );
        const runWhileHandedBack = await storedRun(redisUrl, keyPrefix, "archive-account", runId);
        const concurrencyKeysWhileHandedBack = await withRedis(redisUrl, (redis) =>
          redis.keys(`${keyPrefix}:runs:concurrency-*`),
        );
        await olderReleaseWorker.stop();
        await nextasking.startWorker();
        await waitForLogMessage(lines, "run completed");

        expect(olderReleaseWorker.logLines.find((line) => line.message === "run handed back: unknown task")).toEqual({
          level: "debug",
          fields: { task: "archive-account", runId, attempt: 1 },
          message: "run handed back: unknown task",
        });
        expect(olderReleaseWorker.logLines.map((line) => line.message)).not.toContain("attempt stalled");
        expect(runWhileHandedBack).toMatchObject({
          firstFoundUnknownAt: expect.stringMatching(ISO_TIMESTAMP),
          errors: [],
        });
        expect(concurrencyKeysWhileHandedBack).toEqual([]);
        expect(lines.find((line) => line.message === "attempt started")?.fields).toEqual({
          task: "archive-account",
          runId,
          attempt: 1,
        });
        expect(handledAccountIds).toEqual(["account-7"]);
      },
    );
  },
);

integrationTest(
  "a run of a task no worker defines fails as unknown task once the give-up delay has passed since a worker first found it unknown",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withNextReleaseAndOlderReleaseWorker(redisUrl, keyPrefix, async ({ nextasking, olderReleaseWorker }) => {
      const { runId } = await nextasking.dispatch("archive-account", { accountId: "account-7" });
      await waitForLogMessage(olderReleaseWorker.logLines, "run failed", { timeout: 15_000 });
      const run = (await storedRun(redisUrl, keyPrefix, "archive-account", runId)) as {
        firstFoundUnknownAt: string;
        errors: { failedAt: string }[];
      };
      const runState = await vi.waitFor(
        () =>
          withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
            const state = await (await runQueue.getJob(deriveJobId("archive-account", runId)))?.getState();
            expect(state).toBe("failed");
            return state;
          }),
        { timeout: 5000 },
      );

      expect(runState).toBe("failed");
      expect(olderReleaseWorker.logLines.find((line) => line.message === "run failed")).toEqual({
        level: "error",
        fields: {
          task: "archive-account",
          runId,
          attempt: 1,
          err: {
            name: "UnknownTaskRunError",
            message: `No worker defined task "archive-account" within 3s after a worker first found run "${runId}" unknown.`,
          },
          reason: "unknown task",
          runDurationInMilliseconds: expect.any(Number),
          executionDurationInMilliseconds: expect.any(Number),
        },
        message: "run failed",
      });
      expect(olderReleaseWorker.logLines.map((line) => line.message)).not.toContain("attempt failed");
      expect(run).toMatchObject({
        reason: "unknown task",
        errors: [{ attempt: 1, name: "UnknownTaskRunError" }],
      });
      expect(Date.parse(run.errors[0]?.failedAt ?? "") - Date.parse(run.firstFoundUnknownAt)).toBeGreaterThanOrEqual(
        3000,
      );
    });
  },
);

integrationTest(
  "a fan-out of an event the worker doesn't define is handed back, then fails once the give-up delay has passed",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withNextReleaseAndOlderReleaseWorker(redisUrl, keyPrefix, async ({ nextasking, olderReleaseWorker }) => {
      const { eventId } = await nextasking.emit("account.closed", { accountId: "account-7" });
      const fanOutFailure = await vi.waitFor(
        () =>
          withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
            const fanOutJobId = deriveJobId("account.closed", eventId);
            expect(await runQueue.getJobState(fanOutJobId)).toBe("failed");
            const fanOutJob = await runQueue.getJob(fanOutJobId);
            return { failedReason: fanOutJob?.failedReason, data: fanOutJob?.data as Record<string, unknown> };
          }),
        { timeout: 15_000, interval: 200 },
      );

      expect(olderReleaseWorker.logLines.find((line) => line.message === "fan-out handed back: unknown event")).toEqual(
        {
          level: "debug",
          fields: { eventName: "account.closed", eventId },
          message: "fan-out handed back: unknown event",
        },
      );
      expect(fanOutFailure).toEqual({
        failedReason: `No worker defined event "account.closed" within 3s after a worker first found event "${eventId}" unknown.`,
        data: expect.objectContaining({ firstFoundUnknownAt: expect.stringMatching(ISO_TIMESTAMP) }),
      });
    });
  },
);
