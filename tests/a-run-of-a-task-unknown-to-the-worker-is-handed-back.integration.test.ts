import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";
import { backdateFirstFoundUnknownAt, engineTestContextOf } from "./engine-test.js";
import { startWorkerProcess } from "./worker-process.js";
import type { WorkerProcess } from "./worker-process.js";
import { workerProcessTasks } from "./worker-process-tasks.js";

const GIVE_UP_DELAY_IN_MILLISECONDS = 30 * 60 * 1000;

const FAST_HAND_BACK = {
  unknownNameHandBackDelayInMilliseconds: 300,
};

const EPOCH_MILLISECONDS = /^\d+$/;

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
    await closeNextaskingAndCheckItsDuration(nextasking);
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
  "a run of a task the worker doesn't define is handed back without using a retry or a stall, frees its concurrency slot at each hand back, and runs first of its key on a worker that defines it",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withNextReleaseAndOlderReleaseWorker(
      redisUrl,
      keyPrefix,
      async ({ nextasking, lines, olderReleaseWorker, handledAccountIds }) => {
        const { runId } = await nextasking.dispatch("archive-account", { accountId: "account-7" });
        const orderAtDispatch = (await storedRun(redisUrl, keyPrefix, "archive-account", runId))?.order;
        await vi.waitFor(
          () => {
            expect(
              olderReleaseWorker.logLines.filter((line) => line.message === "run handed back: unknown task"),
            ).toHaveLength(2);
          },
          { timeout: 10_000 },
        );
        await nextasking.dispatch("archive-account", { accountId: "account-8" });
        olderReleaseWorker.kill("SIGTERM");
        await olderReleaseWorker.exited;
        const runWhileHandedBack = await storedRun(redisUrl, keyPrefix, "archive-account", runId);
        const slotsHeldWhileHandedBack = await withRedis(redisUrl, (redis) =>
          redis.exists(engineKeysOf(keyPrefix).slotSet("archive-account", undefined)),
        );
        await nextasking.startWorker();
        await vi.waitFor(
          () => {
            expect(lines.filter((line) => line.message === "run completed")).toHaveLength(2);
          },
          { timeout: 10_000 },
        );

        expect(olderReleaseWorker.logLines.find((line) => line.message === "run handed back: unknown task")).toEqual({
          level: "debug",
          fields: { task: "archive-account", runId, attempt: 1 },
          message: "run handed back: unknown task",
        });
        expect(olderReleaseWorker.logLines.map((line) => line.message)).not.toContain("attempt stalled");
        expect(runWhileHandedBack).toMatchObject({
          state: "pending",
          attempt: "1",
          stalls: "0",
          firstFoundUnknownAt: expect.stringMatching(EPOCH_MILLISECONDS),
          order: orderAtDispatch,
          errors: [],
        });
        expect(Number(orderAtDispatch)).toBeGreaterThan(0);
        expect(slotsHeldWhileHandedBack).toBe(0);
        expect(lines.find((line) => line.message === "attempt started")?.fields).toEqual({
          task: "archive-account",
          runId,
          attempt: 1,
        });
        expect(handledAccountIds).toEqual(["account-7", "account-8"]);
      },
    );
  },
);

integrationTest(
  "a run of a task no worker defines fails as unknown task once 30 minutes have passed since a worker first found it unknown",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withNextReleaseAndOlderReleaseWorker(redisUrl, keyPrefix, async ({ nextasking, olderReleaseWorker }) => {
      const { runId } = await nextasking.dispatch("archive-account", { accountId: "account-7" });
      await waitForLogMessage(olderReleaseWorker.logLines, "run handed back: unknown task");
      await withRedis(redisUrl, (redis, client) =>
        backdateFirstFoundUnknownAt(
          engineTestContextOf(redis, client, keyPrefix),
          "archive-account",
          runId,
          GIVE_UP_DELAY_IN_MILLISECONDS,
        ),
      );
      await waitForLogMessage(olderReleaseWorker.logLines, "run failed", { timeout: 15_000 });
      const run = (await storedRun(redisUrl, keyPrefix, "archive-account", runId)) as unknown as {
        state: string;
        firstFoundUnknownAt: string;
        errors: { failedAt: string }[];
      };

      expect(run.state).toBe("failed");
      expect(olderReleaseWorker.logLines.find((line) => line.message === "run failed")).toEqual({
        level: "error",
        fields: {
          task: "archive-account",
          runId,
          attempt: 1,
          err: {
            name: "UnknownTaskRunError",
            message: `No worker defined task "archive-account" within 30m after a worker first found run "${runId}" unknown.`,
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
      expect(Date.parse(run.errors[0]?.failedAt ?? "") - Number(run.firstFoundUnknownAt)).toBeGreaterThanOrEqual(
        GIVE_UP_DELAY_IN_MILLISECONDS,
      );
    });
  },
);
