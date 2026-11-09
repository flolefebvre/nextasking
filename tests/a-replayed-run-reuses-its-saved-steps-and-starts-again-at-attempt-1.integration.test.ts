import { expect, vi } from "vitest";
import { z } from "zod";

import { RunNotReplayableError, UnknownTaskError, createNextasking, defineTask } from "../src/index.js";
import type { Logger } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { engineKeysOf } from "../src/engine/engine-keys.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

type ReceiptService = { isFixed: boolean; chargesCreated: number; attempts: number[] };

function createChargeInvoiceNextasking(
  redisUrl: string,
  keyPrefix: string,
  receiptService: ReceiptService,
  logger?: Logger,
) {
  const chargeInvoice = defineTask("charge-invoice", {
    schema: z.object({ invoiceId: z.string() }),
    retries: 1,
    backoff: "0s",
    handler: async ({ step, attempt }) => {
      receiptService.attempts.push(attempt);
      await step.run("create-charge", () => {
        receiptService.chargesCreated += 1;
        return { chargeId: "ch-7" };
      });
      if (!receiptService.isFixed) {
        throw new Error("receipt service unavailable");
      }
    },
  });
  return createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    ...(logger === undefined ? {} : { logger }),
    tasks: [chargeInvoice],
  });
}

function brokenReceiptService(): ReceiptService {
  return { isFixed: false, chargesCreated: 0, attempts: [] };
}

integrationTest(
  "a replayed run keeps its run id, reuses its saved steps, starts again at attempt 1 and completes",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const receiptService = brokenReceiptService();
    const nextasking = createChargeInvoiceNextasking(redisUrl, keyPrefix, receiptService, logger);

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("charge-invoice", { invoiceId: "inv-7" });
      await waitForLogMessage(lines, "run failed");
      receiptService.isFixed = true;

      const replayedRun = await nextasking.replay("charge-invoice", runId);
      await waitForLogMessage(lines, "run completed");

      expect(replayedRun).toEqual({ runId });
      expect(lines.filter((line) => line.message === "run replayed")).toEqual([
        { level: "info", fields: { task: "charge-invoice", runId }, message: "run replayed" },
      ]);
      expect(receiptService.attempts).toEqual([1, 2, 1]);
      expect(receiptService.chargesCreated).toBe(1);
      expect(await nextasking.getRun("charge-invoice", runId)).toMatchObject({
        state: "completed",
        attempt: 1,
        errors: [],
      });
      expect(await storedRun(redisUrl, keyPrefix, "charge-invoice", runId)).toMatchObject({
        state: "completed",
        replays: "1",
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);

integrationTest(
  "replay resets the failed run's errors, reason, stall count and unknown-task marker before its first new attempt, and counts the replay",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const worker = createChargeInvoiceNextasking(redisUrl, keyPrefix, brokenReceiptService(), logger);
    const operator = createChargeInvoiceNextasking(redisUrl, keyPrefix, brokenReceiptService());

    try {
      await worker.startWorker();
      const { runId } = await worker.dispatch("charge-invoice", { invoiceId: "inv-7" });
      await waitForLogMessage(lines, "run failed");
      await closeNextasking(worker);
      await withRedis(redisUrl, (redis) =>
        redis.hset(
          engineKeysOf(keyPrefix).runRecord("charge-invoice", runId),
          "stalls",
          2,
          "firstFoundUnknownAt",
          Date.now(),
        ),
      );

      await operator.replay("charge-invoice", runId);
      const replayedRun = await storedRun(redisUrl, keyPrefix, "charge-invoice", runId);

      expect(replayedRun).toMatchObject({
        payload: { invoiceId: "inv-7" },
        errors: [],
        stalls: "0",
        attempt: "1",
        replays: "1",
      });
      expect(replayedRun).not.toHaveProperty("reason");
      expect(replayedRun).not.toHaveProperty("firstFoundUnknownAt");
      expect(await operator.getRun("charge-invoice", runId)).toMatchObject({
        state: "pending",
        attempt: 1,
        steps: [{ id: "create-charge", result: { chargeId: "ch-7" } }],
      });
    } finally {
      await closeNextasking(worker);
      await closeNextasking(operator);
    }
  },
);

integrationTest("a replayed run that fails again keeps only its new life's errors", async ({ redisUrl, keyPrefix }) => {
  const { logger, lines } = createRecordingLogger();
  const nextasking = createChargeInvoiceNextasking(redisUrl, keyPrefix, brokenReceiptService(), logger);

  try {
    await nextasking.startWorker();
    const { runId } = await nextasking.dispatch("charge-invoice", { invoiceId: "inv-7" });
    await waitForLogMessage(lines, "run failed");

    await nextasking.replay("charge-invoice", runId);
    await vi.waitFor(
      () => {
        expect(lines.filter((line) => line.message === "run failed")).toHaveLength(2);
      },
      { timeout: 10_000 },
    );

    expect(await storedRun(redisUrl, keyPrefix, "charge-invoice", runId)).toMatchObject({
      reason: "retries exhausted",
      errors: [{ attempt: 1 }, { attempt: 2 }],
      replays: "1",
    });
  } finally {
    await closeNextasking(nextasking);
  }
});

integrationTest(
  "replay throws for a run that is unknown or not failed, and for a task the instance doesn't list",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const nextasking = createChargeInvoiceNextasking(
      redisUrl,
      keyPrefix,
      { isFixed: true, chargesCreated: 0, attempts: [] },
      logger,
    );

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("charge-invoice", { invoiceId: "inv-7" });
      await waitForLogMessage(lines, "run completed");
      const replayByName = nextasking.replay as (taskName: string, runId: string) => Promise<unknown>;

      const completedRunReplayError: unknown = await replayByName("charge-invoice", runId).catch((error) => error);
      const unknownRunReplayError: unknown = await replayByName("charge-invoice", "run-that-never-was").catch(
        (error) => error,
      );
      const unknownTaskReplayError: unknown = await replayByName("charge-card", runId).catch((error) => error);

      expect(completedRunReplayError).toBeInstanceOf(RunNotReplayableError);
      expect(completedRunReplayError).toHaveProperty(
        "message",
        `Run "${runId}" of task "charge-invoice" can't be replayed: it is not failed.`,
      );
      expect(unknownRunReplayError).toBeInstanceOf(RunNotReplayableError);
      expect(unknownRunReplayError).toHaveProperty(
        "message",
        `Run "run-that-never-was" of task "charge-invoice" can't be replayed: no such run is kept, its id is unknown or the run was removed.`,
      );
      expect(unknownTaskReplayError).toBeInstanceOf(UnknownTaskError);
      expect(lines.map((line) => line.message)).not.toContain("run replayed");
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
