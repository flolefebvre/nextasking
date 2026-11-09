import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";

const WAITING_RUN_COUNT = 4;

function messagesOfRun(lines: readonly RecordedLogLine[], runId: string): string[] {
  return lines.filter((line) => line.fields.runId === runId).map((line) => line.message);
}

integrationTest(
  "runs beyond a key's limit wait without using retries, start in dispatch order, and leave other keys free",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const startedRuns: { order: number; attempt: number }[] = [];
    let releaseHoldingRun = () => undefined as void;
    const holdingRunReleased = new Promise<void>((resolve) => {
      releaseHoldingRun = resolve;
    });
    const rebuildIndex = defineTask("rebuild-index", {
      schema: z.object({ accountId: z.string(), order: z.int() }),
      retries: 0,
      concurrency: { key: (payload) => payload.accountId, limit: 1 },
      handler: async ({ payload, attempt }) => {
        startedRuns.push({ order: payload.order, attempt });
        if (payload.order === 0) {
          await holdingRunReleased;
        }
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [rebuildIndex],
    });

    try {
      await nextasking.startWorker();
      const holdingRun = await nextasking.dispatch("rebuild-index", { accountId: "account-7", order: 0 });
      await waitForLogMessage(lines, "attempt started");
      const waitingRunIds: string[] = [];
      for (let order = 1; order <= WAITING_RUN_COUNT; order += 1) {
        const { runId } = await nextasking.dispatch("rebuild-index", { accountId: "account-7", order });
        waitingRunIds.push(runId);
        await vi.waitFor(
          () => {
            expect(messagesOfRun(lines, runId)).toContain("run waiting for a concurrency slot");
          },
          { timeout: 10_000 },
        );
      }
      const otherKeyRun = await nextasking.dispatch("rebuild-index", { accountId: "account-9", order: 99 });
      await vi.waitFor(
        () => {
          expect(messagesOfRun(lines, otherKeyRun.runId)).toContain("run completed");
        },
        { timeout: 10_000 },
      );
      releaseHoldingRun();
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(WAITING_RUN_COUNT + 2);
        },
        { timeout: 10_000 },
      );

      expect(startedRuns).toEqual([
        { order: 0, attempt: 1 },
        { order: 99, attempt: 1 },
        { order: 1, attempt: 1 },
        { order: 2, attempt: 1 },
        { order: 3, attempt: 1 },
        { order: 4, attempt: 1 },
      ]);
      expect(messagesOfRun(lines, holdingRun.runId)).toEqual(["run dispatched", "attempt started", "run completed"]);
      expect(messagesOfRun(lines, otherKeyRun.runId)).toEqual(["run dispatched", "attempt started", "run completed"]);
      for (const runId of waitingRunIds) {
        expect(messagesOfRun(lines, runId)).toEqual([
          "run dispatched",
          "run waiting for a concurrency slot",
          "attempt started",
          "run completed",
        ]);
      }
      expect(lines.find((line) => line.message === "run waiting for a concurrency slot")).toEqual({
        level: "debug",
        fields: { task: "rebuild-index", runId: waitingRunIds[0], attempt: 1 },
        message: "run waiting for a concurrency slot",
      });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
