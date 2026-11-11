import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { z } from "zod";

import { defineScheduledTask, defineTask } from "../src/index.js";

const SCHEDULED_BLOCK_IN_MILLISECONDS = 4000;

export type WriteRecord = (taskName: string, record: unknown) => Promise<void>;

const ABORT_WAIT_LIMIT_IN_MILLISECONDS = 30_000;

function blockEventLoop(blockInMilliseconds: number): void {
  const blockEndsAt = Date.now() + blockInMilliseconds;
  const neverNotified = new Int32Array(new SharedArrayBuffer(4));
  while (Date.now() < blockEndsAt) {
    Atomics.wait(neverNotified, 0, 0, blockEndsAt - Date.now());
  }
}

async function waitForAbort(abortSignal: AbortSignal): Promise<void> {
  if (abortSignal.aborted) {
    return;
  }
  await new Promise<void>((resolve) => {
    const giveUp = setTimeout(resolve, ABORT_WAIT_LIMIT_IN_MILLISECONDS);
    abortSignal.addEventListener(
      "abort",
      () => {
        clearTimeout(giveUp);
        resolve();
      },
      { once: true },
    );
  });
}

function messageOf(value: unknown): string {
  return value instanceof Error ? value.message : String(value);
}

export type LimitedExecutionRecord = {
  runId: string;
  executionId: string;
  pid: number;
  attempt: number;
  concurrencyKey: string;
  event: "started" | "ended";
  at: number;
};

const limitedExecutionSchema = z.object({ concurrencyKey: z.string(), workInMilliseconds: z.int() });

function limitedExecutionTask<const TaskName extends string>(
  writeRecord: WriteRecord,
  taskName: TaskName,
  concurrency: { limit: number; isKeyed: boolean },
) {
  return defineTask(taskName, {
    schema: limitedExecutionSchema,
    concurrency: {
      limit: concurrency.limit,
      ...(concurrency.isKeyed ? { key: (payload: { concurrencyKey: string }) => payload.concurrencyKey } : {}),
    },
    handler: async ({ payload, runId, attempt }) => {
      const executionRecord = {
        runId,
        executionId: randomUUID(),
        pid: process.pid,
        attempt,
        concurrencyKey: concurrency.isKeyed ? payload.concurrencyKey : "",
      };
      await writeRecord(taskName, { ...executionRecord, event: "started", at: Date.now() });
      await sleep(payload.workInMilliseconds);
      await writeRecord(taskName, { ...executionRecord, event: "ended", at: Date.now() });
    },
  });
}

export function workerProcessTasks(writeRecord: WriteRecord) {
  const blockEventLoopTask = defineTask("block-event-loop", {
    schema: z.object({ blockInMilliseconds: z.int() }),
    handler: async ({ payload, runId }) => {
      await writeRecord("block-event-loop", { runId, event: "started" });
      blockEventLoop(payload.blockInMilliseconds);
    },
  });

  const recordAbortAfterBlockingTask = defineTask("record-abort-after-blocking", {
    schema: z.object({ blockInMilliseconds: z.int() }),
    handler: async ({ payload, step, abortSignal }) => {
      blockEventLoop(payload.blockInMilliseconds);
      await waitForAbort(abortSignal);
      const stepError = await step
        .run("after-abort", async () => {
          await writeRecord("record-abort-after-blocking", { event: "after-abort step ran" });
        })
        .catch((error: unknown) => error);
      await writeRecord("record-abort-after-blocking", {
        abortReason: messageOf(abortSignal.reason),
        stepError: messageOf(stepError),
      });
    },
  });

  const waitThenRecordTask = defineTask("wait-then-record", {
    schema: z.object({ waitInMilliseconds: z.int() }),
    handler: async ({ payload, step, attempt }) => {
      await step.run("wait", async () => {
        await writeRecord("wait-then-record", { step: "wait", attempt });
        await sleep(payload.waitInMilliseconds);
        return "waited";
      });
      await step.run("record", async () => {
        await writeRecord("wait-then-record", { step: "record", attempt });
      });
    },
  });

  const waitForAbortTask = defineTask("wait-for-abort", {
    schema: z.object({}),
    handler: async ({ step, abortSignal }) => {
      await writeRecord("wait-for-abort", { event: "started" });
      await waitForAbort(abortSignal);
      await writeRecord("wait-for-abort", { event: "aborted", abortReason: messageOf(abortSignal.reason) });
      await step.run("after-abort", async () => {
        await writeRecord("wait-for-abort", { event: "after-abort step ran" });
      });
    },
  });

  const completeAtOnceTask = defineTask("complete-at-once", {
    schema: z.object({}),
    handler: async () => undefined,
  });

  const runIdsAbortedOnce = new Set<string>();
  const waitForAbortOnceTask = defineTask("wait-for-abort-once", {
    schema: z.object({}),
    handler: async ({ runId, abortSignal }) => {
      await writeRecord("wait-for-abort-once", { event: "started", at: Date.now() });
      if (!runIdsAbortedOnce.has(runId)) {
        await waitForAbort(abortSignal);
        runIdsAbortedOnce.add(runId);
        await writeRecord("wait-for-abort-once", {
          event: "aborted",
          abortReason: messageOf(abortSignal.reason),
          at: Date.now(),
        });
      }
    },
  });

  const blockEventLoopOnScheduleTask = defineScheduledTask("block-event-loop-on-schedule", {
    schedule: { cron: "0 0 1 1 *" },
    handler: async ({ runId, scheduledAt }) => {
      await writeRecord("block-event-loop-on-schedule", { runId, scheduledAt, event: "started" });
      blockEventLoop(SCHEDULED_BLOCK_IN_MILLISECONDS);
    },
  });

  const holdConcurrencySlotTask = defineTask("hold-concurrency-slot", {
    schema: z.object({}),
    concurrency: { limit: 1 },
    handler: async ({ abortSignal }) => {
      await writeRecord("hold-concurrency-slot", { event: "started" });
      await waitForAbort(abortSignal);
      await writeRecord("hold-concurrency-slot", { event: "aborted", abortReason: messageOf(abortSignal.reason) });
    },
  });

  const emitProcessWarningTask = defineTask("emit-process-warning", {
    schema: z.object({ warning: z.string() }),
    handler: async ({ payload }) => {
      process.emitWarning(payload.warning);
    },
  });

  const rejectWithNoHandlerTask = defineTask("reject-with-no-handler", {
    schema: z.object({ reason: z.string() }),
    handler: async ({ payload }) => {
      void Promise.reject(new Error(payload.reason));
    },
  });

  return [
    limitedExecutionTask(writeRecord, "one-per-key", { limit: 1, isKeyed: true }),
    emitProcessWarningTask,
    rejectWithNoHandlerTask,
    limitedExecutionTask(writeRecord, "two-per-key", { limit: 2, isKeyed: true }),
    limitedExecutionTask(writeRecord, "three-in-all", { limit: 3, isKeyed: false }),
    holdConcurrencySlotTask,
    blockEventLoopTask,
    recordAbortAfterBlockingTask,
    waitThenRecordTask,
    waitForAbortTask,
    waitForAbortOnceTask,
    completeAtOnceTask,
    blockEventLoopOnScheduleTask,
  ] as const;
}
