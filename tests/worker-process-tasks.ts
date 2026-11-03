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

export function workerProcessTasks(writeRecord: WriteRecord) {
  const blockEventLoopTask = defineTask("block-event-loop", {
    schema: z.object({ blockInMilliseconds: z.int() }),
    handler: async ({ payload, runId }) => {
      await writeRecord("block-event-loop", { runId, event: "started" });
      blockEventLoop(payload.blockInMilliseconds);
    },
  });

  const recordAbortAfterBlocking = defineTask("record-abort-after-blocking", {
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

  const waitThenRecord = defineTask("wait-then-record", {
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

  const blockEventLoopOnSchedule = defineScheduledTask("block-event-loop-on-schedule", {
    schedule: { cron: "0 0 1 1 *" },
    handler: async ({ runId, scheduledAt }) => {
      await writeRecord("block-event-loop-on-schedule", { runId, scheduledAt, event: "started" });
      blockEventLoop(SCHEDULED_BLOCK_IN_MILLISECONDS);
    },
  });

  return [
    blockEventLoopTask,
    recordAbortAfterBlocking,
    waitThenRecord,
    waitForAbortTask,
    blockEventLoopOnSchedule,
  ] as const;
}
