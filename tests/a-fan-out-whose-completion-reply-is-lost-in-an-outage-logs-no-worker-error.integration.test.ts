import { expect, vi } from "vitest";
import { z } from "zod";

import { deriveJobId } from "../src/ids.js";
import { createNextasking, defineEvent, defineListener } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { runQueueKeyPrefix } from "./run-job-key.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRunQueue } from "./with-run-queue.js";

const MOVE_TO_COMPLETED_ARGUMENT = "returnvalue";

const LISTENER_NAMES = ["record-payment", "send-receipt"];

const WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS = 10_000;

integrationTest(
  "a connection cut after the completion of a fan-out is stored, before its reply arrives, logs no worker error and hands back the listener run that reply carried to a worker of concurrency 1, so each listener runs once, at once after the reconnect, with no stall",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    const listenerRunIds: string[] = [];
    const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
    const listeners = LISTENER_NAMES.map((listenerName) =>
      defineListener(listenerName, {
        on: invoicePaid,
        handler: async ({ runId }) => {
          listenerRunIds.push(runId);
        },
      }),
    );
    const workerNextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      events: [invoicePaid],
      tasks: listeners,
    });
    const directNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      events: [invoicePaid],
      tasks: listeners,
    });
    const eventId = "stripe-evt-7";
    const fanOutJobKey = `${runQueueKeyPrefix(keyPrefix)}${deriveJobId("invoice.paid", eventId)}`;
    closeWhenTestFinishes({ proxy, nextaskings: [workerNextasking, directNextasking], lines });

    await workerNextasking.startWorker({ concurrency: 1 });
    const completionReplyCut = proxy.cutBeforeReplyTo(
      (request) => request.includes(fanOutJobKey) && request.includes(MOVE_TO_COMPLETED_ARGUMENT),
    );
    await directNextasking.emit("invoice.paid", { invoiceId: "invoice-7" }, { eventId });
    await waitForLogMessage(lines, "connection error");
    await completionReplyCut;
    await proxy.restore();
    await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
    await vi.waitFor(
      () => {
        expect(
          lines
            .filter((line) => line.message === "run completed")
            .map((line) => line.fields.task)
            .sort(),
        ).toEqual(LISTENER_NAMES);
      },
      { timeout: WAIT_WELL_UNDER_THE_LOCK_DURATION_IN_MILLISECONDS },
    );
    const stalledCountersOfListenerRuns = await withRunQueue(redisUrl, keyPrefix, async (runQueue) =>
      (await runQueue.getCompleted(0, -1))
        .filter((completedJob) => LISTENER_NAMES.includes(completedJob.name))
        .map((completedJob) => completedJob.stalledCounter),
    );

    expect(listenerRunIds).toHaveLength(LISTENER_NAMES.length);
    expect(stalledCountersOfListenerRuns).toEqual([0, 0]);
    expectOneConnectionErrorAndOneConnectionRestored(lines);
    expect(
      lines.filter((line) =>
        ["worker error", "fan-out completion unconfirmed", "attempt stalled", "next job hand-back failed"].includes(
          line.message,
        ),
      ),
    ).toEqual([]);
  },
);
