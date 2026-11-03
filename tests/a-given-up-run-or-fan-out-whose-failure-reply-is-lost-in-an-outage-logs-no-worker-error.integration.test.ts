import { expect, vi } from "vitest";
import { z } from "zod";

import { deriveJobId } from "../src/ids.js";
import { createNextasking, defineEvent, defineListener, defineTask, runFailed } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { runQueueKeyPrefix } from "./run-job-key.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRunQueue } from "./with-run-queue.js";

const MOVE_TO_FAILED_ARGUMENT = "failedReason";

const LONGER_AGO_THAN_THE_GIVE_UP_DELAY_IN_MILLISECONDS = 60 * 60 * 1000;

const accountClosed = defineEvent("account.closed", { schema: z.object({ accountId: z.string() }) });

const archiveAccount = defineTask("archive-account", {
  schema: z.object({ accountId: z.string() }),
  handler: async () => undefined,
});

const notifyAccountClosed = defineListener("notify-account-closed", {
  on: accountClosed,
  handler: async () => undefined,
});

async function storeFirstFoundUnknownLongerAgoThanTheGiveUpDelay(
  redisUrl: string,
  keyPrefix: string,
  jobId: string,
): Promise<void> {
  await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
    const job = await runQueue.getJob(jobId);
    if (job === undefined) {
      throw new Error(`No job ${jobId} to preset`);
    }
    await job.updateData({
      ...(job.data as object),
      firstFoundUnknownAt: new Date(Date.now() - LONGER_AGO_THAN_THE_GIVE_UP_DELAY_IN_MILLISECONDS).toISOString(),
    });
  });
}

type GivenUpFailureReplyOutage = {
  lines: RecordedLogLine[];
  runFailedListenerTaskNames: string[];
};

async function withGivenUpFailureReplyCut(
  { redisUrl, keyPrefix }: { redisUrl: string; keyPrefix: string },
  enqueueJobOfANameTheWorkerLacks: (
    nextReleaseNextasking: ReturnType<typeof createNextasking>,
  ) => Promise<{ jobId: string }>,
  afterOutage: (outage: GivenUpFailureReplyOutage) => Promise<void>,
): Promise<void> {
  const proxy = await startTcpProxy(redisUrl);
  const { logger, lines } = createRecordingLogger();
  const runFailedListenerTaskNames: string[] = [];
  const nextReleaseNextasking = createNextasking({
    connection: { url: redisUrl },
    prefix: keyPrefix,
    events: [accountClosed],
    tasks: [archiveAccount, notifyAccountClosed],
  });
  const olderReleaseNextasking = createNextasking({
    connection: { url: proxy.url, giveUpAfter: "1s" },
    prefix: keyPrefix,
    logger,
    tasks: [
      defineListener("alert-on-run-failed", {
        on: runFailed,
        handler: async ({ payload }) => {
          runFailedListenerTaskNames.push(payload.task);
        },
      }),
    ],
  });

  try {
    const { jobId } = await enqueueJobOfANameTheWorkerLacks(nextReleaseNextasking);
    await storeFirstFoundUnknownLongerAgoThanTheGiveUpDelay(redisUrl, keyPrefix, jobId);
    const jobKey = `${runQueueKeyPrefix(keyPrefix)}${jobId}`;
    const failureReplyCut = proxy.cutBeforeReplyTo(
      (request) => request.includes(jobKey) && request.includes(MOVE_TO_FAILED_ARGUMENT),
    );
    await olderReleaseNextasking.startWorker({ concurrency: 1 });
    await waitForLogMessage(lines, "connection error");
    await failureReplyCut;
    await proxy.restore();
    await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
    await afterOutage({ lines, runFailedListenerTaskNames });

    expectOneConnectionErrorAndOneConnectionRestored(lines);
    expect(
      lines.filter((line) =>
        ["worker error", "run failure unconfirmed", "fan-out failure unconfirmed"].includes(line.message),
      ),
    ).toEqual([]);
  } finally {
    await closeNextasking(olderReleaseNextasking);
    await closeNextasking(nextReleaseNextasking);
    await proxy.stop();
  }
}

integrationTest(
  "a connection cut after a given-up run of an unknown task is stored as failed, before its reply arrives, logs no worker error and sends the failure notice",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    let runId = "";
    await withGivenUpFailureReplyCut(
      { redisUrl, keyPrefix },
      async (nextReleaseNextasking) => {
        ({ runId } = await nextReleaseNextasking.dispatch("archive-account", { accountId: "account-7" }));
        return { jobId: deriveJobId("archive-account", runId) };
      },
      async ({ lines, runFailedListenerTaskNames }) => {
        await vi.waitFor(
          () => {
            expect(runFailedListenerTaskNames).toEqual(["archive-account"]);
          },
          { timeout: 10_000 },
        );
        const failedRun = await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
          const runJob = await runQueue.getJob(deriveJobId("archive-account", runId));
          return { state: await runJob?.getState(), reason: (runJob?.data as { reason?: string }).reason };
        });

        expect(failedRun).toEqual({ state: "failed", reason: "unknown task" });
        expect(lines.filter((line) => line.message === "run failed").map((line) => line.fields.reason)).toEqual([
          "unknown task",
        ]);
      },
    );
  },
);

integrationTest(
  "a connection cut after a given-up fan-out of an unknown event is stored as failed, before its reply arrives, logs no worker error and one fan-out failed",
  { timeout: 30_000 },
  async ({ redisUrl, keyPrefix }) => {
    let eventId = "";
    await withGivenUpFailureReplyCut(
      { redisUrl, keyPrefix },
      async (nextReleaseNextasking) => {
        ({ eventId } = await nextReleaseNextasking.emit("account.closed", { accountId: "account-7" }));
        return { jobId: deriveJobId("account.closed", eventId) };
      },
      async ({ lines }) => {
        await waitForLogMessage(lines, "fan-out failed");
        const failedFanOut = await withRunQueue(redisUrl, keyPrefix, async (runQueue) => {
          const fanOutJob = await runQueue.getJob(deriveJobId("account.closed", eventId));
          const { finalError, failureCount } = fanOutJob?.data as {
            finalError?: { name: string };
            failureCount?: number;
          };
          return { state: await fanOutJob?.getState(), finalErrorName: finalError?.name, failureCount };
        });

        expect(lines.filter((line) => line.message === "fan-out failed")).toEqual([
          {
            level: "error",
            fields: {
              eventName: "account.closed",
              eventId,
              err: expect.objectContaining({ name: "UnrecoverableError" }),
            },
            message: "fan-out failed",
          },
        ]);
        expect(failedFanOut).toEqual({ state: "failed", finalErrorName: "UnrecoverableError", failureCount: 1 });
      },
    );
  },
);
