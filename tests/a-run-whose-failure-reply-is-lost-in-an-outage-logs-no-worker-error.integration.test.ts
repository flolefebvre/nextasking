import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineListener, defineTask, runFailed } from "../src/index.js";
import type { RunFailedPayload } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import type { RecordedLogLine } from "./recording-logger.js";
import { runJobKey } from "./run-job-key.js";
import { startTcpProxy } from "./tcp-proxy.js";

const MOVE_TO_FAILED_ARGUMENT = "failedReason";

type FailureReplyOutage = {
  nextasking: ReturnType<typeof createNextasking>;
  lines: RecordedLogLine[];
  runId: string;
  runFailedNotices: RunFailedPayload[];
};

async function withFailureReplyCut(
  { redisUrl, keyPrefix, retries }: { redisUrl: string; keyPrefix: string; retries: number },
  afterOutage: (outage: FailureReplyOutage) => Promise<void>,
): Promise<void> {
  const proxy = await startTcpProxy(redisUrl);
  const { logger, lines } = createRecordingLogger();
  const runFailedNotices: RunFailedPayload[] = [];
  let failureReplyCut: Promise<void> | undefined;
  const nextasking = createNextasking({
    connection: { url: proxy.url, giveUpAfter: "1s" },
    prefix: keyPrefix,
    logger,
    tasks: [
      defineTask("sync-account", {
        schema: z.object({ accountId: z.string() }),
        retries,
        backoff: "1s",
        handler: async ({ runId, attempt }) => {
          if (attempt > 1) {
            return;
          }
          const jobKey = runJobKey(keyPrefix, "sync-account", runId);
          failureReplyCut = proxy.cutBeforeReplyTo(
            (request) => request.includes(jobKey) && request.includes(MOVE_TO_FAILED_ARGUMENT),
          );
          throw new Error("upstream unavailable");
        },
      }),
      defineListener("alert-on-run-failed", {
        on: runFailed,
        handler: async ({ payload }) => {
          runFailedNotices.push(payload);
        },
      }),
    ],
  });
  closeWhenTestFinishes({ proxy, nextaskings: [nextasking], lines });

  await nextasking.startWorker();
  const { runId } = await nextasking.dispatch("sync-account", { accountId: "account-7" });
  await waitForLogMessage(lines, "connection error");
  await failureReplyCut;
  await proxy.restore();
  await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
  await afterOutage({ nextasking, lines, runId, runFailedNotices });

  expectOneConnectionErrorAndOneConnectionRestored(lines);
  expect(lines.filter((line) => line.message === "worker error" || line.message === "run failure unconfirmed")).toEqual(
    [],
  );
}

integrationTest(
  "a connection cut after a failed attempt is stored with a retry left, before its reply arrives, logs no worker error and the run retries after its backoff",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withFailureReplyCut({ redisUrl, keyPrefix, retries: 1 }, async ({ lines, runId, runFailedNotices }) => {
      await waitForLogMessage(lines, "run completed", { timeout: 15_000 });

      expect(lines.filter((line) => line.message === "attempt failed")).toEqual([
        {
          level: "warn",
          fields: {
            task: "sync-account",
            runId,
            attempt: 1,
            err: expect.objectContaining({ message: "upstream unavailable" }),
            retriesLeft: 1,
            backoffInMilliseconds: expect.any(Number),
          },
          message: "attempt failed",
        },
      ]);
      expect(lines.filter((line) => line.message === "run completed")).toEqual([
        expect.objectContaining({ fields: expect.objectContaining({ task: "sync-account", runId, attempt: 2 }) }),
      ]);
      expect(runFailedNotices).toEqual([]);
    });
  },
);

integrationTest(
  "a connection cut after a final failure is stored, before its reply arrives, logs no worker error, keeps the run failed and sends its failure notice without a restart",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    await withFailureReplyCut(
      { redisUrl, keyPrefix, retries: 0 },
      async ({ nextasking, lines, runId, runFailedNotices }) => {
        await vi.waitFor(
          () => {
            expect(runFailedNotices).toHaveLength(1);
          },
          { timeout: 15_000 },
        );

        expect(runFailedNotices).toEqual([expect.objectContaining({ task: "sync-account", runId })]);
        expect(lines.filter((line) => line.message === "run failed")).toEqual([
          expect.objectContaining({
            fields: expect.objectContaining({ runId, attempt: 1, reason: "retries exhausted" }),
          }),
        ]);
        expect(await nextasking.getRun("sync-account", runId)).toMatchObject({ runId, state: "failed" });
      },
    );
  },
);
