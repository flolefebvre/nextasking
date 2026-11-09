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
import { isSettleRequestOf } from "./engine-requests.js";
import { startTcpProxy } from "./tcp-proxy.js";

type FailureReplyOutage = {
  directNextasking: ReturnType<typeof createNextasking>;
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
  const tasks = [
    defineTask("sync-account", {
      schema: z.object({ accountId: z.string() }),
      retries,
      backoff: "1s",
      handler: async ({ runId, attempt }) => {
        if (attempt > 1) {
          return;
        }
        failureReplyCut = proxy.cutBeforeReplyTo((request) =>
          isSettleRequestOf(request, keyPrefix, {
            task: "sync-account",
            runId,
            settleKind: retries === 0 ? "fail" : "retry",
          }),
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
  ];
  const workerNextasking = createNextasking({
    connection: { url: proxy.url, giveUpAfter: "1s" },
    prefix: keyPrefix,
    logger,
    tasks,
  });
  const directNextasking = createNextasking({ connection: { url: redisUrl }, prefix: keyPrefix, tasks });
  closeWhenTestFinishes({ proxy, nextaskings: [workerNextasking, directNextasking], lines });

  await workerNextasking.startWorker();
  const { runId } = await directNextasking.dispatch("sync-account", { accountId: "account-7" });
  await waitForLogMessage(lines, "connection error");
  await failureReplyCut;
  await proxy.restore();
  await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
  await afterOutage({ directNextasking, lines, runId, runFailedNotices });

  expectOneConnectionErrorAndOneConnectionRestored(lines);
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
      async ({ directNextasking, lines, runId, runFailedNotices }) => {
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
        expect(await directNextasking.getRun("sync-account", runId)).toMatchObject({ runId, state: "failed" });
      },
    );
  },
);
