import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineListener, defineTask, runFailedEvent } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { isSettleRequestOf } from "./engine-requests.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { storedRun } from "./stored-run.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";

const LONGER_AGO_THAN_THE_GIVE_UP_DELAY_IN_MILLISECONDS = 60 * 60 * 1000;

integrationTest(
  "a connection cut after a given-up run of an unknown task is stored as failed, before its reply arrives, logs no worker error and sends the failure notice",
  { timeout: 50_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    const runFailedListenerTaskNames: string[] = [];
    const nextReleaseNextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [
        defineTask("archive-account", { schema: z.object({ accountId: z.string() }), handler: async () => undefined }),
      ],
    });
    const olderReleaseNextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineListener("alert-on-run-failed", {
          on: runFailedEvent,
          handler: async ({ payload }) => {
            runFailedListenerTaskNames.push(payload.task);
          },
        }),
      ],
    });
    closeWhenTestFinishes({ proxy, nextaskings: [olderReleaseNextasking, nextReleaseNextasking], lines });

    const { runId } = await nextReleaseNextasking.dispatch("archive-account", { accountId: "account-7" });
    await withRedis(redisUrl, (redis) =>
      redis.hSet(
        engineKeysOf(keyPrefix).runRecord("archive-account", runId),
        "firstFoundUnknownAt",
        Date.now() - LONGER_AGO_THAN_THE_GIVE_UP_DELAY_IN_MILLISECONDS,
      ),
    );
    const failureReplyCut = proxy.cutBeforeReplyTo((request) =>
      isSettleRequestOf(request, keyPrefix, { task: "archive-account", runId, settleKind: "handBack" }),
    );
    await olderReleaseNextasking.startWorker({ concurrency: 1 });
    await waitForLogMessage(lines, "connection error", { timeout: 20_000 });
    await failureReplyCut;
    await proxy.restore();
    await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
    await vi.waitFor(
      () => {
        expect(runFailedListenerTaskNames).toEqual(["archive-account"]);
      },
      { timeout: 10_000 },
    );

    expect(await storedRun(redisUrl, keyPrefix, "archive-account", runId)).toMatchObject({
      state: "failed",
      reason: "unknown task",
    });
    expect(lines.filter((line) => line.message === "run failed").map((line) => line.fields.reason)).toEqual([
      "unknown task",
    ]);
    expectOneConnectionErrorAndOneConnectionRestored(lines);
  },
);
