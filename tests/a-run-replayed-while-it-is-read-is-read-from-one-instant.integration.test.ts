import { expect } from "vitest";
import { z } from "zod";

import { createEngine } from "../src/engine/engine.js";
import type { Engine } from "../src/engine/engine.js";
import { deriveListenerRunId } from "../src/ids.js";
import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import type { RunId } from "../src/ids.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { endRuns } from "./ended-runs.js";
import { isRunErrorsRequestOf } from "./engine-requests.js";
import { integrationTest } from "./integration-test.js";
import { startTcpProxy } from "./tcp-proxy.js";
import { withRedis } from "./with-redis.js";

type ReplayedRun = { task: string; runId: RunId };

const CHARGE_CARD = "charge-card";

const EMAIL_RECEIPT = "email-receipt";

const invoicePaidEvent = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });

function createReader(url: string, keyPrefix: string) {
  return createNextasking({
    connection: { url },
    prefix: keyPrefix,
    events: [invoicePaidEvent],
    tasks: [
      defineTask(CHARGE_CARD, { schema: z.string(), handler: async () => undefined }),
      defineListener(EMAIL_RECEIPT, { on: invoicePaidEvent, handler: async () => undefined }),
    ],
  });
}

type Reader = ReturnType<typeof createReader>;

async function readAcrossAReplay<Read>(
  { redisUrl, keyPrefix }: { redisUrl: string; keyPrefix: string },
  replayedRun: ReplayedRun,
  read: (reader: Reader) => Promise<Read>,
): Promise<Read> {
  const proxy = await startTcpProxy(redisUrl);
  const reader = createReader(proxy.url, keyPrefix);
  try {
    return await withRedis(redisUrl, async (_redis, client) => {
      const engine: Engine = createEngine(client, keyPrefix);
      await endRuns(engine, replayedRun.task, [replayedRun.runId], "fail");
      const heldRead = proxy.holdRequest((request) => isRunErrorsRequestOf(request, keyPrefix, replayedRun));

      const reading = read(reader);
      const { release, waitForEarlierRequestsAnswered } = await heldRead;
      await waitForEarlierRequestsAnswered();
      expect(await engine.replay(replayedRun)).toEqual({ kind: "replayed", task: replayedRun.task });
      release();
      return await reading;
    });
  } finally {
    await closeNextaskingAndCheckItsDuration(reader);
    await proxy.stop();
  }
}

function replayedRunReadOf({ task, runId }: ReplayedRun) {
  return {
    task,
    runId,
    state: "pending",
    attempt: 1,
    dispatchedAt: expect.any(Date),
    payload: runId,
    errors: [],
    steps: [],
  };
}

integrationTest(
  "getRun of a failed run replayed while it is read returns the run before or after the replay, never a failed run without its error",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const replayedRun = { task: CHARGE_CARD, runId: "run-1" };

    const run = await readAcrossAReplay({ redisUrl, keyPrefix }, replayedRun, (reader) =>
      reader.getRun(CHARGE_CARD, "run-1"),
    );

    expect(run).toEqual(replayedRunReadOf(replayedRun));
  },
);

integrationTest(
  "getEvent of an event whose failed listener run is replayed while it is read returns that run before or after the replay, never failed without its error",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const replayedRun = { task: EMAIL_RECEIPT, runId: deriveListenerRunId("event-1", EMAIL_RECEIPT) };

    const event = await readAcrossAReplay({ redisUrl, keyPrefix }, replayedRun, (reader) =>
      reader.getEvent("invoice.paid", "event-1"),
    );

    expect(event.runsByListener[EMAIL_RECEIPT]).toEqual(replayedRunReadOf(replayedRun));
  },
);

integrationTest(
  "listRuns of failed runs, one replayed while its page is read, never lists a failed run without its last error",
  { timeout: 15_000 },
  async ({ redisUrl, keyPrefix }) => {
    const replayedRun = { task: CHARGE_CARD, runId: "run-1" };

    const page = await readAcrossAReplay({ redisUrl, keyPrefix }, replayedRun, (reader) =>
      reader.listRuns({ state: "failed", task: CHARGE_CARD }),
    );

    expect(page).toEqual({ runs: [], totalRunCount: 1, firstRunPosition: 1 });
  },
);
