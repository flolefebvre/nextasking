import { setTimeout as sleep } from "node:timers/promises";

import { expect, vi } from "vitest";
import { z } from "zod";

import { parseDuration } from "../src/durations.js";
import { createNextasking, defineTask } from "../src/index.js";
import { isSettledWithin } from "../src/settled-within.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS } from "./invariant-violations.js";
import { measuredDurationOf } from "./measured-duration.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";

const SHUTDOWN_TIMEOUT = "2s";

const CLOSE_BOUND_IN_MILLISECONDS =
  parseDuration(SHUTDOWN_TIMEOUT) + CLOSE_BOUND_PAST_THE_SHUTDOWN_TIMEOUT_IN_MILLISECONDS;

const CLOSE_WAIT_IN_MILLISECONDS = CLOSE_BOUND_IN_MILLISECONDS + 1_000;

const NO_RECONNECT_WINDOW_IN_MILLISECONDS = 3_000;

const TEST_TIMEOUT_IN_MILLISECONDS =
  10_000 + 10_000 + CLOSE_WAIT_IN_MILLISECONDS + NO_RECONNECT_WINDOW_IN_MILLISECONDS + 5_000;

function createWorkerNextasking(proxy: TcpProxy, keyPrefix: string) {
  const { logger, lines } = createRecordingLogger();
  const nextasking = createNextasking({
    connection: { url: proxy.url, giveUpAfter: "1s" },
    prefix: keyPrefix,
    logger,
    tasks: [defineTask("send-receipt", { schema: z.object({}), handler: async () => undefined })],
  });
  return { nextasking, lines };
}

type WorkerNextasking = ReturnType<typeof createWorkerNextasking>;

async function closedWorkerBehind(proxy: TcpProxy, { nextasking }: WorkerNextasking) {
  const closing = closeNextaskingAndCheckItsDuration(nextasking, { shutdownTimeout: SHUTDOWN_TIMEOUT });
  const closeMeasure = await measuredDurationOf(() => isSettledWithin(closing, CLOSE_WAIT_IN_MILLISECONDS));
  const connectionCountsAtTheClose = [proxy.acceptedConnectionCount(), proxy.resetConnectionCount()];
  await sleep(NO_RECONNECT_WINDOW_IN_MILLISECONDS);
  const connectionCountsAfterTheWindow = [proxy.acceptedConnectionCount(), proxy.resetConnectionCount()];
  return { closeMeasure, connectionCountsAtTheClose, connectionCountsAfterTheWindow };
}

function expectClosedWithinTheBoundAndEnded(
  proxy: TcpProxy,
  {
    closeMeasure,
    connectionCountsAtTheClose,
    connectionCountsAfterTheWindow,
  }: Awaited<ReturnType<typeof closedWorkerBehind>>,
): void {
  expect(closeMeasure.result, `close() still pending after ${String(CLOSE_WAIT_IN_MILLISECONDS)} ms`).toBe(true);
  expect(closeMeasure.elapsedNetOfStallInMilliseconds).toBeLessThanOrEqual(CLOSE_BOUND_IN_MILLISECONDS);
  expect(connectionCountsAfterTheWindow, "connections accepted and reset by the proxy").toEqual(
    connectionCountsAtTheClose,
  );
  expect(proxy.liveConnectionCount()).toBe(0);
}

async function startedWorkerAfterAnOutageBegan(proxy: TcpProxy, keyPrefix: string): Promise<WorkerNextasking> {
  const workerNextasking = createWorkerNextasking(proxy, keyPrefix);
  await workerNextasking.nextasking.startWorker({ shutdownTimeout: SHUTDOWN_TIMEOUT });
  await proxy.cut();
  await waitForLogMessage(workerNextasking.lines, "connection error");
  return workerNextasking;
}

integrationTest(
  "a worker closed as soon as its connection is cut, its attempts drained, closes within the shutdown timeout plus 2 s and never connects again",
  { timeout: TEST_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    try {
      const workerNextasking = await startedWorkerAfterAnOutageBegan(proxy, keyPrefix);

      const outcome = await closedWorkerBehind(proxy, workerNextasking);

      expectClosedWithinTheBoundAndEnded(proxy, outcome);
    } finally {
      await proxy.stop();
    }
  },
);

integrationTest(
  "a worker closed right after Redis is back, while a reconnect's handshake is unanswered, closes within the shutdown timeout plus 2 s and never connects again",
  { timeout: TEST_TIMEOUT_IN_MILLISECONDS },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    try {
      const workerNextasking = await startedWorkerAfterAnOutageBegan(proxy, keyPrefix);
      const reconnectHandshake = proxy.holdRequest(() => true);
      await proxy.restore();
      const { release } = await vi.waitUntil(() => reconnectHandshake, { timeout: 10_000 });

      const outcome = await closedWorkerBehind(proxy, workerNextasking);
      release();

      expectClosedWithinTheBoundAndEnded(proxy, outcome);
    } finally {
      await proxy.stop();
    }
  },
);
