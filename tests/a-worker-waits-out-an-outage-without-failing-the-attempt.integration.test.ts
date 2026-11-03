import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { startTcpProxy } from "./tcp-proxy.js";
import type { TcpProxy } from "./tcp-proxy.js";

const OUTAGE_LONGER_THAN_GIVE_UP_AFTER_IN_MILLISECONDS = 2_500;

function createHeldHandler() {
  let isStarted = false;
  let release = () => undefined as void;
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  return {
    isStarted: () => isStarted,
    release: () => {
      release();
    },
    waitForRelease: async () => {
      isStarted = true;
      await released;
    },
  };
}

type HeldHandler = ReturnType<typeof createHeldHandler>;

async function resumeTheHandlerDuringAnOutage(proxy: TcpProxy, heldHandler: HeldHandler): Promise<void> {
  await vi.waitFor(() => expect(heldHandler.isStarted()).toBe(true), { timeout: 10_000 });
  const outage = proxy.cutFor(OUTAGE_LONGER_THAN_GIVE_UP_AFTER_IN_MILLISECONDS);
  heldHandler.release();
  await outage;
}

integrationTest(
  "a handler's dispatch made during an outage longer than giveUpAfter waits for Redis, and the run completes in its first attempt",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const heldHandler = createHeldHandler();
    const nextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      tasks: [
        defineTask("create-workspace", {
          schema: z.object({ userId: z.string() }),
          retries: 0,
          handler: async ({ payload, dispatch }) => {
            await heldHandler.waitForRelease();
            await dispatch("send-welcome-email", { email: `${payload.userId}@example.com` });
          },
        }),
        defineTask("send-welcome-email", { schema: z.object({ email: z.string() }), handler: async () => undefined }),
      ],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("create-workspace", { userId: "ada" });

      await resumeTheHandlerDuringAnOutage(proxy, heldHandler);

      await vi.waitFor(
        async () => {
          expect(await nextasking.getRun("create-workspace", runId)).toMatchObject({
            state: "completed",
            attempt: 1,
          });
        },
        { timeout: 15_000 },
      );
      await vi.waitFor(
        async () => {
          const runs = await nextasking.listRuns({ state: "completed", task: "send-welcome-email" });
          expect(runs.runs).toHaveLength(1);
        },
        { timeout: 15_000 },
      );
    } finally {
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
  30_000,
);

integrationTest(
  "a handler that throws during an outage records its own error once Redis is back, not the connection's",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const heldHandler = createHeldHandler();
    const { logger, lines } = createRecordingLogger();
    const nextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("charge-card", {
          schema: z.object({ cardId: z.string() }),
          retries: 0,
          handler: async () => {
            await heldHandler.waitForRelease();
            throw new Error("card declined");
          },
        }),
      ],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("charge-card", { cardId: "card-7" });

      await resumeTheHandlerDuringAnOutage(proxy, heldHandler);

      await vi.waitFor(
        async () => {
          expect(await nextasking.getRun("charge-card", runId)).toMatchObject({
            state: "failed",
            reason: "retries exhausted",
            errors: [{ attempt: 1, message: "card declined" }],
          });
        },
        { timeout: 15_000 },
      );
      expect(lines.filter((line) => line.message === "run failed")).toEqual([
        expect.objectContaining({
          fields: expect.objectContaining({ err: expect.objectContaining({ message: "card declined" }) }),
        }),
      ]);
    } finally {
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
  30_000,
);
