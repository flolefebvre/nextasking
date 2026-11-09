import { expect, vi } from "vitest";
import { z } from "zod";

import { RedisUnavailableError, createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { closeWhenTestFinishes } from "./close-when-test-finishes.js";
import { integrationTest } from "./integration-test.js";
import { measuredRejectionOf } from "./measured-duration.js";
import { startTcpProxy } from "./tcp-proxy.js";

const GIVE_UP_AFTER_IN_MILLISECONDS = 1_000;

const SLOWEST_GIVE_UP_IN_MILLISECONDS = 4_000;

function createInvoicingNextasking(url: string, keyPrefix: string) {
  const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
  return createNextasking({
    connection: { url, giveUpAfter: "1s" },
    prefix: keyPrefix,
    events: [invoicePaid],
    tasks: [
      defineTask("send-welcome-email", { schema: z.object({ email: z.string() }), handler: async () => undefined }),
      defineListener("send-receipt", { on: invoicePaid, handler: async () => undefined }),
    ],
  });
}

async function rejectionOf(call: Promise<unknown>): Promise<unknown> {
  const {
    result: rejection,
    elapsedInMilliseconds,
    elapsedNetOfStallInMilliseconds,
  } = await measuredRejectionOf(() => call);
  expect(elapsedInMilliseconds).toBeGreaterThanOrEqual(GIVE_UP_AFTER_IN_MILLISECONDS - 50);
  expect(elapsedNetOfStallInMilliseconds).toBeLessThan(SLOWEST_GIVE_UP_IN_MILLISECONDS);
  return rejection;
}

integrationTest(
  "with Redis unreachable from the start, each call throws RedisUnavailableError after giveUpAfter, then works once Redis is back",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    await proxy.cut();
    const nextasking = createInvoicingNextasking(proxy.url, keyPrefix);

    closeWhenTestFinishes({ proxy, nextaskings: [nextasking] });

    const dispatchRejection = await rejectionOf(
      nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" }),
    );
    const emitRejection = await rejectionOf(nextasking.emit("invoice.paid", { invoiceId: "inv-7" }));
    const getRunRejection = await rejectionOf(nextasking.getRun("send-welcome-email", "welcome-ada"));
    const countRunsRejection = await rejectionOf(nextasking.countRuns());

    expect(dispatchRejection).toBeInstanceOf(RedisUnavailableError);
    expect(dispatchRejection).toMatchObject({
      name: "RedisUnavailableError",
      message: "Redis didn't answer within 1s",
      runId: "welcome-ada",
      cause: expect.any(Error),
    });
    expect(emitRejection).toBeInstanceOf(RedisUnavailableError);
    expect((emitRejection as RedisUnavailableError).eventId).toEqual(expect.any(String));
    expect(getRunRejection).toMatchObject({ name: "RedisUnavailableError", runId: "welcome-ada" });
    expect(countRunsRejection).toBeInstanceOf(RedisUnavailableError);

    await proxy.restore();
    await vi.waitFor(
      async () => {
        await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" }, { runId: "welcome-ada" });
      },
      { timeout: 10_000 },
    );
    expect(await nextasking.getRun("send-welcome-email", "welcome-ada")).toMatchObject({ runId: "welcome-ada" });
  },
  30_000,
);

integrationTest(
  "with the connection cut after it was up, a call throws RedisUnavailableError after giveUpAfter, then works once it is back",
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const nextasking = createInvoicingNextasking(proxy.url, keyPrefix);

    closeWhenTestFinishes({ proxy, nextaskings: [nextasking] });

    await nextasking.dispatch("send-welcome-email", { email: "ada@example.com" });
    await proxy.cut();

    const dispatchRejection = await rejectionOf(
      nextasking.dispatch("send-welcome-email", { email: "grace@example.com" }, { runId: "welcome-grace" }),
    );
    const emitRejection = await rejectionOf(
      nextasking.emit("invoice.paid", { invoiceId: "invoice-7" }, { eventId: "invoice-7-paid" }),
    );

    expect(dispatchRejection).toMatchObject({
      name: "RedisUnavailableError",
      runId: "welcome-grace",
      cause: expect.any(Error),
    });
    expect(emitRejection).toMatchObject({ name: "RedisUnavailableError", eventId: "invoice-7-paid" });

    await proxy.restore();
    await vi.waitFor(
      async () => {
        await nextasking.dispatch("send-welcome-email", { email: "grace@example.com" }, { runId: "welcome-grace" });
      },
      { timeout: 10_000 },
    );
    expect(await nextasking.getRun("send-welcome-email", "welcome-grace")).toMatchObject({ runId: "welcome-grace" });
  },
  30_000,
);
