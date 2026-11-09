import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createEngine } from "../src/engine/engine.js";
import { deriveListenerRunId } from "../src/ids.js";
import { createNextasking, defineEvent, defineListener } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { DAY_IN_MILLISECONDS } from "./ended-runs.js";
import { backdateEndedRun } from "./engine-test.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { serverInstantInMillisecondsOf } from "./server-instant.js";
import { storedRunIdentities } from "./stored-run-identities.js";
import { storedRun } from "./stored-run.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a repeated event id emits nothing within its 24-hour dedup window, and after it starts only the listeners whose run is gone",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const recordedListenerNames: string[] = [];
    const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
    const listenerOf = (name: string) =>
      defineListener(name, {
        on: invoicePaid,
        handler: async () => {
          recordedListenerNames.push(name);
        },
      });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      events: [invoicePaid],
      tasks: [listenerOf("record-payment"), listenerOf("send-receipt")],
    });
    const eventId = "stripe-evt-7";
    const recordPaymentRunId = deriveListenerRunId(eventId, "record-payment");
    const sendReceiptRunId = deriveListenerRunId(eventId, "send-receipt");
    const keys = engineKeysOf(keyPrefix);

    try {
      await nextasking.startWorker();
      await nextasking.emit("invoice.paid", { invoiceId: "inv-7" }, { eventId });
      await vi.waitFor(
        () => {
          expect(recordedListenerNames.sort()).toEqual(["record-payment", "send-receipt"]);
        },
        { timeout: 10_000 },
      );
      await nextasking.emit("invoice.paid", { invoiceId: "inv-7" }, { eventId });
      const runsWithinTheWindow = await storedRunIdentities(redisUrl, keyPrefix);
      await withRedis(redisUrl, async (redis) => {
        await redis.zadd(
          keys.eventDedup("invoice.paid"),
          "XX",
          (await serverInstantInMillisecondsOf(redis)) - 1,
          eventId,
        );
        await backdateEndedRun(
          { engine: createEngine(redis, keyPrefix), redis, keys, keyPrefix },
          "record-payment",
          recordPaymentRunId,
          2 * DAY_IN_MILLISECONDS,
        );
      });
      await vi.waitFor(
        async () => {
          expect(await storedRun(redisUrl, keyPrefix, "record-payment", recordPaymentRunId)).toBeUndefined();
        },
        { timeout: 10_000 },
      );
      const linesBeforeTheRepeat = lines.length;

      await nextasking.emit("invoice.paid", { invoiceId: "inv-7" }, { eventId });
      await vi.waitFor(
        () => {
          expect(recordedListenerNames.sort()).toEqual(["record-payment", "record-payment", "send-receipt"]);
        },
        { timeout: 10_000 },
      );

      expect(runsWithinTheWindow).toEqual([
        { task: "record-payment", runId: recordPaymentRunId },
        { task: "send-receipt", runId: sendReceiptRunId },
      ]);
      expect(lines.filter((line) => line.message === "duplicate dispatch dropped").slice(0, 1)).toEqual([
        { level: "debug", fields: { eventName: "invoice.paid", eventId }, message: "duplicate dispatch dropped" },
      ]);
      expect(lines.slice(linesBeforeTheRepeat).filter((line) => line.message === "duplicate dispatch dropped")).toEqual(
        [
          {
            level: "debug",
            fields: { task: "send-receipt", runId: sendReceiptRunId, eventName: "invoice.paid", eventId },
            message: "duplicate dispatch dropped",
          },
        ],
      );
      expect(await storedRun(redisUrl, keyPrefix, "record-payment", recordPaymentRunId)).toMatchObject({
        state: "completed",
      });
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
