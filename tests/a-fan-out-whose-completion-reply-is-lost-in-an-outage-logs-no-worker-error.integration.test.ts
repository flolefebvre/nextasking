import { expect, vi } from "vitest";
import { z } from "zod";

import { deriveJobId } from "../src/ids.js";
import { createNextasking, defineEvent, defineListener, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import {
  createRecordingLogger,
  expectOneConnectionErrorAndOneConnectionRestored,
  waitForLogMessage,
} from "./recording-logger.js";
import { runQueueKeyPrefix } from "./run-job-key.js";
import { startTcpProxy } from "./tcp-proxy.js";

const MOVE_TO_COMPLETED_ARGUMENT = "returnvalue";

integrationTest(
  "a connection cut after the completion of a fan-out is stored, before its reply arrives, logs no worker error and starts no listener run twice",
  { timeout: 20_000 },
  async ({ redisUrl, keyPrefix }) => {
    const proxy = await startTcpProxy(redisUrl);
    const { logger, lines } = createRecordingLogger();
    const listenerRunIds: string[] = [];
    const invoicePaid = defineEvent("invoice.paid", { schema: z.object({ invoiceId: z.string() }) });
    const nextasking = createNextasking({
      connection: { url: proxy.url, giveUpAfter: "1s" },
      prefix: keyPrefix,
      logger,
      events: [invoicePaid],
      tasks: [
        defineListener("record-payment", {
          on: invoicePaid,
          handler: async ({ runId }) => {
            listenerRunIds.push(runId);
          },
        }),
        defineListener("send-receipt", {
          on: invoicePaid,
          handler: async ({ runId }) => {
            listenerRunIds.push(runId);
          },
        }),
        defineTask("ping-after-outage", { schema: z.object({}), handler: async () => undefined }),
      ],
    });
    const eventId = "stripe-evt-7";
    const fanOutJobKey = `${runQueueKeyPrefix(keyPrefix)}${deriveJobId("invoice.paid", eventId)}`;

    try {
      await nextasking.startWorker();
      const completionReplyCut = proxy.cutBeforeReplyTo(
        (request) => request.includes(fanOutJobKey) && request.includes(MOVE_TO_COMPLETED_ARGUMENT),
      );
      await nextasking.emit("invoice.paid", { invoiceId: "invoice-7" }, { eventId });
      await waitForLogMessage(lines, "connection error");
      await completionReplyCut;
      await proxy.restore();
      await waitForLogMessage(lines, "connection restored", { timeout: 15_000 });
      await nextasking.dispatch("ping-after-outage", {});
      await vi.waitFor(
        () => {
          expect(
            lines.filter((line) => line.message === "run completed" && line.fields.task === "ping-after-outage"),
          ).toHaveLength(1);
        },
        { timeout: 15_000 },
      );

      expect(new Set(listenerRunIds).size).toBe(listenerRunIds.length);
      expectOneConnectionErrorAndOneConnectionRestored(lines);
      expect(
        lines.filter((line) => line.message === "worker error" || line.message === "fan-out completion unconfirmed"),
      ).toEqual([]);
    } finally {
      await closeNextasking(nextasking);
      await proxy.stop();
    }
  },
);
