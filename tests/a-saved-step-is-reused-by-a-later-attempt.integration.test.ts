import { expect } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";

integrationTest(
  "a later attempt re-enters the handler from the top and gets the saved result without running fn again",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const idempotencyKeysSeenByFn: string[] = [];
    const chargesSeenByAttempt: unknown[] = [];
    const chargeInvoice = defineTask("charge-invoice", {
      schema: z.object({ invoiceId: z.string() }),
      retries: 1,
      backoff: "0s",
      handler: async ({ payload, step, attempt }) => {
        const charge = await step.run("create-charge", ({ idempotencyKey }) => {
          idempotencyKeysSeenByFn.push(idempotencyKey);
          return { chargeId: `ch-for-${payload.invoiceId}`, refundedAt: undefined };
        });
        chargesSeenByAttempt.push(charge);
        if (attempt === 1) {
          throw new Error("receipt service unavailable");
        }
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [chargeInvoice],
    });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("charge-invoice", { invoiceId: "inv-7" });
      await waitForLogMessage(lines, "run completed");

      expect(idempotencyKeysSeenByFn).toHaveLength(1);
      expect(chargesSeenByAttempt).toStrictEqual([{ chargeId: "ch-for-inv-7" }, { chargeId: "ch-for-inv-7" }]);
      expect(lines.filter((line) => line.message === "attempt started")).toHaveLength(2);
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
