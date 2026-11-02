import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask, NonRetriableError } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger } from "./recording-logger.js";
import { storedRun } from "./stored-run.js";

class NonRetriableErrorFromAnotherCopy extends Error {
  override readonly name = "NonRetriableError";
}

integrationTest(
  "throwing NonRetriableError, or an error of that name, fails the run at once with its retries left unused",
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const chargeCard = defineTask("charge-card", {
      schema: z.object({ cardId: z.string() }),
      backoff: "0s",
      handler: async () => {
        throw new NonRetriableError("card declined");
      },
    });
    const refundCard = defineTask("refund-card", {
      schema: z.object({ cardId: z.string() }),
      backoff: "0s",
      handler: async () => {
        throw new NonRetriableErrorFromAnotherCopy("refund refused");
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [chargeCard, refundCard],
    });

    try {
      await nextasking.startWorker();
      const charge = await nextasking.dispatch("charge-card", { cardId: "card-7" });
      const refund = await nextasking.dispatch("refund-card", { cardId: "card-7" });
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run failed")).toHaveLength(2);
        },
        { timeout: 10_000 },
      );
      const storedCharge = await storedRun(redisUrl, keyPrefix, "charge-card", charge.runId);
      const storedRefund = await storedRun(redisUrl, keyPrefix, "refund-card", refund.runId);

      expect(lines.map((line) => line.message)).not.toContain("attempt failed");
      expect(lines.filter((line) => line.message === "attempt started")).toHaveLength(2);
      expect(lines.filter((line) => line.message === "run failed")).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            level: "error",
            fields: expect.objectContaining({
              task: "charge-card",
              attempt: 1,
              reason: "non-retriable",
              err: expect.objectContaining({ name: "NonRetriableError", message: "card declined" }),
            }),
          }),
          expect.objectContaining({
            fields: expect.objectContaining({ task: "refund-card", attempt: 1, reason: "non-retriable" }),
          }),
        ]),
      );
      expect(storedCharge).toMatchObject({
        reason: "non-retriable",
        errors: [{ attempt: 1, name: "NonRetriableError", message: "card declined" }],
      });
      expect(storedRefund).toMatchObject({ reason: "non-retriable", errors: [{ attempt: 1 }] });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
