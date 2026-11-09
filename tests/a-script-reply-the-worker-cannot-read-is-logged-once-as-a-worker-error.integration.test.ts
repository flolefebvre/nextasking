import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask, NonRetriableError } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a script error the worker meets while it fails a run is logged once as a worker error, and the worker goes on",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    const { logger, lines } = createRecordingLogger();
    const completedOrderIds: string[] = [];
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [
        defineTask("charge-card", {
          schema: z.object({ cardId: z.string() }),
          handler: async () => {
            throw new NonRetriableError("card declined");
          },
        }),
        defineTask("ship-order", {
          schema: z.object({ orderId: z.string() }),
          handler: async ({ payload }) => {
            completedOrderIds.push(payload.orderId);
          },
        }),
      ],
    });

    try {
      await nextasking.startWorker();
      const failedIndexKey = engineKeysOf(keyPrefix).runIndex("charge-card", "failed");
      await withRedis(redisUrl, (redis) => redis.set(failedIndexKey, "not a sorted set"));

      const { runId } = await nextasking.dispatch("charge-card", { cardId: "card-1" });
      expectInvariantViolation("stored keys", `${failedIndexKey} is a string, not a zset`);
      expectInvariantViolation(
        "stored keys",
        `run "${storedRunIdOf("charge-card", runId)}" is failed but missing from ${failedIndexKey}`,
      );
      await waitForLogMessage(lines, "worker error");
      const scriptError = lines.find((line) => line.message === "worker error")?.fields["err"] as Error;
      expectInvariantViolation("worker error", scriptError.message);
      await nextasking.dispatch("ship-order", { orderId: "order-1" });
      await vi.waitFor(() => expect(completedOrderIds).toEqual(["order-1"]), { timeout: 10_000 });

      expect(lines.filter((line) => line.message.endsWith(" error"))).toEqual([
        {
          level: "error",
          fields: { err: expect.objectContaining({ message: expect.stringContaining("WRONGTYPE") }) },
          message: "worker error",
        },
      ]);
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
