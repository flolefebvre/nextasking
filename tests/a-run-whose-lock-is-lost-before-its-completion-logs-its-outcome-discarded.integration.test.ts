import { expect } from "vitest";
import { z } from "zod";

import { engineKeysOf, storedRunIdOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";
import { runLeftExecutingSentenceOf } from "./stored-key-violations.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a run whose lock is taken over when its handler returns, while its record still says executing, logs its outcome discarded and no run completed",
  async ({ redisUrl, keyPrefix, expectInvariantViolation }) => {
    await withRedis(redisUrl, async (redis) => {
      const { logger, lines } = createRecordingLogger();
      const nextasking = createNextasking({
        connection: { url: redisUrl },
        prefix: keyPrefix,
        logger,
        tasks: [
          defineTask("archive-invoice", {
            schema: z.object({ invoiceId: z.string() }),
            handler: async ({ runId }) => {
              await redis.hset(
                engineKeysOf(keyPrefix).runRecord("archive-invoice", runId),
                "lockToken",
                "token-of-another-attempt",
              );
            },
          }),
        ],
      });

      try {
        await nextasking.startWorker();
        const { runId } = await nextasking.dispatch("archive-invoice", { invoiceId: "invoice-1" });
        expectInvariantViolation("run left executing", runLeftExecutingSentenceOf("archive-invoice", runId));
        await waitForLogMessage(lines, "attempt outcome discarded: lock lost");

        expect(lines.filter((line) => line.message === "attempt outcome discarded: lock lost")).toEqual([
          {
            level: "warn",
            fields: { task: "archive-invoice", runId, attempt: 1, outcome: "complete" },
            message: "attempt outcome discarded: lock lost",
          },
        ]);
        expect(lines.filter((line) => line.message === "run completed" || line.message === "worker error")).toEqual([]);
        expect(await redis.hgetall(engineKeysOf(keyPrefix).runRecord("archive-invoice", runId))).toMatchObject({
          state: "executing",
          lockToken: "token-of-another-attempt",
        });
        expect(
          await redis.zscore(engineKeysOf(keyPrefix).locks, storedRunIdOf("archive-invoice", runId)),
        ).not.toBeNull();
      } finally {
        await closeNextasking(nextasking);
      }
    });
  },
);
