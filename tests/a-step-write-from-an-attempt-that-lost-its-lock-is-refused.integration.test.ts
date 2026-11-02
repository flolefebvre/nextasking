import { expect, vi } from "vitest";
import { z } from "zod";

import { deriveJobId } from "../src/ids.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { savedSteps } from "./saved-steps.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a step write is refused once BullMQ's lock key <prefix>:runs:<jobId>:lock holds another attempt's token",
  async ({ redisUrl, keyPrefix }) => {
    const stepErrors: unknown[] = [];
    const sendNewsletter = defineTask("send-newsletter", {
      schema: z.object({ issueId: z.string() }),
      retries: 0,
      handler: async ({ step, runId }) => {
        await step.run("pick-recipients", () => ["ada@example.com"]);
        const lockKey = `${keyPrefix}:runs:${deriveJobId("send-newsletter", runId)}:lock`;
        await withRedis(redisUrl, (redis) => redis.set(lockKey, "token-of-another-attempt", "PX", 60_000));
        await step
          .run("send", () => "sent")
          .catch((error: unknown) => {
            stepErrors.push(error);
            throw error;
          });
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      tasks: [sendNewsletter],
    });

    try {
      await nextasking.startWorker();
      const { runId } = await nextasking.dispatch("send-newsletter", { issueId: "issue-7" });
      await vi.waitFor(
        () => {
          expect(stepErrors).toHaveLength(1);
        },
        { timeout: 10_000 },
      );
      const steps = await savedSteps(redisUrl, keyPrefix, "send-newsletter", runId);

      expect(stepErrors[0]).toMatchObject({
        name: "LostRunLockError",
        message: expect.stringContaining('Step "send" was not saved'),
      });
      expect(steps.serializedResultsByStepId).toEqual({ "pick-recipients": '[["ada@example.com"]]' });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
