import { expect, vi } from "vitest";
import { z } from "zod";

import { engineKeysOf } from "../src/engine/engine-keys.js";
import { createNextasking, defineTask } from "../src/index.js";
import { closeNextasking } from "../src/registry.js";
import { integrationTest } from "./integration-test.js";
import { savedSteps } from "./saved-steps.js";
import { withRedis } from "./with-redis.js";

integrationTest(
  "a step is refused, read or write, once the run's record holds another attempt's lock token",
  async ({ redisUrl, keyPrefix }) => {
    const stepErrors: unknown[] = [];
    const sendNewsletter = defineTask("send-newsletter", {
      schema: z.object({ issueId: z.string() }),
      retries: 0,
      handler: async ({ step, runId }) => {
        await step.run("pick-recipients", () => ["ada@example.com"]);
        const recordKey = engineKeysOf(keyPrefix).runRecord("send-newsletter", runId);
        await withRedis(redisUrl, (redis) => redis.hset(recordKey, "lockToken", "token-of-another-attempt"));
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
        message: `Step "send" of run "${runId}" was not read: this attempt no longer holds its lock, so another attempt owns it now.`,
      });
      expect(steps.serializedResultsByStepId).toEqual({ "pick-recipients": '[["ada@example.com"]]' });
    } finally {
      await closeNextasking(nextasking);
    }
  },
);
