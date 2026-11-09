import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";
import { createRecordingLogger, waitForLogMessage } from "./recording-logger.js";

integrationTest(
  "a run waiting out its backoff or asleep frees its concurrency slot for another run of the same key",
  { timeout: 45_000 },
  async ({ redisUrl, keyPrefix }) => {
    const { logger, lines } = createRecordingLogger();
    const handlerEvents: string[] = [];
    const refreshAccount = defineTask("refresh-account", {
      schema: z.object({ accountId: z.string(), role: z.enum(["fail once", "sleep", "other"]) }),
      backoff: "2s",
      concurrency: { key: (payload) => payload.accountId, limit: 1 },
      handler: async ({ payload, attempt, step }) => {
        handlerEvents.push(`${payload.role} attempt ${String(attempt)}`);
        if (payload.role === "fail once" && attempt === 1) {
          throw new Error("upstream unavailable");
        }
        if (payload.role === "sleep") {
          await step.sleep("wait-for-upstream", "2s");
          handlerEvents.push("sleep resumed");
        }
      },
    });
    const nextasking = createNextasking({
      connection: { url: redisUrl },
      prefix: keyPrefix,
      logger,
      tasks: [refreshAccount],
    });

    try {
      await nextasking.startWorker();
      await nextasking.dispatch("refresh-account", { accountId: "account-7", role: "fail once" });
      await waitForLogMessage(lines, "attempt failed");
      await nextasking.dispatch("refresh-account", { accountId: "account-7", role: "other" });
      await waitForLogMessage(lines, "run completed");
      await nextasking.dispatch("refresh-account", { accountId: "account-9", role: "sleep" });
      await waitForLogMessage(lines, "run suspended");
      await nextasking.dispatch("refresh-account", { accountId: "account-9", role: "other" });
      await vi.waitFor(
        () => {
          expect(lines.filter((line) => line.message === "run completed")).toHaveLength(4);
        },
        { timeout: 10_000 },
      );

      expect(handlerEvents.indexOf("other attempt 1")).toBeLessThan(handlerEvents.indexOf("fail once attempt 2"));
      expect(handlerEvents.lastIndexOf("other attempt 1")).toBeLessThan(handlerEvents.indexOf("sleep resumed"));
      expect(lines.map((line) => line.message)).not.toContain("run waiting for a concurrency slot");
    } finally {
      await closeNextaskingAndCheckItsDuration(nextasking);
    }
  },
);
