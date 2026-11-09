import { expect, vi } from "vitest";
import { z } from "zod";

import { createNextasking, defineTask } from "../src/index.js";
import { closeNextaskingAndCheckItsDuration } from "./close-nextasking-and-check-its-duration.js";
import { integrationTest } from "./integration-test.js";

const CONSOLE_METHODS = ["log", "debug", "info", "warn", "error", "trace"] as const;

integrationTest("a run without a logger writes nothing to the console", async ({ redisUrl, keyPrefix }) => {
  const consoleSpies = CONSOLE_METHODS.map((method) => vi.spyOn(console, method));
  let isRunCompleted = false;
  const purgeSessions = defineTask("purge-sessions", {
    schema: z.object({}),
    handler: async (context) => {
      context.logger.info({}, "sessions purged");
      isRunCompleted = true;
    },
  });
  const nextasking = createNextasking({ connection: { url: redisUrl }, prefix: keyPrefix, tasks: [purgeSessions] });

  try {
    await nextasking.startWorker();
    await nextasking.dispatch("purge-sessions", {});
    await vi.waitUntil(() => isRunCompleted, { timeout: 10_000 });
    await closeNextaskingAndCheckItsDuration(nextasking);

    for (const consoleSpy of consoleSpies) {
      expect(consoleSpy).not.toHaveBeenCalled();
    }
  } finally {
    await closeNextaskingAndCheckItsDuration(nextasking);
    for (const consoleSpy of consoleSpies) {
      consoleSpy.mockRestore();
    }
  }
});
